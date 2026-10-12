"""Bounded subprocess capture with local cancellation and process-tree cleanup."""
from __future__ import annotations

import math
import os
import signal
import subprocess
import threading
import time


class ProcessStopped(RuntimeError):
    def __init__(self, code, *, stdout=b'', stderr=b'', started=False, stream=None):
        super().__init__(code)
        self.code, self.stdout, self.stderr = code, stdout, stderr
        self.started, self.stream = started, stream


class _WindowsJob:
    """Kill descendants when capture ends, including a child that exited early."""
    def __init__(self, process):
        import ctypes
        from ctypes import wintypes

        class Basic(ctypes.Structure):
            _fields_ = [('PerProcessUserTimeLimit', ctypes.c_int64),
                        ('PerJobUserTimeLimit', ctypes.c_int64), ('LimitFlags', wintypes.DWORD),
                        ('MinimumWorkingSetSize', ctypes.c_size_t),
                        ('MaximumWorkingSetSize', ctypes.c_size_t),
                        ('ActiveProcessLimit', wintypes.DWORD), ('Affinity', ctypes.c_size_t),
                        ('PriorityClass', wintypes.DWORD), ('SchedulingClass', wintypes.DWORD)]

        class IO(ctypes.Structure):
            _fields_ = [(name, ctypes.c_uint64) for name in (
                'ReadOperationCount', 'WriteOperationCount', 'OtherOperationCount',
                'ReadTransferCount', 'WriteTransferCount', 'OtherTransferCount')]

        class Extended(ctypes.Structure):
            _fields_ = [('BasicLimitInformation', Basic), ('IoInfo', IO),
                        ('ProcessMemoryLimit', ctypes.c_size_t), ('JobMemoryLimit', ctypes.c_size_t),
                        ('PeakProcessMemoryUsed', ctypes.c_size_t), ('PeakJobMemoryUsed', ctypes.c_size_t)]

        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        kernel.CreateJobObjectW.restype = wintypes.HANDLE
        kernel.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
        kernel.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
        self.kernel, self.handle = kernel, kernel.CreateJobObjectW(None, None)
        if not self.handle:
            raise OSError(ctypes.get_last_error(), 'Cannot create subprocess cleanup job')
        info = Extended()
        info.BasicLimitInformation.LimitFlags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if (not kernel.SetInformationJobObject(self.handle, 9, ctypes.byref(info), ctypes.sizeof(info)) or
                not kernel.AssignProcessToJobObject(self.handle, wintypes.HANDLE(int(process._handle)))):
            error = ctypes.get_last_error()
            self.close()
            raise OSError(error, 'Cannot contain subprocess descendants')

    def resume(self, process):
        """Resume the suspended primary thread after assignment to the job."""
        import ctypes
        from ctypes import wintypes

        class ThreadEntry(ctypes.Structure):
            _fields_ = [('dwSize', wintypes.DWORD), ('cntUsage', wintypes.DWORD),
                        ('th32ThreadID', wintypes.DWORD), ('th32OwnerProcessID', wintypes.DWORD),
                        ('tpBasePri', wintypes.LONG), ('tpDeltaPri', wintypes.LONG),
                        ('dwFlags', wintypes.DWORD)]

        kernel = self.kernel
        kernel.CreateToolhelp32Snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
        kernel.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
        kernel.Thread32First.argtypes = [wintypes.HANDLE, ctypes.POINTER(ThreadEntry)]
        kernel.Thread32Next.argtypes = [wintypes.HANDLE, ctypes.POINTER(ThreadEntry)]
        kernel.OpenThread.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.OpenThread.restype = wintypes.HANDLE
        kernel.ResumeThread.argtypes = [wintypes.HANDLE]
        kernel.ResumeThread.restype = wintypes.DWORD
        snapshot = kernel.CreateToolhelp32Snapshot(0x4, 0)  # TH32CS_SNAPTHREAD
        if snapshot == wintypes.HANDLE(-1).value:
            raise OSError(ctypes.get_last_error(), 'Cannot locate suspended primary thread')
        entry = ThreadEntry()
        entry.dwSize = ctypes.sizeof(entry)
        try:
            present = kernel.Thread32First(snapshot, ctypes.byref(entry))
            while present:
                if entry.th32OwnerProcessID == process.pid:
                    thread = kernel.OpenThread(0x0002, False, entry.th32ThreadID)
                    if not thread:
                        raise OSError(ctypes.get_last_error(), 'Cannot open suspended primary thread')
                    try:
                        previous = kernel.ResumeThread(thread)
                        if previous == 0xFFFFFFFF or previous != 1:
                            raise OSError(ctypes.get_last_error(), 'Cannot resume suspended primary thread')
                        return
                    finally:
                        kernel.CloseHandle(thread)
                present = kernel.Thread32Next(snapshot, ctypes.byref(entry))
            raise OSError('Suspended primary thread was not found')
        finally:
            kernel.CloseHandle(snapshot)

    def close(self):
        if self.handle:
            self.kernel.TerminateJobObject(self.handle, 1)
            if not self.kernel.CloseHandle(self.handle):
                self.kernel.TerminateJobObject(self.handle, 1)
                if not self.kernel.CloseHandle(self.handle):
                    import ctypes
                    raise OSError(ctypes.get_last_error(), 'Cannot close subprocess cleanup job')
            self.handle = None


def run_bounded(command, *, input=None, cwd=None, env=None, timeout=120,
                max_stdout_bytes=20 * 1024**2, max_stderr_bytes=64 * 1024,
                cancel_event=None):
    """Return bytes, retaining at most each output budget; never invoke a shell.

    Timeout/cancellation kills only this local subprocess tree. A remote request
    that was already transmitted can still have executed. No calls are retried.
    """
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('timeout must be positive and finite')
    if any(type(value) is not int or value <= 0 for value in (max_stdout_bytes, max_stderr_bytes)):
        raise ValueError('output budgets must be positive integers')
    if cancel_event is not None and cancel_event.is_set():
        raise ProcessStopped('CANCELLED')
    job = None
    buffers = {'stdout': bytearray(), 'stderr': bytearray()}
    overflow = threading.Event()
    failed_stream = []
    io_errors = []
    metadata_lock = threading.Lock()

    def read(stream, budget):
        pipe = getattr(child, stream)
        try:
            while chunk := pipe.read(65536):
                remaining = budget - len(buffers[stream])
                buffers[stream].extend(chunk[:remaining])
                if len(chunk) > remaining:
                    with metadata_lock:
                        if not failed_stream:
                            failed_stream.append(stream)
                    overflow.set()
                    # Continue draining after the bound until the parent kills it.
        except Exception as exc:
            with metadata_lock:
                if not io_errors:
                    io_errors.append(exc)
        finally:
            pipe.close()

    def write():
        try:
            child.stdin.write(input)
            child.stdin.flush()
        except (BrokenPipeError, OSError):
            pass
        except Exception as exc:
            with metadata_lock:
                if not io_errors:
                    io_errors.append(exc)
        finally:
            child.stdin.close()

    readers, writer = [], None
    started_threads = []
    reason = None
    failure = None
    start = time.monotonic()
    child = subprocess.Popen(command, stdin=subprocess.PIPE if input is not None else subprocess.DEVNULL,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False, cwd=cwd, env=env,
                             start_new_session=os.name != 'nt',
                             creationflags=(subprocess.CREATE_NEW_PROCESS_GROUP | 0x4) if os.name == 'nt' else 0)
    try:
        if os.name == 'nt':
            job = _WindowsJob(child)
        readers = [threading.Thread(target=read, args=(name, budget), daemon=True) for name, budget in
                   (('stdout', max_stdout_bytes), ('stderr', max_stderr_bytes))]
        for reader in readers:
            reader.start()
            started_threads.append(reader)
        writer = threading.Thread(target=write, daemon=True) if input is not None else None
        if writer is not None:
            writer.start()
            started_threads.append(writer)
        if job is not None:
            job.resume(child)
        while True:
            if cancel_event is not None and cancel_event.is_set():
                reason = 'CANCELLED'
                break
            if overflow.is_set():
                reason = 'OUTPUT_LIMIT'
                break
            if io_errors:
                raise io_errors[0]
            if child.poll() is not None and not any(reader.is_alive() for reader in readers):
                break
            remaining = timeout - (time.monotonic() - start)
            if remaining <= 0:
                reason = 'TIMEOUT'
                break
            time.sleep(min(0.02, remaining))
    except Exception as exc:
        failure = exc
        reason = 'CONTAINMENT_FAILED'
    finally:
        def cleanup(operation):
            nonlocal failure, reason
            try:
                operation()
                return True
            except Exception as exc:
                failure, reason = failure or exc, 'CONTAINMENT_FAILED'
                return False

        if job is not None:
            cleanup(job.close)
            if job.handle:
                # A failing close wrapper must not leave descendants alive.
                def force_close_job():
                    job.kernel.TerminateJobObject(job.handle, 1)
                    if not job.kernel.CloseHandle(job.handle):
                        import ctypes
                        raise OSError(ctypes.get_last_error(), 'Cannot close cleanup job after retry')
                    job.handle = None
                cleanup(force_close_job)
        elif os.name != 'nt':
            def kill_group():
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            if not cleanup(kill_group):
                cleanup(kill_group)
        def kill_child_if_running():
            if child.poll() is None:
                child.kill()
        cleanup(kill_child_if_running)
        cleanup(lambda: child.wait(timeout=5))
        for thread in started_threads:
            cleanup(lambda thread=thread: thread.join(timeout=2))
            if thread.is_alive():
                reason = 'CONTAINMENT_FAILED'
        # Unstarted pipes need closing too, after the process has been killed.
        if not any(thread.is_alive() for thread in started_threads):
            for pipe in (child.stdin, child.stdout, child.stderr):
                if pipe is not None and not pipe.closed:
                    cleanup(pipe.close)
    stdout, stderr = bytes(buffers['stdout']), bytes(buffers['stderr'])
    if reason is None and overflow.is_set():
        reason = 'OUTPUT_LIMIT'
    if reason is not None:
        raise ProcessStopped(reason, stdout=stdout, stderr=stderr, started=True,
                             stream=failed_stream[0] if failed_stream else None) from failure
    return subprocess.CompletedProcess(command, child.returncode, stdout, stderr)
