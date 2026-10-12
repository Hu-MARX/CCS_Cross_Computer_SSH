"""A wall-time stop request only at a completed optimizer-step boundary."""
import math
import time


class TrainingTimeBudget:
    def __init__(self, seconds=None, clock=time.monotonic):
        if seconds is not None and (isinstance(seconds, bool) or not isinstance(seconds, (int, float))
                                    or not math.isfinite(seconds) or seconds <= 0):
            raise ValueError('Training wall-time budget must be finite and positive')
        self.seconds, self.clock = seconds, clock
        self.started = None
        self.previous_step = 0

    def start(self, step):
        self.started = self.clock()
        self.previous_step = step

    @property
    def elapsed(self):
        return 0 if self.started is None else self.clock() - self.started

    def after_batch(self, step):
        advanced = step > self.previous_step
        self.previous_step = step
        return self.seconds is not None and self.started is not None and advanced and self.elapsed >= self.seconds
