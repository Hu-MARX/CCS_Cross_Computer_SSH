"""Optional Lightning callback; import only inside a configured training environment."""
import math
import time
import warnings
from pytorch_lightning import Callback
from .progress import ProgressReporter


class TrainingProgressCallback(Callback):
    def __init__(self, interval=15):
        self.reporter = ProgressReporter(interval=interval)
        self.updates = 0
        self.hooks = []
        self.warned = False

    def _updated(self, *unused):
        self.updates += 1

    def _emit(self, trainer, phase, force=False):
        if not trainer.is_global_zero:
            return
        if not force and time.monotonic() - self.reporter.last < self.reporter.interval:
            return
        try:
            self._publish(trainer, phase, force)
        except Exception as exc:
            # Observation failures must not terminate the research workload.
            if not self.warned:
                warnings.warn(f'Training progress reporting unavailable: {exc}', RuntimeWarning)
                self.warned = True

    def _publish(self, trainer, phase, force):
        metrics = {'epoch': trainer.current_epoch, 'optimizer_updates_this_process': self.updates}
        from .runtime_configuration import learning_rate_metrics
        metrics.update(learning_rate_metrics(trainer))
        for name, value in trainer.callback_metrics.items():
            try:
                number = float(value.detach().cpu()) if hasattr(value, 'detach') else float(value)
            except (ValueError, TypeError, RuntimeError):
                continue
            if math.isfinite(number):
                metrics[str(name)] = number
        self.reporter.emit(phase, step=trainer.global_step, metrics=metrics, force=force)

    def on_train_start(self, trainer, pl_module):
        self.hooks = [item.register_step_post_hook(self._updated) for item in trainer.optimizers]
        try:
            from controller.domain import utc_now
            from .runtime_configuration import project, write
            if self.reporter.directory is not None:
                write(self.reporter.directory, project(trainer, utc_now()))
        except Exception as exc:
            if not self.warned:
                warnings.warn(f'Runtime configuration reporting unavailable: {exc}', RuntimeWarning)
                self.warned = True
        self._emit(trainer, 'train', True)

    def on_train_batch_end(self, trainer, pl_module, outputs, batch, batch_idx):
        self._emit(trainer, 'train')

    def on_validation_start(self, trainer, pl_module):
        self._emit(trainer, 'validation', True)

    def on_validation_end(self, trainer, pl_module):
        self._emit(trainer, 'validation_finished', True)

    def on_train_end(self, trainer, pl_module):
        self._emit(trainer, 'finished', True)

    def teardown(self, trainer, pl_module, stage):
        for hook in self.hooks:
            hook.remove()
        self.hooks.clear()
