from queue import Queue
from queue import Empty
from django_q.brokers import get_broker
from django_q.process_manager import ProcessManager
from django_q.signals import post_execute
from django_q.conf import logger
from django.utils.translation import gettext_lazy as _
from multiprocessing import current_process

try:
    import setproctitle
except ModuleNotFoundError:
    setproctitle = None


class Monitor(ProcessManager):
    def __init__(self):
        super().__init__()
        self.task_queue = Queue()

    @property
    def is_done(self):
        return self.status.value == self.Status.IDLE.value and self.task_queue.empty()

    def get_target(self):
        return self.run_monitor

    def run_item(self):
        if self.is_idle:
            try:
                task = self.task_queue.get_nowait()
            except Empty:
                # if the queue is empty, then just stop
                return
            self.manager_pipe.send(task)

    def add_task(self, task):
        self.task_queue.put(task)

    def run_monitor(self, status, pipe) -> None:
        broker = get_broker()
        proc_name = current_process().name
        if setproctitle:
            setproctitle.setproctitle(f"qcluster {proc_name} monitor")
        logger.info(
            _("%(name)s monitoring at %(id)s") % {"name": proc_name, "id": current_process().pid}
        )
        status.value = self.Status.IDLE.value

        while True:
            task = pipe.recv()
            if task == "STOP":
                logger.info(f"Monitor {proc_name} shut down")
                break
            status.value = self.Status.BUSY.value
            # save the result
            if task.cached:
                task.save_cached(broker)
            else:
                task.save_to_db(broker)
            # acknowledge result
            if task.ack_id and (not task.has_succeeded or task.ack_failure):
                broker.acknowledge(task.ack_id)
            # signal execution done
            post_execute.send(sender="django_q", task=task)
            # log the result
            if task.has_succeeded:
                # log success
                logger.info(
                    _("Processed '%(info_name)s' (%(task_name)s)")
                    % {"info_name": task.func_name, "task_name": task.name}
                )
            else:
                # log failure
                logger.error(
                    _("Failed '%(info_name)s' (%(task_name)s) - %(task_result)s")
                    % {
                        "info_name": task.func_name,
                        "task_name": task.name,
                        "task_result": task.result_payload,
                    }
                )
            status.value = self.Status.IDLE.value
        logger.info(_("%(name)s stopped monitoring results") % {"name": proc_name})

