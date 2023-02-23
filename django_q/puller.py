from django_q.worker import Worker
from django_q.signing import BadSignature, SignedPackage
from time import sleep
from django_q.brokers import get_broker
import multiprocessing
from django_q.queue_task import QueueTask
from django.utils import timezone
import enum
import traceback

from multiprocessing import Event, Process, Value, current_process
from django_q.utils import close_old_django_connections
from django.utils.translation import gettext_lazy as _
from django_q.conf import Conf, logger, setproctitle, error_reporter, resource, psutil
from django_q.exceptions import TimeoutException, TimeoutHandler
from django_q.process_manager import ProcessManager


class Puller(ProcessManager):
    """The Puller is responsible for pulling the tasks from the broker, then return them to be picked up by the
    guard"""

    def get_target(self):
        return self.run_puller

    def stop_puller(self):
        self.status.value = self.Status.DONE.value

    def run_puller(self, status, pipe) -> None:
        broker = get_broker()
        proc_name = current_process().name
        if setproctitle:
            setproctitle.setproctitle(f"qcluster {proc_name} puller")
        logger.info(
            _("%(name)s pulling tasks from broker %(id)s")
            % {"name": proc_name, "id": current_process().pid}
        )
        while True:
            if status.value == Worker.Status.DONE.value:
                logger.info("Stopping Puller")
                break
            try:
                task_set = broker.dequeue()
            except Exception:
                logger.exception("Failed to pull task from broker")
                # broker probably crashed. Let the sentinel handle it.
                sleep(10)
                break
            if task_set:
                logger.info(
                    _("Found %(amount_tasks)s tasks") % {"amount_tasks": len(task_set)}
                )
                for task in task_set:
                    ack_id = task[0]
                    # unpack the task
                    try:
                        queue_task = SignedPackage.loads(task[1])
                    except (TypeError, BadSignature):
                        logger.exception("Failed to pull task from broker - bad task")
                        broker.fail(ack_id)
                        continue
                    queue_task.ack_id = ack_id
                    # send back to main process
                    pipe.send(queue_task)
                logger.debug(
                    _("queueing from %(list_key)s") % {"list_key": broker.list_key}
                )
        logger.info(_("%(name)s stopped pushing tasks") % {"name": current_process().name})
