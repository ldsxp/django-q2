# Standard
import ast
from queue import Empty
from django_q.worker import Pool, Worker
import pydoc
import signal
from django_q.monitor import Monitor
import socket
import traceback
import uuid
from datetime import datetime, timedelta
from multiprocessing import Event, Process, Value, current_process
from time import sleep

# Django
from django import core, db
from django.apps.registry import apps

try:
    apps.check_apps_ready()
except core.exceptions.AppRegistryNotReady:
    import django

    django.setup()

from django.utils import timezone
from django.utils.translation import gettext_lazy as _

# Local
import django_q.tasks
from django_q.brokers import Broker, get_broker
from django_q.conf import (
    Conf,
    croniter,
    error_reporter,
    get_ppid,
    logger,
    psutil,
    setproctitle,
    resource,
)
from django_q.humanhash import humanize
from django_q.status import Stat, Status

class Cluster:
    def __init__(self, broker: Broker = None):
        self.broker = broker or get_broker()
        self.sentinel = None
        self.stop_event = None
        self.start_event = None
        self.pid = current_process().pid
        self.cluster_id = uuid.uuid4()
        self.host = socket.gethostname()
        self.timeout = Conf.TIMEOUT
        signal.signal(signal.SIGTERM, self.sig_handler)
        signal.signal(signal.SIGINT, self.sig_handler)

    def start(self) -> int:
        if setproctitle:
            setproctitle.setproctitle(f"qcluster {current_process().name} {self.name}")
        # Start Sentinel
        self.stop_event = Event()
        self.start_event = Event()
        self.sentinel = Process(
            target=Sentinel,
            args=(
                self.stop_event,
                self.start_event,
                self.cluster_id,
                self.broker,
                self.timeout,
            ),
        )
        self.sentinel.start()
        logger.info(_("Q Cluster %(name)s starting.") % {"name": self.name})
        while not self.start_event.is_set():
            sleep(0.1)
        return self.pid

    def stop(self) -> bool:
        if not self.sentinel.is_alive():
            return False
        logger.info(_("Q Cluster %(name)s stopping.") % {"name": self.name})
        self.stop_event.set()
        self.sentinel.join()
        logger.info(_("Q Cluster %(name)s has stopped.") % {"name": self.name})
        self.start_event = None
        self.stop_event = None
        return True

    def sig_handler(self, signum, frame):
        logger.debug(
            _("%(name)s got signal %(signal)s")
            % {
                "name": current_process().name,
                "signal": Conf.SIGNAL_NAMES.get(signum, "UNKNOWN"),
            }
        )
        self.stop()

    @property
    def stat(self) -> Status:
        if self.sentinel:
            return Stat.get(pid=self.pid, cluster_id=self.cluster_id)
        return Status(pid=self.pid, cluster_id=self.cluster_id)

    @property
    def name(self) -> str:
        return humanize(self.cluster_id.hex)

    @property
    def is_starting(self) -> bool:
        return self.stop_event and self.start_event and not self.start_event.is_set()

    @property
    def is_running(self) -> bool:
        return self.stop_event and self.start_event and self.start_event.is_set()

    @property
    def is_stopping(self) -> bool:
        return (
            self.stop_event
            and self.start_event
            and self.start_event.is_set()
            and self.stop_event.is_set()
        )

    @property
    def has_stopped(self) -> bool:
        return self.start_event is None and self.stop_event is None and self.sentinel


class Sentinel:
    def __init__(
        self,
        stop_event,
        start_event,
        cluster_id,
        broker=None,
        timeout=Conf.TIMEOUT,
        start=True,
    ):
        # Make sure we catch signals for the pool
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        self.pid = current_process().pid
        self.cluster_id = cluster_id
        self.parent_pid = get_ppid()
        self.name = current_process().name
        self.broker = broker or get_broker()
        self.reincarnations = 0
        self.tob = timezone.now()
        self.stop_event = stop_event
        self.start_event = start_event
        self.pool = []
        self.timeout = timeout
        self.event_out = Event()
        logger.info(
            _("%(name)s main at %(id)s") % {"name": self.name, "id": current_process().pid}
        )
        from django_q.puller import Puller
        self.puller = Puller()

        self.monitor = Monitor()
        if start:
            self.start()

    def start(self):
        self.broker.ping()
        self.spawn_cluster()
        self.guard()

    def status(self) -> str:
        if not self.start_event.is_set() and not self.stop_event.is_set():
            return Conf.STARTING
        elif self.start_event.is_set() and not self.stop_event.is_set():
            if self.result_queue.empty() and self.task_queue.empty():
                return Conf.IDLE
            return Conf.WORKING
        elif self.stop_event.is_set() and self.start_event.is_set():
            if self.monitor.is_alive() or self.puller.is_alive() or len(self.pool) > 0:
                return Conf.STOPPING
            return Conf.STOPPED

    def spawn_cluster(self):
        # Stat(self).save()
        # close connections before spawning new process
        if not Conf.SYNC:
            db.connections.close_all()
        # spawn worker pool
        self.pool = Pool()
        # set worker cpu affinity if needed
        if psutil and Conf.CPU_AFFINITY:
            set_cpu_affinity(Conf.CPU_AFFINITY, [w.process.process_id for w in self.pool.workers])


    def guard(self):
        logger.info(
            _("%(name)s guarding cluster %(cluster_name)s")
            % {
                "name": current_process().name,
                "cluster_name": humanize(self.cluster_id.hex),
            }
        )
        self.start_event.set()
        logger.info(
            _("Q Cluster %(cluster_name)s running.")
            % {"cluster_name": humanize(self.cluster_id.hex)}
        )
        counter = 0
        # Guard loop. Runs at least once
        while not self.stop_event.is_set() or not counter:
            logger.info("is set")
            logger.info(self.stop_event.is_set())
            # Check if the pool of workers is healthy
            logger.info("Check if pool is healthy")
            if not self.pool.is_healthy:
                # reincarnate workers that died
                print("reincarnate workers")
                self.pool.reincarnate_stopped_workers()

            print("Check if puller is healthy")
            if not self.puller.is_alive:
                self.puller.reincarnate_process()

            print("Check if monitor is healthy")
            if not self.monitor.is_alive:
                self.monitor.reincarnate_process()

            print("add tasks and mark workers idle")
            for worker in self.pool.get_done_workers():
                # put result in task_queue to be picked up by monitor for processing
                self.monitor.add_task(worker.get_result())
                # mark task back to idle or reincarnate to be picked up for a new task
                if worker.is_recycle:
                    worker.reincarnate_process()
                else:
                    worker.mark_idle()

            # check if monitor has items to process
            print("run monitor item")
            self.monitor.run_item()

            print("Add task to worker pool")
            if self.puller.has_results:
                self.pool.add_task(self.puller.get_result())

            # delegate tasks to workers that are now available
            print("delegate tasks")
            self.pool.delegate_tasks()

            logger.info("sleep")
            sleep(Conf.GUARD_CYCLE)
            counter += 1
        self.stop()

    def stop(self):
        name = current_process().name
        logger.info(_("%(name)s stopping cluster processes") % {"name": name})
        # Stopping guard
        self.stop_event.set()
        logger.info(_("Guard has stopped"))

        # End all workers gracefully
        for __ in range(Conf.WORKERS):
            self.pool.add_task("STOP")

        # manually loop through the tasks
        while not self.pool.task_queue.empty():
            self.monitor.run_item()
            self.pool.delegate_tasks()
            sleep(0.5)

        logger.info(_("All tasks were processed and workers where stopped"))

        self.monitor.add_task("STOP")
        while not self.monitor.task_queue.empty():
            # in the case the monitor was behind, let's run through all
            self.monitor.run_item()
            sleep(0.5)

        logger.info(_("All tasks were saved"))

        self.puller.stop_puller()

        # make sure all processes are terminated
        for worker in self.pool.workers:
            worker.process.terminate()

        self.monitor.process.terminate()
        self.puller.process.terminate()

        logger.info(_("All processes were terminated"))


def set_cpu_affinity(n: int, process_ids: list, actual: bool = not Conf.TESTING):
    """
    Sets the cpu affinity for the supplied processes.
    Requires the optional psutil module.
    :param int n: affinity
    :param list process_ids: a list of pids
    :param bool actual: Test workaround for Travis not supporting cpu affinity
    """
    # check if we have the psutil module
    if not psutil:
        logger.warning(_("Skipping cpu affinity because psutil was not found."))
        return
    # check if the platform supports cpu_affinity
    if actual and not hasattr(psutil.Process(process_ids[0]), "cpu_affinity"):
        logger.warning(
            _("Faking cpu affinity because it is not supported on this platform")
        )
        actual = False
    # get the available processors
    cpu_list = list(range(psutil.cpu_count()))
    # affinities of 0 or gte cpu_count, equals to no affinity
    if not n or n >= len(cpu_list):
        return
    # spread the workers over the available processors.
    index = 0
    for pid in process_ids:
        affinity = []
        for k in range(n):
            if index == len(cpu_list):
                index = 0
            affinity.append(cpu_list[index])
            index += 1
        if psutil.pid_exists(pid):
            p = psutil.Process(pid)
            if actual:
                p.cpu_affinity(affinity)
            logger.info(
                _("%(pid)s will use cpu %(affinity)s")
                % {"pid": pid, "affinity": affinity}
            )
