from utils import localtime
from models import Schedule
from djang_q import tasks
import ast
from django_q.humanhash import humanize
from django import db
from signing import BadSignature, SignedPackage
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


class Scheduler(ProcessManager):
    """The Scheduler is responsible for scheduling new tasks"""

    def get_target(self):
        return self.run_scheduler

    def run_scheduler(self, status, pipe) -> None:
        while True:
            broker = get_broker()
            close_old_django_connections()
            try:
                with db.transaction.atomic(using=db.router.db_for_write(Schedule)):
                    for s in (
                        Schedule.objects.select_for_update()
                        .exclude(repeats=0)
                        .filter(db.models.Q(next_run__lt=timezone.now()), db.models.Q(cluster__isnull=True) | db.models.Q(cluster=Conf.PREFIX))
                    ):
                        args = ()
                        kwargs = {}
                        # get args, kwargs and hook
                        if s.kwargs:
                            try:
                                # first try the dict syntax
                                kwargs = ast.literal_eval(s.kwargs)
                            except (SyntaxError, ValueError):
                                # else use the kwargs syntax
                                try:
                                    parsed_kwargs = (
                                        ast.parse(f"f({s.kwargs})").body[0].value.keywords
                                    )
                                    kwargs = {
                                        kwarg.arg: ast.literal_eval(kwarg.value)
                                        for kwarg in parsed_kwargs
                                    }
                                except (SyntaxError, ValueError):
                                    kwargs = {}
                        if s.args:
                            args = ast.literal_eval(s.args)
                            # single value won't eval to tuple, so:
                            if type(args) != tuple:
                                args = (args,)
                        q_options = kwargs.get("q_options", {})
                        if s.intended_date_kwarg:
                            kwargs[s.intended_date_kwarg] = s.next_run.isoformat()
                        if s.hook:
                            q_options["hook"] = s.hook
                        # set up the next run time
                        if s.schedule_type != s.ONCE:
                            next_run = s.next_run
                            while True:
                                next_run = s.calculate_next_run(next_run)
                                if Conf.CATCH_UP or next_run > localtime():
                                    break

                            s.next_run = next_run
                            s.repeats += -1
                        # send it to the cluster
                        scheduled_broker = broker
                        try:
                            scheduled_broker = get_broker(q_options["broker_name"])
                        except:  # noqa: E722
                            # invalid broker_name or non existing broker with broker_name
                            pass
                        q_options["broker"] = scheduled_broker
                        q_options["group"] = q_options.get("group", s.name or s.id)
                        kwargs["q_options"] = q_options
                        s.task = tasks.async_task(s.func, *args, **kwargs)
                        # log it
                        if not s.task:
                            logger.error(
                                _(
                                    "%(process_name)s failed to create a task from schedule "
                                    "[%(schedule)s]"
                                )
                                % {
                                    "process_name": current_process().name,
                                    "schedule": s.name or s.id,
                                }
                            )
                        else:
                            logger.info(
                                _(
                                    "%(process_name)s created task %(task_name)s from schedule "
                                    "[%(schedule)s]"
                                )
                                % {
                                    "process_name": current_process().name,
                                    "task_name": humanize(s.task),
                                    "schedule": s.name or s.id,
                                }
                            )
                        # default behavior is to delete a ONCE schedule
                        if s.schedule_type == s.ONCE:
                            if s.repeats < 0:
                                s.delete()
                                continue
                            # but not if it has a positive repeats
                            s.repeats = 0
                        # save the schedule
                        s.save()
            except Exception:
                logger.exception("Could not create task from schedule")
            # sleep 60 seconds for next schedule
            sleep(60)

