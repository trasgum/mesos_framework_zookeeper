#!/usr/bin/env python2.7
# A skeleton for writing a custom Mesos executor.
#
# For more information, see:
#   * https://github.com/apache/mesos/blob/0.22.2/src/python/interface/src/mesos/interface/__init__.py#L246-L310
#   * https://github.com/apache/mesos/blob/0.22.2/include/mesos/mesos.proto#L262-L291
#
from __future__ import print_function

import site
site.addsitedir('/usr/lib/python2.7/site-packages')
site.addsitedir('/usr/local/lib/python2.7/site-packages')

import logging
import sys
import time
from threading import Thread

from mesos.interface import Executor, mesos_pb2
from mesos.native import MesosExecutorDriver


class ExampleExecutor(Executor):
    def __init__(self):
        pass

    def registered(self, driver, executor_info, framework_info, slave_info):
        """
          Invoked once the executor driver has been able to successfully connect
          with Mesos.  In particular, a scheduler can pass some data to its
          executors through the FrameworkInfo.ExecutorInfo's data field.
        """
        logging.info('Executor registered')

    def reregistered(self, driver, slave_info):
        """
          Invoked when the executor re-registers with a restarted slave.
        """
        pass

    def disconnected(self, driver):
        """
          Invoked when the executor becomes "disconnected" from the slave (e.g.,
          the slave is being restarted due to an upgrade).
        """
        pass

    def launchTask(self, driver, task):
        """
          Invoked when a task has been launched on this executor (initiated via
          Scheduler.launchTasks).  Note that this task can be realized with a
          thread, a process, or some simple computation, however, no other
          callbacks will be invoked on this executor until this callback has
          returned.
        """
        # Tasks should always be run in their own thread or process.
        def run_task():
            logging.info("Running task: {}".format(task.task_id.value))
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            driver.sendStatusUpdate(update)

            # TODO: add your code here
            print(task.data)
            time.sleep(30)

            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED
            driver.sendStatusUpdate(update)
            logging.info('Task finished, sent final status update.')

        thread = Thread(target=run_task, args=())
        thread.start()

    def killTask(self, driver, task_id):
        """
          Invoked when a task running within this executor has been killed (via
          SchedulerDriver.killTask).  Note that no status update will be sent on
          behalf of the executor, the executor is responsible for creating a new
          TaskStatus (i.e., with TASK_KILLED) and invoking ExecutorDriver's
          sendStatusUpdate.
        """
        pass

    def frameworkMessage(self, driver, message):
        """
          Invoked when a framework message has arrived for this executor.  These
          messages are best effort; do not expect a framework message to be
          retransmitted in any reliable fashion.
        """
        pass

    def shutdown(self, driver):
        """
          Invoked when the executor should terminate all of its currently
          running tasks.  Note that after Mesos has determined that an executor
          has terminated any tasks that the executor did not send terminal
          status updates for (e.g., TASK_KILLED, TASK_FINISHED, TASK_FAILED,
          etc) a TASK_LOST status update will be created.
        """
        pass

    def error(self, error, message):
        """
          Invoked when a fatal error has occured with the executor and/or
          executor driver.  The driver will be aborted BEFORE invoking this
          callback.
        """
        logging.error(message)


def main():
    driver = MesosExecutorDriver(ExampleExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)


if __name__ == '__main__':
    main()