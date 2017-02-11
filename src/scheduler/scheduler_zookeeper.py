#!/usr/bin/env python2.7
from __future__ import print_function

import site
site.addsitedir('/usr/lib/python2.7/site-packages')
site.addsitedir('/usr/local/lib/python2.7/site-packages')

import logging
import os
import signal
import sys
import time
import uuid
from threading import Thread

from mesos.interface import Scheduler, mesos_pb2
from mesos.native import MesosSchedulerDriver


class MinimalScheduler(Scheduler):
    def __init__(self, executor, instances):
        self.executor = executor
        self.num_instances = instances

        self.running_instances = dict()
        self.launched_instances = dict()

        self.zoo_ids = [1, 2, 3]

    def resourceOffers(self, driver, offers):
        logging.debug("Running instances: {}, Launched instances: {}".format(len(self.running_instances), len(self.launched_instances)))
        if len(self.running_instances) + len(self.launched_instances) < self.num_instances:
            for offer in offers:
	        logging.info("Received offer with ID: {}".format(offer.id.value))

                for resource in offer.resources:
                    logging.debug("Resorces: {} ".format(resource))
                    if resource.name == "ports":
                        begin_port = resource.ranges.range[0].begin
                        end_port = resource.ranges.range[0].end

                zoo_id = self.zoo_ids.pop()

                task = mesos_pb2.TaskInfo()
                task_id = str(uuid.uuid4())
                task.task_id.value = task_id
                task.slave_id.value = offer.slave_id.value
                task.name = "zookeeper-{}".format(zoo_id)

                command = mesos_pb2.CommandInfo()
                command.shell = False

                environments = mesos_pb2.Environment()

                zoo_id_env = environments.variables.add()
                zoo_id_env.name = "ZOO_MY_ID"
                zoo_id_env.value = str(zoo_id)

                zoo_id_env = environments.variables.add()
                zoo_id_env.name = "ZOO_PORT"
                zoo_id_env.value = str(begin_port)

                zoo_servers_env = environments.variables.add()
                zoo_servers_env.name = "ZOO_SERVERS"
                zoo_servers_env.value = "server.1=zoo1:2888:3888 server.2=zoo2:2888:3888 server.3=zoo3:2888:3888"

                command.environment.MergeFrom(environments)
                task.command.MergeFrom(command)

                container = mesos_pb2.ContainerInfo()
                container.type = 1 #mesos_pb2.ContainerInfo.Type.DOCKER

                docker = mesos_pb2.ContainerInfo.DockerInfo()
                docker.image = "zookeeper:3.4.9"
                docker.network = 2 # mesos_pb2.ContainerInfo.DockerInfo.Network.BRIDGE
                docker.force_pull_image = False

                container.docker.MergeFrom(docker)
                task.container.MergeFrom(container)

                cpus = task.resources.add()
                cpus.name = 'cpus'
                cpus.type = mesos_pb2.Value.SCALAR
                cpus.scalar.value = 0.5

                mem = task.resources.add()
                mem.name = 'mem'
                mem.type = mesos_pb2.Value.SCALAR
                mem.scalar.value = 128

                port = task.resources.add()
                port.name = 'ports'
                port.type = mesos_pb2.Value.RANGES
                _range = port.ranges.range.add()
                _range.begin = begin_port
                _range.end = begin_port + 3

                tasks = [task]
                driver.launchTasks(offer.id, tasks)
                self.launched_instances[task_id] = zoo_id

    def statusUpdate(self, driver, update):
        task_id = update.task_id.value
        logging.info("Task {} is in state {}".format(
            task_id, mesos_pb2.TaskState.Name(update.state)))

        if mesos_pb2.TaskState.Name(update.state) == "TASK_RUNNING":
            zoo_id = self.launched_instances.pop(task_id)
            self.running_instances[task_id] = zoo_id
        elif mesos_pb2.TaskState.Name(update.state) == "TASK_FAILED" \
               or mesos_pb2.TaskState.Name(update.state) == "TASK_FINISHED":
            try:
                self.zoo_ids.append(self.launched_instances.pop(task_id))
            except KeyError, err:
                pass
            try:
                self.zoo_ids.append(self.running_instances.pop(task_id))
            except KeyError, err:
                pass

def main(master):
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s %(levelname)s] %(message)s')

    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = 'ZK_Executor'
    executor.name = executor.executor_id.value
    #executor.command.value = os.path.abspath('./executor-minimal.py')

    framework = mesos_pb2.FrameworkInfo()
    framework.user = ''  # the current user
    framework.name = 'ZK_Framework'
    framework.checkpoint = True
    framework.principal = framework.name

    implicitAcknowledgements = 1

    driver = MesosSchedulerDriver(
        MinimalScheduler(executor, 3),
        framework,
        master,
        implicitAcknowledgements
    )

    def signal_handler(signal, frame):
        driver.stop()

    def run_driver_thread():
        status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        driver.stop()
        sys.exit(status)

    driver_thread = Thread(target=run_driver_thread, args=())
    driver_thread.start()

    logging.info('Scheduler running, Ctrl-C to exit')
    signal.signal(signal.SIGINT, signal_handler)

    while driver_thread.is_alive():
        time.sleep(1)

    logging.info('Framework finished.')
    sys.exit(0)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: {} <mesos_master>".format(sys.argv[0]))
        sys.exit(1)
    else:
        main(sys.argv[1])