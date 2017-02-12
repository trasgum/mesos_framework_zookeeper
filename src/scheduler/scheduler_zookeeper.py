#!/usr/bin/env python2.7
from __future__ import print_function

import site
site.addsitedir('/usr/lib/python2.7/site-packages')
site.addsitedir('/usr/local/lib/python2.7/site-packages')

import logging
import os
# import signal
import sys
import time
import uuid
from threading import Thread
import json

from mesos.interface import Scheduler, mesos_pb2
from mesos.native import MesosSchedulerDriver

from kazoo.client import KazooClient


class MinimalScheduler(Scheduler):
    def __init__(self, executor, instances, zk_driver):
        self.executor = executor
        self.num_instances = instances

        self.running_instances = dict()
        self.launched_instances = dict()

        self.zoo_ids = map(lambda x: x+1, range(instances))

        self.zk_conn = zk_driver

    def resourceOffers(self, driver, offers):
        logging.debug("Running instances: {}, Launched instances: {}".format(len(self.running_instances), len(self.launched_instances)))
        if len(self.running_instances) + len(self.launched_instances) < self.num_instances:
            for offer in offers:
                logging.info("Received offer with ID: {}".format(offer.id.value))
                if offer.hostname in [instance['hostname'] for instance in self.running_instances.values()]:
                    # TODO[trasgum]: decline offer
                    break

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
                zoo_id_env.value = "2181" #str(begin_port)

                zoo_servers_env = environments.variables.add()
                zoo_servers_env.name = "ZOO_SERVERS"
                # zoo_servers_env.value = "server.1=zookeeper-1:" + str(int(begin_port + 1)) + ":" + str(int(begin_port + 2)) \
                #        + " server.2=zookeeper-2:" + str(int(begin_port + 1)) + ":" + str(int(begin_port + 2)) \
                #        + " server.3=zookeeper-3:" + str(int(begin_port + 1)) + ":" + str(int(begin_port + 2))
                # TODO[truco] construct dynamically based in num_instances
                zoo_servers_env.value = "server.1=zookeeper-1:2888:3888 server.2=zookeeper-2:2888:3888 server.3=zookeeper-3:2888:3888"
                logging.debug("ZOO_SERVERS: {}".format(zoo_servers_env.value))

                command.environment.MergeFrom(environments)
                task.command.MergeFrom(command)

                container = mesos_pb2.ContainerInfo()
                container.type = 1 #mesos_pb2.ContainerInfo.Type.DOCKER

                docker = mesos_pb2.ContainerInfo.DockerInfo()
                docker.image = "zookeeper:3.4.9"
                docker.network = 2 # mesos_pb2.ContainerInfo.DockerInfo.Network.BRIDGE
                docker.force_pull_image = False

                volume_data = container.volumes.add()
                volume_data.container_path = "/data"  # Path in container
                volume_data.host_path = "/var/zookeeper/data"  # Path on host
                volume_data.mode = 1  # mesos_pb2.Volume.Mode.RW
                # volume_data.mode = 2 # mesos_pb2.Volume.Mode.RO

                volume_datalog = container.volumes.add()
                volume_datalog.container_path = "/datalog"  # Path in container
                volume_datalog.host_path = "/var/zookeeper/datalog"  # Path on host
                volume_datalog.mode = 1  # mesos_pb2.Volume.Mode.RW
                # volume_data.mode = 2 # mesos_pb2.Volume.Mode.RO

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

                # port = task.resources.add()
                # port.name = 'ports'
                # port.type = mesos_pb2.Value.RANGES
                # range = port.ranges.range.add()
                # range.begin = begin_port
                # range.end = begin_port + 2

                tasks = [task]
                driver.launchTasks(offer.id, tasks)
                # self.launched_instances[task_id] = { "_id": zoo_id,
                #                                     "cport": begin_port,
                #                                     "lport": int(begin_port) + 1,
                #                                     "eport": int(begin_port) + 2
                #                                     }
                self.launched_instances[task_id] = { "_id": zoo_id,
                                                     "cport": 2181,
                                                     "lport": 2888,
                                                     "eport": 3888,
                                                     "agent_hostname": offer.hostname
                                                     }

    def statusUpdate(self, driver, update):
        task_id = update.task_id.value
        logging.info("Task {} is in state {}".format(
            task_id, mesos_pb2.TaskState.Name(update.state)))

        if mesos_pb2.TaskState.Name(update.state) == "TASK_RUNNING":
            zoo_id = self.launched_instances.pop(task_id)
            self.running_instances[task_id] = zoo_id
            self.zk_conn.set('/zookeeper-fw/instances', json.dumps(self.running_instances, ensure_ascii=True))
        elif mesos_pb2.TaskState.Name(update.state) == "TASK_FAILED" \
               or mesos_pb2.TaskState.Name(update.state) == "TASK_FINISHED":
            try:
                self.zoo_ids.append(self.launched_instances.pop(task_id)['_id'])
            except KeyError:
                pass
            try:
                self.zoo_ids.append(self.running_instances.pop(task_id)['_id'])
                self.zk_conn.set('/zookeeper-fw/instances', json.dumps(self.running_instances, ensure_ascii=True))
            except KeyError:
                pass

def main(master):
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s %(levelname)s] %(message)s')

    executor_name = 'zk-executor'
    framework_name = 'zk-framework'
    num_instances = os.getenv('ZK_NUM_INSTANCES', 3)

    if 'zk://' in master:
        zk_host = master.split('/')[2]
        logging.debug("starting connection to zookeeper {}".format(master.split('/')[2]))
        zk_conn = KazooClient(zk_host)  # client_id=framework_name)
        zk_conn.start()
        zk_conn.ensure_path('/zookeeper-fw/instances')

    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = executor_name
    executor.name = executor.executor_id.value
    # executor.command.value = os.path.abspath('./executor-minimal.py')

    framework = mesos_pb2.FrameworkInfo()
    framework.user = ''  # the current user
    framework.name = framework_name
    framework.checkpoint = True
    framework.principal = framework.name

    implicitAcknowledgements = 1

    driver = MesosSchedulerDriver(
        MinimalScheduler(executor, instances=num_instances, zk_driver=zk_conn),
        framework,
        master,
        implicitAcknowledgements
    )

    # def signal_handler(signal, frame):
    #    driver.stop()

    def run_driver_thread():
        status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        driver.stop()
        sys.exit(status)

    driver_thread = Thread(target=run_driver_thread, args=())
    driver_thread.start()

    logging.info('Scheduler running...')
    # logging.info('Scheduler running, Ctrl-C to exit')
    # signal.signal(signal.SIGINT, signal_handler)

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
