#!/usr/bin/env python2.7
from __future__ import print_function

from os.path import join
from os import getenv
import logging
import uuid
import json
from pymesos import Scheduler
from addict import Dict

logging.basicConfig(level=getenv("FW_LOG_LEVEL", logging.DEBUG),
                    format='[%(asctime)s %(levelname)s %(module)s:%(funcName)s] %(message)s'
                    )
log = logging.getLogger(__name__)

class ZookeeperScheduler(Scheduler):
    def __init__(self, executor, instances, zk_driver):
        self.executor = executor
        self.num_instances = instances
        self.zk_conn = zk_driver
        self.running_instances = dict()
        self.launched_instances = dict()
        self.zoo_ids = map(lambda x: x+1, range(instances))

    def registered(self, driver, framework_id, master_info):
        log.info("Registered with framework id: {}".format(framework_id.value))

    def resourceOffers(self, driver, offers):
        log.debug("Launched instances: {}".format(len(self.running_instances) + len(self.launched_instances)))
        for offer in offers:
            log.debug("Received offer with ID: {}".format(offer.id.value))
            if len(self.running_instances) + len(self.launched_instances) >= self.num_instances:
                log.info("Offer {}: DECLINED because all task are running".format(offer.id.value))
                driver.declineOffer(offer.id)
                # TODO: implement reviveOffers if some task fail
                # driver.suppressOffers()
                continue

            elif offer.url.address.hostname in [instance['agent_hostname'] for instance in self.running_instances.values()]:
                log.info("Offer {}: DECLINED because a task is running in agent {}".format(offer.id.value,
                                                                                           offer.url.address.hostname
                                                                                           ))
                driver.declineOffer(offer.id)
                continue
            else:
                for resource in offer.resources:
                    log.debug("Resources: {} ".format(resource))
                    # if resource.name == "ports":
                    #    begin_port = resource.ranges.range[0].begin
                    #    end_port = resource.ranges.range[0].end

                log.debug("ZK id list = {}".format(self.zoo_ids))
                zoo_id = self.zoo_ids.pop()
                zoo_task = self.zookeeper_task(offer, zoo_id)

                log.info("Launching task {} from offer ID: {}". format(zoo_task.task_id.value, offer.id.value))
                driver.launchTasks(offer.id, [zoo_task])
                # TODO[trasgum]: reserve three ports
                self.launched_instances[zoo_task.task_id.value] = {"_id": zoo_id,
                                                                   "agent_hostname": offer.url.address.hostname,
                                                                   "cport": 2181,
                                                                   "lport": 2888,
                                                                   "eport": 3888
                                                                  }

    def statusUpdate(self, driver, update):
        task_id = update.task_id.value
        log.info("Task {} is in state {}".format(task_id, update.state))
        logging.debug("TRUCO: zk_conn: {} {}".format(self.zk_conn, self.num_instances))

        if update.state == "TASK_RUNNING":
            zoo_id = self.launched_instances.pop(task_id)
            self.running_instances[task_id] = zoo_id
            self.__write_status_in_zookeeper(self.zk_conn,
                                             join('/', self.executor.name, '/instances'),
                                             json.dumps(self.running_instances, ensure_ascii=True)
                                             )

        elif update.state == "TASK_FAILED" or update.state == "TASK_FINISHED":
            self.zoo_ids.append(self.running_instances.pop(task_id)['_id'])
            self.__write_status_in_zookeeper(self.zk_conn,
                                             join('/', self.executor.name, '/instances'),
                                             json.dumps(self.running_instances, ensure_ascii=True)
                                             )

        log.info("Running instances: {}, Launched instances: {}".format(len(self.running_instances),
                                                                            len(self.launched_instances)
                                                                            ))

    def zookeeper_task(self, offer, zoo_id):
        task = Dict()
        task_id = str(uuid.uuid4())
        task.task_id.value = task_id
        task.agent_id.value = offer.agent_id.value
        task.name = "zookeeper-{}".format(zoo_id)

        command = Dict()
        command.shell = False

        environments = list()
        # # TODO[trasgum] construct dynamically based in num_instances
        zoo_servers = ""
        for i in map(lambda x: x+1, range(self.num_instances)):
            zoo_servers += "server." + str(i) + "=zookeeper-" + str(i) + "." + self.executor.name + ".mesos:2888:3888 "

        self.__add_environment_variable(environments, key="ZOO_SERVERS", value=zoo_servers.strip())
        self.__add_environment_variable(environments, key="ZOO_MY_ID", value=str(zoo_id))
        self.__add_environment_variable(environments, key="ZOO_PORT", value="2181")
        log.debug("VARS: {}".format([var for var in environments]))

        command.environment.variables = environments
        task.command = command

        container = Dict()
        container.type = "DOCKER"  # mesos_pb2.ContainerInfo.Type.DOCKER

        docker = Dict()
        docker.image = "zookeeper:3.4.9"
        docker.network = "BRIDGE"  # mesos_pb2.ContainerInfo.DockerInfo.Network.BRIDGE
        docker.force_pull_image = False

        volumes = list()
        self.__add_docker_volume(volumes, container_path="/data", host_path="/var/zookeeper/data", mode="RW")
        self.__add_docker_volume(volumes, container_path="/datalog", host_path="/var/zookeeper/datalog", mode="RW")

        docker.volumes = volumes

        # parameters = list()
        # dns = Dict()
        # dns.key = "--dns"
        # dns.value = "[172.17.0.2, 8.8.8.8, 8.8.4.4]"
        # parameters.append(dns)
        #
        # dns_search = Dict()
        # dns_search.key = "--dns-search"
        # dns_search.value = "mesos"
        # parameters.append(dns_search)

        # docker.parameters = parameters
        container.docker = docker
        task.container = container

        resources = list()
        cpus = Dict()
        cpus.name = 'cpus'
        cpus.type = 'SCALAR'
        cpus.scalar.value = 0.5
        resources.append(cpus)

        mem = Dict()
        mem.name = 'mem'
        mem.type = 'SCALAR'
        mem.scalar.value = 128
        resources.append(mem)

        # port = task.resources.add()
        # port.name = 'ports'
        # port.type = mesos_pb2.Value.RANGES
        # range = port.ranges.range.add()
        # range.begin = begin_port
        # range.end = begin_port + 2

        task.resources = resources

        return task

    @staticmethod
    def __add_environment_variable(list_environemnts, key, value):
        environment = Dict()
        environment.name = key
        environment.value = value
        list_environemnts.append(environment)

    @staticmethod
    def __add_docker_volume(volumes, container_path, host_path, mode):
        volume = Dict()
        volume.container_path = container_path
        volume.host_path = host_path
        if mode is "RW" or mode is "RO":
            volume.mode = mode
        else:
            raise AttributeError('Invalid volume mode')
            # volume.mode = 1  # mesos_pb2.Volume.Mode.RW
            # volume_data.mode = 2 # mesos_pb2.Volume.Mode.RO
        volumes.append(volume)

    @staticmethod
    def __write_status_in_zookeeper(zk_con, path, json_data):
        try:
            zk_con.ensure_path(join('/', path))
            zk_con.set(join('/', path), json_data)
        except Exception as err:
            log.error("Status cannot be written to zookeeper: {}".format(err))

