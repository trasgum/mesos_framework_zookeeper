#!/usr/bin/env python2.7
from __future__ import print_function

import os.path as path
from os import getenv
import logging
import uuid
import json
from pymesos import Scheduler
from utils import get_mesos_master, get_mesos_slaves_status
from addict import Dict

logging.basicConfig(level=getenv("FW_LOG_LEVEL", logging.DEBUG),
                    format='[%(asctime)s %(levelname)s %(module)s:%(funcName)s] %(message)s'
                    )
log = logging.getLogger(__name__)

FOREVER = 0xFFFFFFFF


class ZookeeperScheduler(Scheduler):
    def __init__(self, executor, principal, role, instances, zk_resources, zk_driver):
        # Refactor -> Read reserved_resources from mesos_master
        # Check if all resources have been reserved
        ## Reserve all non reserved resources
        # Launch tasks from offers with reserved resources

        self.executor = executor
        self.principal = principal
        self.role = role
        self.num_instances = instances
        self.zk_resources = zk_resources
        self.zk_conn = zk_driver
        self.running_instances = dict()
        self.launched_instances = dict()
        self.reserved_resources = list()
        self.zoo_ids = set(map(lambda x: x+1, range(instances)))

        mesos_master_dict = get_mesos_master(self.zk_conn)
        mesos_state = get_mesos_slaves_status(mesos_master_dict)

        for slave in mesos_state['slaves']:
            try:
                resources = slave['reserved_resources_full'][self.role]
                self.reserved_resources.append(
                    {"agent_hostname": slave['hostname'], "resources": resources})
                self.remove_zoo_ids(resources)
            except KeyError:
                log.info("Slave {} does not have reserved resources".format(slave['id']))
                pass

    def registered(self, driver, framework_id, master_info):
        log.info("Registered with framework id: {}".format(framework_id.value))

    def resourceOffers(self, driver, offers):
        tasks_to_launch = list()

        log.debug("Launched instances: {}".format(len(self.running_instances) + len(self.launched_instances)))

        # Refactor
        for offer in offers:
            # log.debug("Received offer {}".format(json.dumps(offer, indent=2)))
            log.info("Received offer with ID: {}".format(offer.id.value))

            if len(self.running_instances) + len(self.launched_instances) >= self.num_instances:
                log.info("Offer {}: DECLINED: all task are running".format(offer.id.value))
                driver.declineOffer(offer.id, filters=self._filters(FOREVER))
                continue

            log.info("Filtering non reserved resources")
            filtered_offer = offer.copy()
            map(lambda x: filtered_offer.resources.remove(x),
                filter(lambda resource: resource['reservation'] == {},
                       (resource for resource in filtered_offer.resources)))

            if filtered_offer.resources:
                if filtered_offer['agent']['hostname'] not in [resources['agent_hostname']
                                                               for resources in self.reserved_resources]:
                    log.info("Saving already reserved resources to reserved_resources")
                    self.reserved_resources.append(
                        {"agent_hostname": filtered_offer.url.address.hostname, "resources": filtered_offer.resources})
                    self.remove_zoo_ids(filtered_offer.resources)

                log.info("Creating task from already reserved resources")
                task = self.zookeeper_task(offer['slave_id']['value'], filtered_offer.resources)
                tasks_to_launch.append({"offer": offer, "task": task})
            else:
                if len(self.reserved_resources) < self.num_instances:
                    log.info("Reserving resources...")
                    zoo_id = self.zoo_ids.pop()
                    self.reserved_resources.append(self.reserve_resources(offer, zoo_id, self.zk_conn))
                else:
                    log.info("Offer {}: Declined all resources were reserved")
                    driver.declineOffer(offer.id)

        if len(tasks_to_launch) > 0:
            log.info("Launching {} tasks".format(len(tasks_to_launch)))
            log.debug("Tasks to launch => {}, Offer <= {}".format(
                [task_info['task'] for task_info in tasks_to_launch.values()],
                [task_info['offer'] for task_info in tasks_to_launch.values()]
                ))

            try:
                # Tasks can only be aggregated if offer belong to same agent
                for task in tasks_to_launch.values():
                    driver.launchTasks(
                        task['offer']['id'],
                        task['task']
                    )
                # driver.launchTasks(
                #    [task['offer']['id'] for task in tasks_to_launch.values()],
                #    [task['task'] for task in tasks_to_launch.values()]
                #    )

                for zoo_id, task in tasks_to_launch.items():
                    log.debug("task {} => launched_instances".format(task['task']['task_id']['value']))
                    self.launched_instances[task['task']['task_id']['value']] = {
                         "_id": zoo_id,
                         "agent_hostname": task['offer']['url']['address']['hostname'],
                         "cport": 2181,
                         "lport": 2888,
                         "eport": 3888
                         }
            except Exception as err:
                log.exception("Something wrong launching tasks: {}".format(err))

    def statusUpdate(self, driver, update):
        task_id = update.task_id.value
        log.debug("Task status: {}".format(update))
        log.info("Task {} is {} -> {} : {}".format(task_id, update.state, update.reason, update.message))

        if update.state == "TASK_RUNNING":
            zoo_id = self.launched_instances.pop(task_id)
            self.running_instances[task_id] = zoo_id
            self.__write_status_in_zookeeper(self.zk_conn,
                                             path.join('/', self.executor.name, '/instances'),
                                             json.dumps(self.running_instances, ensure_ascii=True)
                                             )

        elif update.state == "TASK_FAILED" or update.state == "TASK_FINISHED":
            self.running_instances.pop(task_id)
            self.__write_status_in_zookeeper(self.zk_conn,
                                             path.join('/', self.executor.name, '/instances'),
                                             json.dumps(self.running_instances, ensure_ascii=True)
                                             )

        elif update.state == "TASK_LOST":
            # TODO reconcile task
            pass
        elif update.state == "TASK_ERROR":
            self.launched_instances.pop(task_id)

        log.info("Running instances: {}, Launched instances: {}".format(len(self.running_instances),
                                                                        len(self.launched_instances)
                                                                        ))

    def reserve_resources(self, offer, zoo_id, driver):
        cpus, mem, gpus, port_ranges = self.get_resources(offer)
        zk_task_port_range = [(_range[0], _range[0] + 2) for _range in port_ranges
                              if len(range(_range[0], _range[1], 1)) >= 2][0]

        if offer.url.address.hostname in [reserved_resource['agent_hostname'] for reserved_resource
                                          in self.reserved_resources]:
            # log.debug("reserved_resources: {}".format(self.reserved_resources))
            log.info("Offer {}: DECLINED: the agent {} have already reserved resources".format(
                offer.id.value,
                offer.url.address.hostname
            ))
            driver.declineOffer(offer.id)

        elif cpus < self.zk_resources.cpu:
            log.info("Offer {}: DECLINED: offer does not complain min requirements: cpus: {}".format(
                offer.id.value,
                cpus
            ))
            driver.declineOffer(offer.id)

        elif mem < self.zk_resources.mem:
            log.info("Offer {}: DECLINED: offer does not complain min requirements: mem: {}".format(
                offer.id.value,
                mem
            ))
            driver.declineOffer(offer.id)

        elif len(zk_task_port_range) is not 2:
            log.info("Offer {}: DECLINED: offer does not complain min requirements: three consecutive ports".format(
                offer.id.value
            ))
            driver.declineOffer(offer.id)
        # TODO: add disk resources to condition
        else:
            log.info("Reserving zookeeper resources to task id: {} from offer {}".format(
                zoo_id,
                offer.id.value
            ))
            resources = self.zookeeper_task_resources(zoo_id,
                                                      self.principal,
                                                      self.role,
                                                      self.zk_resources.cpu,
                                                      self.zk_resources.mem,
                                                      zk_task_port_range,
                                                      self.zk_resources.disk_data,
                                                      self.zk_resources.disk_log
                                                      )
            self.reserveResources(offer, resources, driver)
            return {"agent_hostname": offer.url.address.hostname, "resources": resources}
            # self.__write_status_in_zookeeper(self.zk_conn,
            #                                  join('/', self.executor.name, '/resources'),
            #                                 json.dumps(self.reserved_resources, ensure_ascii=True))

    @staticmethod
    def zookeeper_task_resources(zoo_id,
                                 principal,
                                 role,
                                 task_cpu,
                                 task_mem,
                                 task_port_range,
                                 task_disk_data_size,
                                 task_disk_log_size):

        resources = list()
        labels = list()
        reservation = Dict()

        label_zoo_id = Dict()
        label_zoo_id.key = "zoo_id"
        label_zoo_id.value = str(zoo_id)
        labels.append(label_zoo_id)
        reservation.labels.labels = labels

        reservation.principal = principal

        cpus = Dict()
        cpus.name = 'cpus'
        cpus.type = 'SCALAR'
        cpus.scalar.value = task_cpu
        cpus.role = role
        cpus.reservation = reservation
        resources.append(cpus)

        mem = Dict()
        mem.name = 'mem'
        mem.type = 'SCALAR'
        mem.scalar.value = task_mem
        mem.role = role
        mem.reservation = reservation
        resources.append(mem)

        disk_d = Dict()
        disk_d.name = 'disk'
        disk_d.type = 'SCALAR'
        disk_d.scalar.value = task_disk_data_size
        disk_d.role = role
        disk_d.reservation = reservation
        resources.append(disk_d)

        disk_l = Dict()
        disk_l.name = 'disk'
        disk_l.type = 'SCALAR'
        disk_l.scalar.value = task_disk_log_size
        disk_l.role = role
        disk_d.reservation = reservation
        resources.append(disk_d)

        disk_l.reservation = reservation
        resources.append(disk_l)

        task_ports = Dict()
        range_list = list()
        task_ports.name = 'ports'
        task_ports.type = 'RANGES'
        task_ports.role = role
        _range = Dict(
            begin=task_port_range[0],
            end=task_port_range[1]
        )
        range_list.append(_range)
        task_ports.ranges = Dict(
            range=range_list
        )
        task_ports.reservation = reservation
        resources.append(task_ports)

        return resources

    def zookeeper_task(self, agent_id, resources):
        zoo_id = self.get_zoo_id_label(resources)[0]
        # TODO implement
        # zk_client_port = get_port_range_from_resources(resources)

        task = Dict()
        task_id = str(uuid.uuid4())
        task.task_id.value = task_id
        task.agent_id.value = agent_id
        task.name = "zookeeper-{}".format(zoo_id)

        command = Dict()
        command.shell = False

        environments = list()
        # # TODO[trasgum] construct dynamically based in num_instances and reserved ports
        #zoo_servers = ""
        #for i in map(lambda x: x+1, range(self.num_instances)):
        #    zoo_servers += "server." + str(i) + "=zookeeper-" + str(i) + "." + self.executor.name + ".mesos:2888:3888 "

        zoo_servers = self.get_cluster_string()

        self.__add_environment_variable(environments, key="ZOO_SERVERS", value=zoo_servers.strip())
        self.__add_environment_variable(environments, key="ZOO_MY_ID", value=str(zoo_id))
        self.__add_environment_variable(environments, key="ZOO_PORT", value=str(zk_client_port))
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

        container.docker = docker
        task.container = container
        task.resources = resources
        return task

    def remove_zoo_ids(self, resources):
        filter(lambda x: x not in self.get_zoo_id_label(resources), self.zoo_ids)

    def get_cluster_string(self, resources):
        cluster_string = ""
        # TODO implement
        list_zoo_ids = self.get_zoo_id_label(resources)
        for zoo_id in list_zoo_ids:
            zk_client_port = get_port_range_from_resources(zoo_id, resources)
            # for zoo_id in self.reserved_resources:
                # ports = [resource for resource in zoo_id.resources if resource.name is "ports"]
            lport = zk_client_port + 1
            eport = zk_client_port + 2
            cluster_string += "server." + str(zoo_id) + "=zookeeper-" + str(zoo_id) \
                              + "." + executor_name \
                              + ".mesos:" \
                              + str(lport) + ":" \
                              + str(eport)
        return cluster_string

    @staticmethod
    def reserveResources(offer, resources, driver):
        reserve_operation = [Dict(
            type='RESERVE',
            reserve=Dict(
                resources=resources
            )
        )]
        # log.debug("operation: {}".format(json.dumps(reserve_operation, indent=2)))
        driver.acceptOffers(offer.id, operations=reserve_operation)

    @staticmethod
    def _filters(seconds):
        f = dict(refuse_seconds=seconds)
        return f

    @staticmethod
    def __add_environment_variable(list_environments, key, value):
        environment = Dict()
        environment.name = key
        environment.value = value
        list_environments.append(environment)

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
            # volume.mode = 2 # mesos_pb2.Volume.Mode.RO
        volumes.append(volume)

    @staticmethod
    def __write_status_in_zookeeper(zk_con, path, json_data):
        try:
            zk_con.ensure_path(path.join('/', path))
            zk_con.set(path.join('/', path), json_data)
        except Exception as err:
            log.error("Status cannot be written to zookeeper: {}".format(err))

    @staticmethod
    def get_resources(offer):
        cpus, mem, gpus = 0.0, 0.0, 0
        port_list = list()
        for resource in offer['resources']:
            if resource['name'] == 'cpus':
                cpus = float(resource['scalar']['value'])
            elif resource['name'] == 'mem':
                mem = float(resource['scalar']['value'])
            elif resource['name'] == 'gpus':
                gpus = int(resource['scalar']['value'])
            elif resource['name'] == "ports":
                for _range in resource.ranges.range:
                    port_list.append((int(_range.begin), int(_range.end)))

        return cpus, mem, gpus, port_list

    @staticmethod
    def get_reservation_info(offer):
        reservation_cpu, reservation_mem, reservation_gpu, reservation_ports = Dict(), Dict(), Dict(), Dict()
        for resource in offer['resources']:
            log.debug("resource: {}".format(resource))
            if resource['name'] == 'cpus':
                reservation_cpu = Dict(resource['reservation'])
            elif resource['name'] == 'mem':
                reservation_mem = Dict(resource['reservation'])
            elif resource['name'] == 'gpus':
                reservation_gpu = Dict(resource['reservation'])
            elif resource['name'] == 'ports':
                reservation_ports = Dict(resource['reservation'])

        return reservation_cpu, \
               reservation_mem, \
               reservation_gpu, \
               reservation_ports

    @staticmethod
    def get_zoo_id_label(resources):
        resource_zoo_ids = list()
        labels = (label['reservation']['labels'] for label in (resource for resource in resources))
        for label in labels:
            for l in label['labels']:
                if l['key'] == 'zoo_id':
                    resource_zoo_ids.append(l['value'])
        set_zoo_ids = set(resource_zoo_ids)
        return set_zoo_ids if len(set_zoo_ids) == 1 else list()



