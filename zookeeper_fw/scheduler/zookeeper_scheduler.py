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

FOREVER = 0xFFFFFFFF

class ZookeeperScheduler(Scheduler):
    def __init__(self, executor, principal, instances, zk_resources, zk_driver):
        self.executor = executor
        self.principal = principal
        self.num_instances = instances
        self.zk_conn = zk_driver
        self.running_instances = dict()
        self.launched_instances = dict()
        # TODO get reserved resources from zk_driver else Dict()
        self.reserved_resources = list()
        self.zoo_ids = map(lambda x: x+1, range(instances))
        self.zk_resources = zk_resources

    def registered(self, driver, framework_id, master_info):
        log.info("Registered with framework id: {}".format(framework_id.value))

    def resourceOffers(self, driver, offers):
        tasks_to_launch = Dict()

        log.debug("Launched instances: {}".format(len(self.running_instances) + len(self.launched_instances)))

        for offer in offers:
            # log.debug("Received offer {}".format(json.dumps(offer, indent=2)))
            log.info("Received offer with ID: {}".format(offer.id.value))

            cpus, mem, gpus, port_ranges = self.get_resources(offer)
            zk_task_port_range = [(_range[0], _range[0] + 2) for _range in port_ranges
                                  if len(range(_range[0], _range[1], 1)) >= 2][0]

            if len(self.running_instances) + len(self.launched_instances) >= self.num_instances:
                log.info("Offer {}: DECLINED: all task are running".format(offer.id.value))
                driver.declineOffer(offer.id, filters=self._filters(FOREVER))
                continue

            if len(self.zoo_ids) > 0:
                log.debug("Reserving resources...")
                if offer.url.address.hostname in [reserved_resource['agent_hostname'] for reserved_resource
                                                    in self.reserved_resources]:
                    # log.debug("reserved_resources: {}".format(self.reserved_resources))
                    log.info("Offer {}: DECLINED: this host have already resources in agent {}".format(
                                                                                                  offer.id.value,
                                                                                                  offer.url.address.hostname
                                                                                                ))
                    driver.declineOffer(offer.id)
                    continue

                elif (cpus < self.zk_resources.cpu
                        or mem < self.zk_resources.mem):
                    #TODO: add disk resources to condition
                    log.info("Offer {}: DECLINED: offer does not complain min requirements: cpus: {}, mem: {}".format(
                                                                                                                    offer.id.value,
                                                                                                                    cpus,
                                                                                                                    mem
                                                                                                                     ))
                    driver.declineOffer(offer.id)
                    continue
                elif len(zk_task_port_range) is not 2:
                    log.info("Offer {}: DECLINED: offer does not complain min requirements: three consecutive ports".format(
                        offer.id.value
                    ))
                    driver.declineOffer(offer.id)
                    continue
                else:
                    #TODO: add disk resources
                    log.debug("Zookeeper required resources: cpu: {}, mem: {} and {} offered ports".format(
                                                                                self.zk_resources.cpu * self.num_instances,
                                                                                self.zk_resources.mem * self.num_instances,
                                                                                self.num_instances * 3
                                                                                ))

                    zoo_id = self.zoo_ids.pop()
                    log.info("Reserving zookeeper resources to task id: {} from offer {}".format(
                        zoo_id,
                        offer.id.value
                    ))
                    resources = self.zookeeper_resources(zoo_id, zk_task_port_range)
                    self.reserve_zookeeper_resources(offer, resources, driver)
                    self.reserved_resources.append({"agent_hostname": offer.url.address.hostname, "resources": resources})
                    self.__write_status_in_zookeeper(self.zk_conn,
                                                     join('/', self.executor.name, '/resources'),
                                                     json.dumps(self.reserved_resources, ensure_ascii=True))

            else:
                log.info("All resources reserved, num reservations: {}".format(len(self.reserved_resources)))
                log.debug("Filtering non reserved resources")
                filtered_offer = offer.copy()
                map(lambda x: filtered_offer.resources.remove(x),
                    filter(lambda resource: resource['reservation'] == {},
                           (resource for resource in filtered_offer.resources)))

                if filtered_offer.resources:
                    cpus_reservation, \
                    mem_reservation, \
                    gpus_reservation, \
                    port_ranges_reservation = self.get_reservation_info(filtered_offer)

                    if cpus_reservation.principal == self.principal \
                            and mem_reservation.principal == self.principal \
                            and port_ranges_reservation.principal == self.principal:

                        log.debug("All resources have {} princiapl assigned".format(self.principal))
                        for label in cpus_reservation.labels.labels:
                            if label.key == "zoo_id":
                                zoo_id = label.value

                        # log.debug("offer: {}".format(json.dumps(offer, indent=2)))
                        log.info("Creating task for zookeeper: {} from offer {}".format(zoo_id, offer.id.value))
                        tasks_to_launch[zoo_id] = {
                                                    "task": self.zookeeper_task(offer.agent_id.value,
                                                                                cpus,
                                                                                mem,
                                                                                zk_task_port_range,
                                                                                zoo_id
                                                                                ),
                                                    "offer": offer
                                                   }

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
                                             join('/', self.executor.name, '/instances'),
                                             json.dumps(self.running_instances, ensure_ascii=True)
                                             )

        elif update.state == "TASK_FAILED" or update.state == "TASK_FINISHED":
            # self.zoo_ids.append(self.running_instances.pop(task_id)['_id'])
            self.running_instances.pop(task_id)
            self.__write_status_in_zookeeper(self.zk_conn,
                                             join('/', self.executor.name, '/instances'),
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

    def zookeeper_resources(self, zoo_id, zk_task_port_range):
        resources = list()
        labels = list()
        reservation = Dict()

        label_zoo_id = Dict()
        label_zoo_id.key = "zoo_id"
        label_zoo_id.value = str(zoo_id)
        labels.append(label_zoo_id)
        reservation.labels.labels = labels

        reservation.principal = self.principal

        log.debug("Reserving {} cpu(s)".format(self.zk_resources.cpu))
        cpus = Dict()
        cpus.name = 'cpus'
        cpus.type = 'SCALAR'
        cpus.scalar.value = self.zk_resources.cpu
        cpus.role = "zk-framework-r"
        cpus.reservation = reservation
        resources.append(cpus)

        log.debug("Reserving {} MB of mem".format(self.zk_resources.mem))
        mem = Dict()
        mem.name = 'mem'
        mem.type = 'SCALAR'
        mem.scalar.value = self.zk_resources.mem
        mem.role = "zk-framework-r"
        mem.reservation = reservation
        resources.append(mem)

        log.debug("Reserving {} MB of disk for disk data".format(self.zk_resources.disk_data))
        disk_d = Dict()
        disk_d.name = 'disk'
        disk_d.type = 'SCALAR'
        disk_d.scalar.value = self.zk_resources.disk_data
        disk_d.role = "zk-framework-r"
        # label_disk_data = Dict()
        # label_disk_data.key = "disk"
        # label_disk_data.value = "data"
        # labels.append(label_disk_data)
        # reservation.labels.labels = labels
        disk_d.reservation = reservation
        resources.append(disk_d)

        log.debug("Reserving {} MB of disk for disk log".format(self.zk_resources.disk_log))
        disk_l = Dict()
        disk_l.name = 'disk'
        disk_l.type = 'SCALAR'
        disk_l.scalar.value = self.zk_resources.disk_data
        disk_l.role = "zk-framework-r"
        # label_disk_log = Dict()
        # label_disk_log.key = "disk"
        # label_disk_log.value = "log"
        # labels.append(label_disk_log)
        # reservation.labels.labels = labels
        disk_d.reservation = reservation
        resources.append(disk_d)

        disk_l.reservation = reservation
        resources.append(disk_l)

        log.debug("Reserving {} to {} ports".format(zk_task_port_range[0], zk_task_port_range[1]))
        task_ports = Dict()
        range_list = list()
        task_ports.name = 'ports'
        task_ports.type = 'RANGES'
        task_ports.role = "zk-framework-r"
        _range = Dict(
            begin=zk_task_port_range[0],
            end=zk_task_port_range[1]
        )
        range_list.append(_range)
        task_ports.ranges = Dict(
            range=range_list
        )
        task_ports.reservation = reservation
        resources.append(task_ports)

        return resources

    def zookeeper_task(self, agent_id, offered_cpu, offered_mem, offered_ports, zoo_id):
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

        zoo_servers = self.get_cluster_string(offered_ports, zoo_id)

        self.__add_environment_variable(environments, key="ZOO_SERVERS", value=zoo_servers.strip())
        self.__add_environment_variable(environments, key="ZOO_MY_ID", value=str(zoo_id))
        self.__add_environment_variable(environments, key="ZOO_PORT", value=str(offered_ports[0]))
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



        logging.debug("CPUS: {}, MEM: {}".format(self.zk_resources.cpu, self.zk_resources.mem))


        resources = list()
        labels = list()
        reservation = Dict()

        label_zoo_id = Dict()
        label_zoo_id.key = "zoo_id"
        label_zoo_id.value = str(zoo_id)
        labels.append(label_zoo_id)
        reservation.labels.labels = labels

        reservation.principal = self.principal

        cpus = Dict()
        cpus.name = 'cpus'
        cpus.type = 'SCALAR'
        cpus.scalar.value = self.zk_resources.cpu
        cpus.role = "zk-framework-r"
        cpus.reservation = reservation
        resources.append(cpus)

        mem = Dict()
        mem.name = 'mem'
        mem.type = 'SCALAR'
        mem.scalar.value = self.zk_resources.mem
        mem.role = "zk-framework-r"
        mem.reservation = reservation
        resources.append(mem)

        task_ports = Dict()
        range_list = list()
        task_ports.name = 'ports'
        task_ports.type = 'RANGES'
        task_ports.role = "zk-framework-r"
        _range = Dict(
            begin=offered_ports[0],
            end=offered_ports[1]
        )
        range_list.append(_range)
        task_ports.ranges = Dict(
            range=range_list
        )
        task_ports.reservation = reservation
        resources.append(task_ports)

        task.resources = resources
        return task

    def get_cluster_string(self, offered_ports, zoo_id):
        cluster_string = ""
        # for zoo_id in self.reserved_resources:
            # ports = [resource for resource in zoo_id.resources if resource.name is "ports"]
        lport = offered_ports[0] + 1
        eport = offered_ports[0] + 2
        cluster_string += "server." + str(zoo_id) + "=zookeeper-" + str(zoo_id) \
                          + "." + self.executor.name \
                          + ".mesos:" \
                          + str(lport) + ":" \
                          + str(eport)
        return cluster_string

    @staticmethod
    def reserve_zookeeper_resources(offer, resources, driver):
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
