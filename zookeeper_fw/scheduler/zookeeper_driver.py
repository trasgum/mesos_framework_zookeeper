import getpass
import logging
from os import getenv
import os.path as path

from addict import Dict
from kazoo.client import KazooClient
from pymesos import MesosSchedulerDriver
from zookeeper_scheduler import ZookeeperScheduler

logging.basicConfig(level=getenv("FW_LOG_LEVEL", logging.DEBUG),
                    format='[%(asctime)s %(levelname)s %(module)s:%(funcName)s] %(message)s'
                    )
log = logging.getLogger(__name__)


# class ZookeeperDriver(MesosSchedulerDriver):
class ZookeeperDriver(object):
    def __init__(self, mesos_url):
        self.mesos_url = mesos_url
        self.framework_name = getenv('ZK_FW_NAME', 'zk-framework')
        self.framework_role = getenv('ZK_FW_ROLE', self.framework_name + '-r')
        self.executor_name = self.framework_name
        self.framework_url = getenv('ZK_FW_URL', 'http://' + self.framework_name + '.mesos:8000/')
        self.num_instances = getenv('ZK_FW_NUM_INSTANCES', 3)
        self.principal = getenv('ZK_FW_PRINCIPAL')
        self.secret = getenv('ZK_FW_SECRET')
        self.zk_conn = None
        self.driver_thread = None
        self.zk_resources = Dict()
        self.zk_resources.cpu = float(getenv("ZK_FW_CPU", 0.1))
        self.zk_resources.mem = float(getenv('ZK_FW_MEM', 128))
        self.zk_resources.disk_data = float(getenv('ZK_FW_DISK_DATA', 100))
        self.zk_resources.disk_log = float(getenv('ZK_FW_DISK_LOG', 100))
        self.driver = None

        if self.num_instances not in [1, 3, 5]:
            raise Exception("The number of istances must be 1, 3 or 5")

    def get_driver(self):
        self.zk_conn = self.initialize_zk_con(self.mesos_url)
        logging.debug("Connection stablished with zookeeper: {}".format(self.zk_conn.state))
        self.zk_conn.ensure_path(path.join('/', self.framework_name, 'instances'))

        executor = Dict()
        executor.executor_id.value = self.executor_name
        executor.name = executor.executor_id.value

        framework = Dict()
        framework.user = getpass.getuser()  # the current user
        framework.role = self.framework_role
        framework.name = self.framework_name
        framework.hostname = self.framework_name
        framework.webui_url = self.framework_url
        framework.checkpoint = True

        # TODO[trasgum] pymesos added auth to next version
        #  https://github.com/douban/pymesos/commit/48a22dd825f6416c01690c638618519dbf4d8aa0
        if '/mesos' not in self.mesos_url:
            self.mesos_url += '/mesos'

        if self.principal and self.secret:
            framework.principal = self.principal
            self.driver = MesosSchedulerDriver(
                ZookeeperScheduler(executor,
                                   principal=self.principal,
                                   role=self.framework_role,
                                   instances=self.num_instances,
                                   zk_resources=self.zk_resources,
                                   zk_driver=self.zk_conn
                                   ),
                framework,
                self.mesos_url,
                use_addict=True,
                implicit_acknowledgements=True
                # principal=principal, secret=secret
            )
        else:
            framework.principal = self.framework_name
            self.driver = MesosSchedulerDriver(
                ZookeeperScheduler(executor,
                                   principal=self.framework_name,
                                   role=self.framework_role,
                                   instances=self.num_instances,
                                   zk_resources=self.zk_resources,
                                   zk_driver=self.zk_conn
                                   ),
                framework,
                self.mesos_url,
                use_addict=True,
                implicit_acknowledgements=True
            )

        return self.driver

    def stop(self):
        self.driver.stop()

    @staticmethod
    def initialize_zk_con(url):
        zk_host = url.split('/')[2]
        if 'zk://' not in url:
            zk_host = url.split(':')[0] + ':2181'

        log.info("Starting connection to zookeeper {}".format(zk_host))
        zk_conn = KazooClient(zk_host)
        zk_conn.start()
        return zk_conn
