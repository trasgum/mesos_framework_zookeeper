import getpass
import logging
import os

from addict import Dict
from kazoo.client import KazooClient
from pymesos import MesosSchedulerDriver

from zookeeper_scheduler import ZookeeperScheduler

logging.basicConfig(level=os.getenv("FW_LOG_LEVEL", logging.DEBUG),
                    format='[%(asctime)s %(levelname)s %(module)s:%(funcName)s] %(message)s'
                    )
log = logging.getLogger(__name__)


class ZookeeperDriver:
    def __init__(self, mesos_url):
        self.mesos_url = mesos_url
        self.framework_name = os.getenv('ZK_FW_NAME', 'zk-framework')
        self.framework_role = os.getenv('ZK_FW_ROLE', self.framework_name)
        self.executor_name = self.framework_name
        self.framework_url = os.getenv('ZK_FW_URL', 'http://' + self.framework_name + '.mesos:8000/')
        self.num_instances = os.getenv('ZK_FW_NUM_INSTANCES', 1)
        self.principal = os.getenv('ZK_FW_PRINCIPAL')
        self.secret = os.getenv('ZK_FW_SECRET')
        self.zk_conn = None
        self.driver_thread = None

    def get_driver(self):
        zk_conn = self.initialize_zk_con(self.mesos_url)
        zk_conn.ensure_path(os.path.join('/', self.framework_name, 'instances'))
        logging.debug("Connection stablished with zookeeper: {}".format(zk_conn.state))

        executor = Dict()
        executor.executor_id.value = self.executor_name
        executor.name = executor.executor_id.value
        # executor.command.value = os.path.abspath('./executor-minimal.py')

        framework = Dict()
        framework.user = getpass.getuser()  # the current user
        framework.role = self.framework_role
        framework.name = self.framework_name
        framework.hostname = self.framework_name
        framework.webui_url = self.framework_url
        framework.checkpoint = True

        logging.debug("TRUCO driver zk_conn: {}".format(zk_conn))
        # TODO[trasgum] pymesos added auth to next version
        #  https://github.com/douban/pymesos/commit/48a22dd825f6416c01690c638618519dbf4d8aa0
        if '/mesos' not in self.mesos_url:
            self.mesos_url += '/mesos'

        if self.principal and self.secret:
            framework.principal = self.principal
            self.driver = MesosSchedulerDriver(
                ZookeeperScheduler(executor, instances=self.num_instances, zk_driver=self.zk_conn),
                framework,
                self.mesos_url,
                use_addict=True,
                implicit_acknowledgements=True
                # principal=principal, secret=secret
            )
        else:
            framework.principal = self.framework_name
            self.driver = MesosSchedulerDriver(
                ZookeeperScheduler(executor, instances=self.num_instances, zk_driver=self.zk_conn),
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

