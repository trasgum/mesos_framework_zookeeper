import os
import sys
import json
import urllib2
import logging
from multiprocessing import Process
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from zookeeper_driver import ZookeeperDriver
from utils import get_mesos_master, get_mesos_master_status
from addict import Dict

logging.basicConfig(level=os.getenv("FW_LOG_LEVEL", logging.DEBUG),
                    format='[%(asctime)s %(levelname)s %(module)s:%(funcName)s] %(message)s'
                    )
log = logging.getLogger(__name__)


class RESTHandler(BaseHTTPRequestHandler):

    server_version = "zookeeper-framework/0.1.0"
    sys_version = ""
    mesos_url = os.getenv('ZK_MESOS_URL')
    zk_framework_name = os.getenv('ZK_FW_NAME', 'zk-framework')
    try:
        logging.debug("Connecting to zookeeper")
        zk_con_rest = ZookeeperDriver.initialize_zk_con(mesos_url)
    except Exception as err:
        logging.exception("Error connecting to zookeeper: {}".format(err))

    def do_GET(self):
        try:
            log.info("Received request: {} - {} {}".format(self.client_address, self.command, self.path))
            if self.path not in ["/", "/status", "/state"]:
                response = {"error": "Not Found"}
                self.send_response(404, 'Not Found')
            else:
                mesos_master_info = get_mesos_master(self.zk_con_rest)
                response = self.check_scheduler_status(mesos_master_info, self.zk_framework_name)
                self.send_response(200, 'OK')

            message = json.dumps(response, ensure_ascii=True, encoding='UTF-8')
            logging.debug("Mesos status: {}".format(message))

            self.protocol_version = 'HTTP/1.1'
            self.send_header('Content-type', 'application/json')
            self.send_header('Connection', 'close')
            self.end_headers()
            self.wfile.write(message + '\n')
        except Exception as err:
            logging.exception("Error reading framework status: {}".format(err))
        return

    @staticmethod
    def check_scheduler_status(mesos_master_dict, zk_framework_name):
        tasks_dict = dict()
        zk_framework = Dict()

        try:
            logging.debug("Reading mesos status")
            mesos_state = get_mesos_master_status(mesos_master_dict)

            for framework in mesos_state['frameworks']:
                if framework['name'] == zk_framework_name :
                    zk_framework = Dict(framework)

            if len(zk_framework.tasks) > 0:
                tasks_dict[zk_framework_name] = {"state": "Running",
                                                 "id": zk_framework.id
                                                 }
                for task in zk_framework.tasks:
                    tasks_dict[zk_framework_name][task['id']] = {"state": task['state'], "name": task['name']}
            else:
                tasks_dict['zk-framework'] = {"state": "Not running"}

            return tasks_dict
        except Exception as err:
            logging.exception("Error reading mesos master: {}".format(err))


class RESTServer(HTTPServer):
    def __init__(self, host, port):
        server_address = (host, port)
        log.debug("Server address: {}".format(server_address))
        HTTPServer.__init__(self, server_address=server_address, RequestHandlerClass=RESTHandler)

    def run_rest_process(self):
        log.debug("Starting zookeeper rest")
        rest_status_process = Process(name='rest', target=self.serve_forever())
        # rest_status_process.daemon = True
        # rest_status_process.setDaemon(True)
        rest_status_process.daemon = True
        rest_status_process.start()
        log.info("Started zookeeper rest: {}".format(rest_status_process.is_alive()))
        return rest_status_process

    @staticmethod
    def run_server(host, port):
        try:
            server = RESTServer(host, port)
            log.info("Starting status server in: {}:{}".format(host, port))
            server.serve_forever()
            return server
        except Exception as err:
            log.exception("Error: {}".format(err))
