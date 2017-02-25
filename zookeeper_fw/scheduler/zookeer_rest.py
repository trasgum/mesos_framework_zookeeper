import os
import sys
import json
import urllib2
import logging
from multiprocessing import Process
from threading import Thread
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from zookeeper_driver import ZookeeperDriver

logging.basicConfig(level=os.getenv("FW_LOG_LEVEL", logging.DEBUG),
                    format='[%(asctime)s %(levelname)s %(module)s:%(funcName)s] %(message)s'
                    )
log = logging.getLogger(__name__)


class RESTHandler(BaseHTTPRequestHandler):

    server_version = "zookeeper-framework/0.1.0"
    sys_version = ""
    mesos_url = os.getenv('ZK_MESOS_URL')
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
                response = self.check_scheduler_status(self.zk_con_rest)
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
    def check_scheduler_status(zk_conn):
        tasks_dict = dict()
        mesos_master = dict()
        try:
            logging.debug("Reading mesos master {}".format(zk_conn))
            mesos_list = zk_conn.get_children('/mesos')
            logging.debug("Received /mesos tree: {}".format(mesos_list))
            mesos_master = json.loads(zk_conn.get('/mesos/' + mesos_list[0])[0])
        except Exception as err:
            # TODO[trasgum] handle zookeeper connection
            logging.exception("Error reading mesos master: {}".format(err))
            raise SystemExit

        try:
            logging.debug("Reading mesos status")
            resp = urllib2.urlopen('http://' + mesos_master['hostname'] + ':' + str(mesos_master['port']) + '/state')
            mesos_state = json.loads(resp.read())
            running_tasks = [framework['tasks'] for framework in mesos_state['frameworks'] if
                             framework['name'] == 'zk-framework']
            if len(running_tasks) > 0:
                tasks_dict['zk-framework'] = {"state": "Running"}
                for task in running_tasks[0]:
                    tasks_dict['zk-framework'][task['id']] = {"state": task['state'], "name": task['name']}
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
