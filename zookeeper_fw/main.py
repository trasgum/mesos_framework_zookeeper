import os
import logging
import signal
import time
from threading import Thread


def main():
    from scheduler.zookeeper_driver import ZookeeperDriver
    from scheduler.zookeer_rest import RESTServer

    def signal_handler(signal, frame):
        driver.stop()

    def run_driver_thread():
        try:
            status = 0 if driver.run() == "DRIVER_STOPPED" else 1
        except Exception as err:
            logging.exception("Error running driver: {}".format(err))
        driver.stop()
        sys.exit(status)

    def run_rest_thread(server_class=RESTServer):
        rest = server_class('', os.getenv('ZK_FW_STATUS_PORT', 8000))
        # rest.serve_forever()
        while driver_thread.is_alive():
            rest.handle_request()

    logging.basicConfig(level=os.getenv("FW_LOG_LEVEL", logging.DEBUG),
                        format='[%(asctime)s %(levelname)s %(module)s:%(funcName)s] %(message)s'
                        )
    log = logging.getLogger(__name__)

    logging.info("Starting zookeeper framework ...")
    driver = ZookeeperDriver(os.getenv('ZK_MESOS_URL')).get_driver()
    log.info("Starting zookeeper driver")
    driver_thread = Thread(name='driver', target=run_driver_thread, args=())
    driver_thread.start()
    log.info("Started zookeeper driver: {}".format(driver_thread.is_alive()))

    while not driver.connected:
        logging.info("Driver waiting to connect...")
        time.sleep(5)

    logging.info("Starting zookeeper rest ...")
    rest_thread = Thread(name='rest', target=run_rest_thread, args=())
    rest_thread.start()
    log.info("Started zookeeper rest: {}".format(rest_thread.is_alive()))

    log.info('Scheduler running...')
    log.info('Scheduler running, Ctrl-C to exit')
    signal.signal(signal.SIGINT, signal_handler)

    while driver_thread.is_alive() or rest_thread.is_alive():
        time.sleep(1)

    logging.info('Framework finished.')
    sys.exit(0)


if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

    try:
        assert os.getenv('ZK_MESOS_URL')
        main()
    except AssertionError as err:
        print("Usage: " + path.basename(__file__) + " ZK_MESOS_URL env var not found" +
                '''
                Ej: export ZK_MESOS_URL=zk://127.0.0.1:2181
                export ZK_MESOS_URL=leader.mesos:5050
                ''')
        sys.exit(1)
    except Exception as err:
        print("Something went wrong: {}".format(err))

