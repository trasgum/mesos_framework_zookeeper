import logging
import json
import urllib2

logging.basicConfig(level=os.getenv("FW_LOG_LEVEL", logging.DEBUG),
                    format='[%(asctime)s %(levelname)s %(module)s:%(funcName)s] %(message)s'
                    )
log = logging.getLogger(__name__)


def get_mesos_master(zk_conn):

    mesos_master = dict()
    try:
        logging.debug("Reading mesos master {}".format(zk_conn))
        mesos_list = zk_conn.get_children('/mesos')
        logging.debug("Received /mesos tree: {}".format(mesos_list))
        mesos_master = json.loads(zk_conn.get('/mesos/' + mesos_list[0])[0])
    except Exception as err:
        # TODO[trasgum] handle zookeeper connection
        logging.exception("Error reading mesos master: {}".format(err))
        # raise SystemExit
    return mesos_master


def get_mesos_master_status(mesos_master_info):
    resp = urllib2.urlopen('http://' + mesos_master_info['hostname'] + ':' + str(mesos_master_info['port']) + '/state')
    return json.loads(resp.read())


def get_mesos_slaves_status(mesos_master_info):
    resp = urllib2.urlopen('http://' + mesos_master_info['hostname'] + ':' + str(mesos_master_info['port']) + '/slaves')
    return json.loads(resp.read())
