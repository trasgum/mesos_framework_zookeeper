#!flask/bin/python
import json
import urllib2
from flask import Flask, jsonify
from flask_zookeeper import FlaskZookeeperClient
from config import Config


def check_scheduler_status():
    tasks = dict()
    mesos_master = dict()
    if flask_zk_cli.connection is not None:
        mesos_list = flask_zk_cli.connection.get_children('/mesos')
        mesos_master = json.loads(flask_zk_cli.connection.get('/mesos/' + mesos_list[0])[0])

    resp = urllib2.urlopen('http://' + mesos_master['hostname'] + ':' + str(mesos_master['port']) + '/state')
    mesos_state = json.loads(resp.read())
    running_tasks = [framework['tasks'] for framework in mesos_state['frameworks'] if framework['name'] == 'zk-framework']
    if len(running_tasks) > 0:
        tasks['zk-framework'] = {"state": "Running"}
        for task in running_tasks[0]:
            tasks['zk-framework'][task['id']] = {"state": task['state'], "name": task['name']}
    else:
        tasks['zk-framework'] = {"state": "Not running"}

    return tasks


app = Flask(__name__)
app.config.from_object(Config)
flask_zk_cli = FlaskZookeeperClient(app)


@app.route('/')
@app.route('/api/v1/status', methods=['GET'])
def status():
    return jsonify(check_scheduler_status())


# @app.route('/api/v1/start', methods=['POST'])
# def start():
#     if check_scheduler_status()['zk-framework']['state'] == 'Not running':
#         start_scheduler('zk://' + app.config['KAZOO_HOSTS'] + '/mesos')
#         return jsonify({'status': 202})
#     else:
#         return jsonify({'status': 304})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
