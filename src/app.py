#!flask/bin/python
from flask import Flask, jsonify
from scheduler import scheduler_zookeeper as scheduler
from threading import Thread
import os

tasks = [
    {
        'id': 1,
        'title': u'Buy groceries',
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol',
        'done': False
    },
    {
        'id': 2,
        'title': u'Learn Python',
        'description': u'Need to find a good Python tutorial on the web',
        'done': False
    }
]


app = Flask(__name__)
thread = Thread(target=scheduler.main, args=("zk://10.141.141.10:2181/mesos",))
thread.start()

@app.route('/')
@app.route('/api/v1/status', methods=['GET'])
def status():
    return jsonify({'tasks': tasks})

if __name__ == '__main__':
    mesos_master = os.getenv("MESOS_MASTER")
    app.run(host='0.0.0.0', port=8000, debug=True)