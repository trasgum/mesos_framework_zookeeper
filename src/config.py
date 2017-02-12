import os

class Config(object):
    KAZOO_HOSTS = os.getenv('ZK_HOST', "172.17.0.3:2181")
    LOG_LEVEL = os.getenv('LOG_LEVEL', "DEBUG")
