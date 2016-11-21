import logging
import logging.config
import uuid
import time

import requests
import pika


class ProcessServiceClient(object):

    def __init__(self, rabbitmq_host=None, rabbitmq_port=5672, rabbitmq_vhost='/', rabbitmq_user=None, rabbitmq_password=None, logger=None):
        self.rabbitmq = rabbitmq_host
        self.remote = False
        self.session = None
        self.proxy = None
        self.channel = None
        self.biomaj_process = None
        self.logger = logging
        if logger:
            self.logger = logger
        if rabbitmq_host:
            self.remote = True
            connection = None
            if rabbitmq_user:
                credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
                connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host, rabbitmq_port, rabbitmq_vhost, credentials))
            else:
                connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host, rabbitmq_port, rabbitmq_vhost))
            self.channel = connection.channel()

    def create_session(self, bank, proxy=None):
        self.bank = bank
        if not self.remote:
            self.session = str(uuid.uuid4())
            return self.session
        r = requests.post(proxy + '/api/process/session/' + bank)
        if not r.status_code == 200:
            raise Exception('Failed to connect to the download proxy')
        result = r.json()
        self.session = result['session']
        self.proxy = proxy
        return result['session']

    def execute_process(self, biomaj_process):
        if self.remote:
            self.channel.basic_publish(
                exchange='',
                routing_key='biomajprocess',
                body=biomaj_process.SerializeToString(),
                properties=pika.BasicProperties(
                    # make message persistent
                    delivery_mode=2
                ))
        else:
            self.biomaj_process = biomaj_process

    def wait_for_process(self):
        over = False
        exitcode = -1
        info = None
        logging.info("Process:RemoteProcess:Waiting")
        while not over:
            r = requests.get(self.proxy + '/api/process/session/' + self.bank + '/' + self.session)
            if not r.status_code == 200:
                raise Exception('Failed to connect to the download proxy')
            result = r.json()
            # {'error': error, 'exitcode': exitcode, 'info': info}
            if result['exitcode'] > -1:
                exitcode = result['exitcode']
                over = True
                if result['exitcode'] > 0:
                    info = result['info']
                    self.logger.error('Process:RemoteProcess:Error:' + str(result['info']))
            else:
                time.sleep(10)
        return (exitcode, info)

    def clean(self):
        if self.remote:
            r = requests.delete(self.proxy + '/api/process/session/' + self.bank + '/' + self.session)
            if not r.status_code == 200:
                raise Exception('Failed to connect to the download proxy')
