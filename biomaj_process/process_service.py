import logging
import logging.config
import yaml
import redis
import uuid
import traceback

import pika

from biomaj_process.message import message_pb2
from biomaj_process.process import Process


class ProcessService(object):

    channel = None
    redis_client = None

    def __init__(self, config_file, rabbitmq=True):
        self.logger = logging
        self.session = None
        self.bank = None
        self.executed_callback = None
        with open(config_file, 'r') as ymlfile:
            self.config = yaml.load(ymlfile)

        if 'log_config' in self.config:
            for handler in list(self.config['log_config']['handlers'].keys()):
                self.config['log_config']['handlers'][handler] = dict(self.config['log_config']['handlers'][handler])
            logging.config.dictConfig(self.config['log_config'])
            self.logger = logging.getLogger('biomaj')

        if not self.redis_client:
            self.logger.debug('Init redis connection')
            self.redis_client = redis.StrictRedis(host=self.config['redis']['host'],
                                                  port=self.config['redis']['port'],
                                                  db=self.config['redis']['db'],
                                                  decode_responses=True)

        if rabbitmq and not self.channel:
            connection = pika.BlockingConnection(pika.ConnectionParameters(self.config['rabbitmq']['host']))
            self.channel = connection.channel()
            self.logger.info('Process service started')

    def close(self):
        if self.channel:
            self.channel.close()

    def on_executed_callback(self, func):
        self.executed_callback = func

    def clean(self, biomaj_file_info=None):
        '''
        Clean session and download info
        '''
        session = self.session
        bank = self.bank
        if biomaj_file_info:
            session = biomaj_file_info.session
            bank = biomaj_file_info.bank
        self.logger.debug('Clean %s session %s' % (bank, session))
        self.redis_client.delete(self.config['redis']['prefix'] + ':' + bank + ':session:' + session)
        self.redis_client.delete(self.config['redis']['prefix'] + ':' + bank + ':session:' + session + ':error')
        self.redis_client.delete(self.config['redis']['prefix'] + ':' + bank + ':session:' + session + ':exitcode')
        self.redis_client.delete(self.config['redis']['prefix'] + ':' + bank + ':session:' + session + ':error:info')

    def _create_session(self, bank):
        '''
        Creates a unique session
        '''
        self.session = str(uuid.uuid4())
        self.redis_client.set(self.config['redis']['prefix'] + ':' + bank + ':session:' + self.session, 1)
        self.logger.debug('Create %s new session %s' % (bank, self.session))
        self.bank = bank
        return self.session

    def execute(self, biomaj_file_info):
        '''
        List remote content
        '''
        self.logger.debug('New process request %s session %s' % (biomaj_file_info.bank, biomaj_file_info.session))
        session = self.redis_client.get(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session)
        if not session:
            self.logger.debug('Session %s for bank %s has expired, skipping execution of %s' % (biomaj_file_info.session, biomaj_file_info.bank, biomaj_file_info.exe))
            return

        bank_env = {}
        for env_var in biomaj_file_info.env_vars:
            bank_env[env_var.name] = env_var.value

        args = ' '.join(biomaj_file_info.args)

        process = Process(
            biomaj_file_info.name,
            biomaj_file_info.exe,
            args,
            biomaj_file_info.description,
            biomaj_file_info.proc_type,
            biomaj_file_info.shell_expand,
            bank_env,
            biomaj_file_info.log_dir
        )
        exitcode = -1
        proc = {'bank': self.bank}
        try:
            process.run()
            exitcode = process.exitcode
            proc['exitcode'] = exitcode
            proc['execution_time'] = process.exec_time
        except Exception as e:
            self.logger.error('Execution error:%s:%s:%s' % (biomaj_file_info.bank, biomaj_file_info.session, str(e)))
            self.redis_client.set(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':error', 1)
            self.redis_client.set(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':error:info', str(e))
        if exitcode > 0:
            proc['error'] = True

        self.redis_client.set(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':exitcode', exitcode)
        return proc

    def ask_execute(self, biomaj_info_file):
        self.channel.basic_publish(
            exchange='',
            routing_key='biomajprocess',
            body=biomaj_info_file.SerializeToString(),
            properties=pika.BasicProperties(
                # make message persistent
                delivery_mode=2
            ))

    def callback_messages(self, ch, method, properties, body):
        '''
        Manage download and send ACK message
        '''
        try:
            operation = message_pb2.Operation()
            operation.ParseFromString(body)
            # self.logger.debug('Received message: %s' % (operation))
            if operation.type == 1:
                message = operation.process
                self.logger.debug('Execute operation %s, %s' % (message.bank, message.session))
                proc = self.execute(message)
                self.executed_callback(message.bank, [proc])
            else:
                self.logger.warn('Wrong message type, skipping')
        except Exception as e:
            self.logger.error('Error with message: %s' % (str(e)))
            traceback.print_exc()
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def wait_for_messages(self):
        '''
        Loop queue waiting for messages
        '''
        self.channel.queue_declare(queue='biomajprocess', durable=True)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            self.callback_messages,
            queue='biomajprocess')
        self.channel.start_consuming()
