import logging
import logging.config
import yaml
import redis
import uuid
import traceback
import threading

import pika
from flask import Flask
from flask import jsonify
import consul

from biomaj_process.message import message_pb2
from biomaj_process.process import Process
from biomaj_process.process import DockerProcess
from biomaj_core.utils import Utils

from biomaj_zipkin.zipkin import Zipkin


app = Flask(__name__)


@app.route('/api/process-message')
def ping():
    return jsonify({'msg': 'pong'})


def start_web(config):
    app.run(host='0.0.0.0', port=config['web']['port'])


def consul_declare(config):
    if config['consul']['host']:
        consul_agent = consul.Consul(host=config['consul']['host'])
        consul_agent.agent.service.register(
            'biomaj-process-message',
            service_id=config['consul']['id'],
            address=config['web']['hostname'],
            port=config['web']['port'],
            tags=['biomaj']
        )
        check = consul.Check.http(
            url='http://' + config['web']['hostname'] + ':' + str(config['web']['port']) + '/api/process-message',
            interval=20
        )
        consul_agent.agent.check.register(
            config['consul']['id'] + '_check',
            check=check,
            service_id=config['consul']['id']
        )
        return True
    else:
        return False


class ProcessService(object):

    channel = None
    redis_client = None

    def supervise(self):
        if consul_declare(self.config):
            web_thread = threading.Thread(target=start_web, args=(self.config,))
            web_thread.start()

    def __init__(self, config_file, rabbitmq=True):
        self.logger = logging
        self.session = None
        self.bank = None
        self.executed_callback = None
        with open(config_file, 'r') as ymlfile:
            self.config = yaml.load(ymlfile)
            Utils.service_config_override(self.config)

        Zipkin.set_config(self.config)

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
            connection = None
            rabbitmq_port = self.config['rabbitmq']['port']
            rabbitmq_user = self.config['rabbitmq']['user']
            rabbitmq_password = self.config['rabbitmq']['password']
            rabbitmq_vhost = self.config['rabbitmq']['virtual_host']
            if rabbitmq_user:
                credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
                connection = pika.BlockingConnection(pika.ConnectionParameters(self.config['rabbitmq']['host'], rabbitmq_port, rabbitmq_vhost, credentials))
            else:
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
        self.logger.debug('New process request %s session %s, execute %s' % (biomaj_file_info.bank, biomaj_file_info.session, biomaj_file_info.exe))
        session = self.redis_client.get(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session)
        if not session:
            self.logger.debug('Session %s for bank %s has expired, skipping execution of %s' % (biomaj_file_info.session, biomaj_file_info.bank, biomaj_file_info.exe))
            proc = {'bank': self.bank}
            proc['exitcode'] = 129
            proc['execution_time'] = 0
            return

        bank_env = {}
        for env_var in biomaj_file_info.env_vars:
            bank_env[env_var.name] = env_var.value

        args = ' '.join(biomaj_file_info.args)

        if biomaj_file_info.is_docker:
            process = DockerProcess(
                biomaj_file_info.name,
                biomaj_file_info.exe,
                args,
                desc=biomaj_file_info.description,
                proc_type=biomaj_file_info.proc_type,
                expand=biomaj_file_info.shell_expand,
                bank_env=bank_env,
                log_dir=biomaj_file_info.log_dir,
                docker_url=self.config['docker']['url'],
                docker=biomaj_file_info.docker.image,
                run_as_root=True,
                use_sudo=biomaj_file_info.docker.use_sudo
            )
        else:
            process = Process(
                biomaj_file_info.name,
                biomaj_file_info.exe,
                args,
                desc=biomaj_file_info.description,
                proc_type=biomaj_file_info.proc_type,
                expand=biomaj_file_info.shell_expand,
                bank_env=bank_env,
                log_dir=biomaj_file_info.log_dir
            )
        exitcode = -1
        proc = {'bank': self.bank}

        try:
            process.run()
            exitcode = process.exitcode
            proc['exitcode'] = exitcode
            proc['execution_time'] = process.exec_time
        except Exception as e:
            proc['exitcode'] = 129
            proc['execution_time'] = 0
            self.logger.error('Execution error:%s:%s:%s' % (biomaj_file_info.bank, biomaj_file_info.session, str(e)))
            session = self.redis_client.get(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session)
            if session:
                # If session deleted, do not track
                self.redis_client.set(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':error', 1)
                self.redis_client.set(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':error:info', str(e))
        self.logger.debug('Execution result: %d' % (exitcode))
        if exitcode > 0:
            proc['error'] = True

        session = self.redis_client.get(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session)
        if session:
            # If session deleted, do not track
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
                span = None
                if operation.trace and operation.trace.trace_id:
                    span = Zipkin('biomaj-process-executor', message.exe, trace_id=operation.trace.trace_id, parent_id=operation.trace.span_id)

                proc = self.execute(message)
                if span:
                    span.trace()
                if proc:
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
