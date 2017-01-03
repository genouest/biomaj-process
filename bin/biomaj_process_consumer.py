import os
import logging

import requests
import yaml
import consul

from biomaj_process.process_service import ProcessService
from biomaj_core.utils import Utils

config_file = 'config.yml'
if 'BIOMAJ_CONFIG' in os.environ:
        config_file = os.environ['BIOMAJ_CONFIG']

config = None
with open(config_file, 'r') as ymlfile:
    config = yaml.load(ymlfile)
    Utils.service_config_override(config)


def on_executed(bank, procs):
    metrics = []
    if not procs:
        metric = {'bank': bank, 'error': 1}
        metrics.append(metrics)
    else:
        for proc in procs:
            metric = {'bank': bank}
            if 'error' in proc and proc['error']:
                metric['error'] = 1
            else:
                metric['execution_time'] = proc['execution_time']
            metrics.append(metric)
        r = requests.post(config['web']['local_endpoint'] + '/api/process/metrics', json = metrics)


process = ProcessService(config_file)
process.on_executed_callback(on_executed)
process.supervise()
process.wait_for_messages()
