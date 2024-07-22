#!/usr/bin/env python3

import os
import logging

import requests
import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader
import consul

from biomaj_process.process_service import ProcessService
from biomaj_core.utils import Utils

config_file = 'config.yml'
if 'BIOMAJ_CONFIG' in os.environ:
        config_file = os.environ['BIOMAJ_CONFIG']

config = None
with open(config_file, 'r') as ymlfile:
    config = yaml.load(ymlfile, Loader=Loader)
    Utils.service_config_override(config)


def on_executed(bank, procs):
    if 'prometheus' in config and not config['prometheus']:
        return
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
            if 'hostname' in config['web']:
                metric['host'] = config['web']['hostname']
            metrics.append(metric)
        proxy = Utils.get_service_endpoint(config, 'process')
        r = requests.post(proxy + '/api/process/metrics', json = metrics)


process = ProcessService(config_file)
process.on_executed_callback(on_executed)
process.supervise()
process.wait_for_messages()
