import json
import shutil
import os
import tempfile
import logging
import copy
import stat
import time
from unittest.mock import patch

from biomaj_process.message import procmessage_pb2
from biomaj_process.process_service import ProcessService

import pytest


class TestBiomajProcess():

  def setup_method(self, m):
    self.curdir = os.path.dirname(os.path.realpath(__file__))
    self.pserv = ProcessService(os.path.join(self.curdir, 'config.yml'),rabbitmq=False)
    self.session = self.pserv._create_session('test')
    self.test_dir = tempfile.mkdtemp('biomaj')

  def teardown_method(self, m):
    self.pserv.clean()
    shutil.rmtree(self.test_dir)

  def test_execute(self):
    """
    Checks bank init
    """
    msg = procmessage_pb2.Process()
    msg.bank = 'test'
    msg.session = self.session
    msg.log_dir = self.test_dir
    msg.exe = os.path.join(self.curdir, 'test.sh')
    msg.args.append('arg1')
    msg.args.append('arg2')
    env_var = msg.env_vars.add()
    env_var.name = 'BIOMAJ_RELEASE'
    env_var.value = '1.2.3'
    msg.shell_expand = False
    print(msg)
    proc = self.pserv.execute(msg)
    assert (proc['exitcode'] == 0)
