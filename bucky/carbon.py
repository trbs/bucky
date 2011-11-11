# -*- coding: utf-8 -
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
# Copyright 2011 Cloudant, Inc.

import logging
import socket
import sys


log = logging.getLogger(__name__)


class CarbonException(Exception):
    def __init__(self, mesg):
        self.mesg = mesg
    def __str__(self):
        return self.mesg


class ConnectError(CarbonException):
    pass


class CarbonClient(object):
    def __init__(self, cfg):
        ip = cfg["graphite_ip"]
        port = cfg["graphite_port"]
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.connect((ip, port))
            log.info("Connect to Carbon at %s:%s" % (ip, port))
        except Exception:
            log.error("Failed to connect to %s:%s" % (ip, port))
            sys.exit(2)

    def send(self, stat, value, mtime):
        mesg = "%s %s %s\n" % (stat, value, mtime)
        self.sock.sendall(mesg)

