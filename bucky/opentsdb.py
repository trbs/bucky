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
# Copyright 2012 Electronic Arts

import logging
import socket
import time as ptime
import sys
import threading
import Queue


log = logging.getLogger(__name__)


class DebugSocket(object):
    def sendall(self, data):
        sys.stdout.write(data)


class TsdbClient(threading.Thread):
    def __init__(self, queue, cfg):
        super(TsdbClient, self).__init__()
        self.setDaemon(True)
        self.ip = cfg.tsdb_ip
        self.queue = queue
        self.port = cfg.tsdb_port
        self.debug = cfg.debug
        self.transform = cfg.transform
        self.metrics = cfg.metrics
        self.connect()

    def run(self):
        while True:
            try:
                stat, value, time = self.queue.get(True, 1)
            except:
                """any queue error, start over"""
                continue

            stats = stat.split('.')
            key = '.'.join(stats[1:])

            if key in self.transform:
                mesg = "put %s %s %s host=%s metricsource=%s %s\n" % (self.transform[key]['name'],
                time, value, stats[0], self.ip, self.transform[key]['tags'])
            else:
                continue

            if self.metrics:
                print mesg

            continue

            try:
                if self.sock.sendall(mesg) is None:
                    continue
            except socket.error, err:
                log.error("Failed to send data to OpenTSDB server: %s" % err)
                if self.reconnect():
                    continue
                else:
                    """pause and reflect, then try again"""
                    ptime.sleep(3)
                    continue
                    

    def connect(self):
        if self.debug:
            log.debug("Connected the debug socket.")
            self.sock = DebugSocket()
            return

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.connect((self.ip, self.port))
            log.info("Connected to TSD Server at %s:%s" % (self.ip, self.port))
            return True
        except socket.error, e:
            args = (self.ip, self.port, e)
            log.error("Failed to connect to %s:%s: %s" % args)
            return False

    def reconnect(self):
        self.close()
        self.connect()

    def close(self):
        try:
            self.sock.close()
        except:
            pass
