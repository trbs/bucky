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
import time

import bucky.cfg as cfg


log = logging.getLogger(__name__)


class DebugSocket(object):
    def sendall(self, data):
        sys.stdout.write(data)


class CarbonClient(object):
    def __init__(self):
        if cfg.graphite_max_reconnects < 0:
            cfg.graphite_max_reconnects = sys.maxint
        self.connect()

    def connect(self):
        if cfg.debug:
            log.debug("Connected the debug socket.")
            self.sock = DebugSocket()
            return
        for i in xrange(cfg.graphite_max_reconnects):
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self.sock.connect((cfg.graphite_ip, cfg.graphite_port))
                log.info("Connected to Carbon at %s:%s" % peer)
                return
            except socket.error, e:
                if i+1 >= cfg.graphite_max_reconnects:
                    raise
                args = (cfg.graphite_ip, cfg.graphite_port, e)
                log.error("Failed to connect to %s:%s: %s" % args)
                if cfg.graphite_reconnect_delay > 0:
                    time.sleep(cfg.graphite_reconnect_delay)

    def reconnect(self):
        self.close()
        self.connect()

    def close(self):
        try:
            self.sock.close()
        except:
            pass

    def send(self, stat, value, mtime):
        mesg = "%s %s %s\n" % (stat, value, mtime)
        for i in xrange(cfg.graphite_max_reconnects):
            try:
                self.sock.sendall(mesg)
                return
            except socket.error, err:
                if i+1 >= cfg.graphite_max_reconnects:
                    raise
                log.error("Failed to send data to Carbon server: %s" % err)
                self.reconnect()
