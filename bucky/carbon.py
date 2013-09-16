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
import struct
import sys
import time
try:
    import cPickle as pickle
except ImportError:
    import pickle

import bucky.client as client
import bucky.names as names


log = logging.getLogger(__name__)


class DebugSocket(object):
    def sendall(self, data):
        sys.stdout.write(data)

class CarbonClient(client.Client):
    def __init__(self, cfg, pipe):
        super(CarbonClient, self).__init__(pipe)
        self.debug = cfg.debug
        self.ip = cfg.graphite_ip
        self.port = cfg.graphite_port
        self.max_reconnects = cfg.graphite_max_reconnects
        self.reconnect_delay = cfg.graphite_reconnect_delay
        if self.max_reconnects <= 0:
            self.max_reconnects = sys.maxint
        self.connect()

    def connect(self):
        if self.debug:
            log.debug("Connected the debug socket.")
            self.sock = DebugSocket()
            return
        for i in xrange(self.max_reconnects):
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self.sock.connect((self.ip, self.port))
                log.info("Connected to Carbon at %s:%s", self.ip, self.port)
                return
            except socket.error, e:
                if i+1 >= self.max_reconnects:
                    raise
                log.error("Failed to connect to %s:%s: %s", self.ip, self.port, e)
                if self.reconnect_delay > 0:
                    time.sleep(self.reconnect_delay)

    def reconnect(self):
        self.close()
        self.connect()

    def close(self):
        try:
            self.sock.close()
        except:
            pass

    def send(self, host, name, value, mtime):
        raise NotImplemented

class PlaintextClient(CarbonClient):
    def send(self, host, name, value, mtime):
        stat = names.statname(host, name)
        mesg = "%s %s %s\n" % (stat, value, mtime)
        for i in xrange(self.max_reconnects):
            try:
                self.sock.sendall(mesg)
                return
            except socket.error, err:
                if i+1 >= self.max_reconnects:
                    raise
                log.error("Failed to send data to Carbon server: %s", err)
                self.reconnect()

class PickleClient(CarbonClient):
    def __init__(self, cfg, pipe):
        super(PickleClient, self).__init__(cfg, pipe)
        self.buffer_size = cfg.graphite_pickle_buffer_size
        self.buffer = []

    def send(self, host, name, value, mtime):
        stat = names.statname(host, name)
        self.buffer.append((stat, (mtime, value)))
        if len(self.buffer) >= self.buffer_size:
            self.transmit()

    def transmit(self):
        payload = pickle.dumps(self.buffer, protocol=-1)
        header = struct.pack("!L", len(payload))
        self.buffer = []
        for i in xrange(self.max_reconnects):
            try:
                self.sock.sendall(header + payload)
                return
            except socket.error, err:
                if i+1 >= self.max_reconnects:
                    raise
                log.error("Failed to send data to Carbon server: %s", err)
                self.reconnect()
