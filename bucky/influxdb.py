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

import six
import time
import socket
import logging

import bucky.client as client


if six.PY3:
    xrange = range
    long = int


log = logging.getLogger(__name__)


class InfluxDBClient(client.Client):
    def __init__(self, cfg, pipe):
        super(InfluxDBClient, self).__init__(pipe)
        self.hosts = cfg.influxdb_hosts
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.flush_timestamp = time.time()
        self.resolved_hosts = None
        self.resolve_timestamp = 0
        self.buffer = []

    def parse_address(self, address, default_port=8089):
        bits = address.split(":")
        if len(bits) == 1:
            host, port = address, default_port
        elif len(bits) == 2:
            host, port = bits[0], int(bits[1])
        else:
            raise ValueError("Address %s is invalid" % (address,))
        hostname, aliaslist, ipaddrlist = socket.gethostbyname_ex(host)
        for ip in ipaddrlist:
            yield ip, port

    def resolve_hosts(self):
        now = time.time()
        if self.resolved_hosts is None or (now - self.resolve_timestamp) > 180:
            resolved_hosts = []
            for host in self.hosts:
                for ip, port in self.parse_address(host):
                    log.info("Resolved InfluxDB endpoint: %s:%d", ip, port)
                    resolved_hosts.append((ip, port))
            self.resolved_hosts = resolved_hosts
            self.resolve_timestamp = now

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass

    def tick(self):
        now = time.time()
        if len(self.buffer) > 10 or ((now - self.flush_timestamp) > 1 and len(self.buffer)):
            payload = '\n'.join(self.buffer).encode()
            self.resolve_hosts()
            for ip, port in self.resolved_hosts:
                self.sock.sendto(payload, (ip, port))
            self.buffer = []
            self.flush_timestamp = now

    def _send(self, host, name, mtime, values, metadata=None):
        # https://docs.influxdata.com/influxdb/v1.2/write_protocols/line_protocol_tutorial/
        label_buf = [name]
        if not metadata and host:
            metadata = ('host', host)
        if metadata:
            # InfluxDB docs recommend sorting tags
            for k, v in metadata:
                # InfluxDB will drop insert with empty tags
                if v is None or v == '':
                    continue
                v = str(v).replace(' ', '')
                label_buf.append(str(k) + '=' + v)
        value_buf = []
        for k in values.keys():
            v = values[k]
            t = type(v)
            if t is long or t is int:
                value_buf.append(str(k) + '=' + str(v) + 'i')
            elif t is float or t is bool:
                value_buf.append(str(k) + '=' + str(v))
            elif t is str:
                value_buf.append(str(k) + '="' + v + '"')
        # So, the lower timestamp precisions don't seem to work with line protocol...
        line = ' '.join((','.join(label_buf), ','.join(value_buf), str(long(mtime) * 1000000000)))
        self.buffer.append(line)
        self.tick()

    def send(self, host, name, value, mtime, metadata=None):
        self._send(host, name, mtime, {'value': value}, metadata)

    def send_bulk(self, host, name, value, mtime, metadata=None):
        self._send(host, name.strip('.'), mtime, value, metadata)
