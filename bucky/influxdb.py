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
        except:
            pass

    def kv(self, k, v):
        return str(k) + '=' + str(v)

    def flush(self):
        now = time.time()
        if len(self.buffer) > 30 or (now - self.flush_timestamp) > 3:
            payload = '\n'.join(self.buffer).encode()
            self.resolve_hosts()
            for ip, port in self.resolved_hosts:
                self.sock.sendto(payload, (ip, port))
            self.buffer = []
            self.flush_timestamp = now

    def send(self, host, name, value, mtime, metadata=None):
        buf = [name]
        if host:
            if metadata is None:
                metadata = {'host': host}
            else:
                if 'host' not in metadata:
                    metadata['host'] = host
        if metadata:
            for k in metadata.keys():
                v = metadata[k]
                # InfluxDB will drop insert with tags without values
                if v is not None:
                    buf.append(self.kv(k, v))
        # https://docs.influxdata.com/influxdb/v1.2/write_protocols/line_protocol_tutorial/
        line = ' '.join((','.join(buf), self.kv('value', value), str(long(mtime) * 1000000000)))
        self.buffer.append(line)
        self.flush()
