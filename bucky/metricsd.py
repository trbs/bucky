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

import logging
import Queue
import re
import struct
import multiprocessing
import time

from bucky.errors import ConfigError, ProtocolError
from bucky.metrics.counter import Counter
from bucky.metrics.gauge import Gauge
from bucky.metrics.histogram import Histogram
from bucky.metrics.meter import Meter
from bucky.metrics.timer import Timer
from bucky.names import statname
from bucky.udpserver import UDPServer


log = logging.getLogger(__name__)


class MetricsDCommand(object):
    UPDATE = object()
    CLEAR = object()
    DELETE = object()

    def __init__(self, name, mtype, action, value=None):
        self.name = name
        self.mtype = mtype
        self.action = action
        self.value = value
        if self.action is not self.UPDATE and self.value is not None:
            raise ValueError("Values are only valid for updates")


class MetricsDParser(object):
    NUMERIC_TYPES = {
        0x00: "!B", 0x01: "!b",
        0x10: "!H", 0x11: "!h",
        0x20: "!I", 0x21: "!i",
        0x30: "!Q", 0x31: "!q",
        0x40: "!f", 0x41: "!d"
    }

    METRIC_TYPES = {
        0x00: Counter,
        0x10: Gauge,
        0x20: Histogram,
        0x30: Meter,
        0x40: Timer
    }

    METRIC_ACTION = {
        0x00: MetricsDCommand.UPDATE,
        0x01: MetricsDCommand.CLEAR,
        0x02: MetricsDCommand.DELETE
    }

    def __init__(self):
        pass

    def parse(self, data):
        if data[0] != 0xAA:
            raise ProtocolError("Invalid magic byte")
        hostname, data = self.parse_string(data)
        while len(data):
            mc, data = self.parse_metric(hostname, data)
            yield mc

    def parse_metric(self, hostname, data):
        cmd, data = data[0], data[1:]
        mtype = cmd & 0xF0
        action = cmd & 0x0F
        if mtype not in self.METRIC_TYPES:
            raise ProtocolError("Invalid metric type")
        if action not in self.METRIC_ACTIONS:
            raise ProtocolError("Invalid metric action")
        name, data = self.parse_string(data)
        if action is MetricsDCommand.UPDATE:
            value, data = self.parse_number(data)
        else:
            value = None
        stat = statname(hostname, name.split("."))
        cmd = MetricsDCommand(stat, mtype, action, value)
        return cmd, data

    def parse_string(self, data):
        (length,) = struct.unpack("!H", data[:2])
        if length > len(data) - 2:
            raise ProtocolError("Truncated string value")
        if data[2+length] != 0x00:
            raise ProtocolError("String missing null-byte terminator")
        try:
            ret = data[2:2+length-1].decode("utf-8")
            return ret, data[2+length+1:]
        except UnicodeDecodeError:
            raise ProtocolError("String is not value UTF-8")

    def parse_number(self, data):
        fmt = self.NUMERIC_TYPES.get(data[0])
        if fmt is None:
            raise ProtocolError("Invalid numeric type")
        sz = struct.calcsize(fmt)
        if sz > len(data) - 1:
            raise ProtocolError("Truncated numeric value")
        (val,) = struct.unpack(data[1:1+sz])
        return val, data[1+sz:]


class MetricsDHandler(multiprocessing.Process):
    def __init__(self, outbox, interval):
        super(MetricsDHandler, self).__init__()
        self.daemon = True
        self.interval = interval
        self.outbox = outbox
        self.inbox = multiprocessing.Queue()
        self.next_update = time.time() + self.interval
        self.metrics = {}

    def enqueue(self, mc):
        self.inbox.put(mc)

    def update_metric(self, mc):
        if mc.action is MetricsDCommand.DELETE:
            metrics.pop(mc.name, None)
            return
        metric = self.metrics.get(mc.name)
        if mc.action is MetricsDCommand.CLEAR:
            if metric is not None:
                metric.clear()
            return
        assert mc.action is MetricsDCommand.UPDATE
        if metric is None:
            metric = mc.mtype(mc.name)
        metric.update(mc.value)

    def run(self):
        while True:
            to_sleep = self.next_update - time.time()
            if to_sleep <= 0:
                self.flush_updates()
            self.next_update = time.time() + self.interval
            to_sleep = self.interval
            try:
                mv = self.inbox.get(True, to_sleep)
                self.update_metric(mv)
            except Queue.Empty:
                continue

    def flush_updates(self):
        for _, metric in self.metrics.iteritems():
            for v in metric.metrics():
                self.outbox.put((v.name, v.value, v.time))


class MetricsDServer(UDPServer):
    def __init__(self, queue, cfg):
        super(MetricsDServer, self).__init__(cfg.metricsd_ip, cfg.metricsd_port)
        self.parser = MetricsDParser()
        self.handlers = self._init_handlers(queue, cfg)

    def handle(self, data, addr):
        try:
            for mc in self.parser.parse(data):
                handler = self._get_handler(mc.name)
                handler.enqueue(mc)
        except ProtocolError:
            log.exception("Error from: %s:%s" % addr)

    def _init_handlers(self, queue, cfg):
        ret = []
        default = cfg.metricsd_default_interval
        handlers = cfg.metricsd_handlers
        if not len(handlers):
            ret = [(None, MetricsDHandler(queue, default))]
            ret[0][1].start()
            return ret
        for item in handlers:
            if len(item) == 2:
                pattern, interval, priority = item[0], item[1], 100
            elif len(item) == 3:
                pattern, interval, priority = item
            else:
                raise ConfigError("Invalid handler specification: %s" % item)
            try:
                pattern = re.compile(pattern)
            except:
                raise ConfigError("Invalid pattern: %s" % pattern)
            if interval < 0:
                raise ConfigError("Invalid interval: %s" % interval)
            ret.append((pattern, interval, priority))
        handlers.sort(key=lambda p: p[2])
        ret = [(p, MetricsDHandler(queue, i)) for (p, i, _) in ret]
        ret.append((None, MetricsDHandler(queue, default)))
        for _, h in ret:
            h.start()
        return ret

    def _get_handler(self, name):
        for (p, h) in self.handlers:
            if p is None:
                return h
            if p.match(name):
                return h
