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
import math
import re
import threading
import time

import bucky.udpserver as udpserver


log = logging.getLogger(__name__)


class StatsDHandler(threading.Thread):
    def __init__(self, queue, flush_time=10):
        super(StatsDHandler, self).__init__()
        self.setDaemon(True)
        self.queue = queue
        self.lock = threading.Lock()
        self.timers = {}
        self.gauges = {}
        self.counters = {}
        self.flush_time = flush_time
        self.key_res = (
            (re.compile("\s+"), "_"),
            (re.compile("\/"), "-"),
            (re.compile("[^a-zA-Z_\-0-9\.]"), "")
        )

    def run(self):
        while True:
            time.sleep(self.flush_time)
            stime = int(time.time())
            with self.lock:
                num_stats = self.enqueue_timers(stime)
                num_stats += self.enqueue_counters(stime)
                num_stats += self.enqueue_gauges(stime)
                self.enqueue("stats.numStats", num_stats, stime)

    def enqueue(self, name, stat, stime):
        # No hostnames on statsd
        self.queue.put((None, name, stat, stime))

    def enqueue_timers(self, stime):
        ret = 0
        for k, v in self.timers.iteritems():
            # Skip timers that haven't collected any values
            if not v:
                continue
            v.sort()
            pct_thresh = 90
            count = len(v)
            vmin, vmax = v[0], v[-1]
            mean, vthresh = vmin, vmax

            if count > 1:
                thresh_idx = int(math.floor(float(pct_thresh) / 100.0 * count))
                v = v[:thresh_idx]
                vthresh = v[-1]
                vsum = sum(v)
                mean = vsum / float(len(v))

            self.enqueue("stats.timers.%s.mean" % k, mean, stime)
            self.enqueue("stats.timers.%s.upper" % k, vmax, stime)
            t = int(pct_thresh)
            self.enqueue("stats.timers.%s.upper_%s" % (k,t), vthresh, stime)
            self.enqueue("stats.timers.%s.lower" % k, vmin, stime)
            self.enqueue("stats.timers.%s.count" % k, count, stime)
            self.timers[k] = []
            ret += 1

        return ret

    def enqueue_gauges(self, stime):
        ret = 0
        for k, v in self.gauges.iteritems():
            stat = "stats.gauges.%s" % k
            self.enqueue(stat, v, stime)
            self.gauges[k] = 0
            ret += 1
        return ret

    def enqueue_counters(self, stime):
        ret = 0
        for k, v in self.counters.iteritems():
            stat = "stats.%s" % k
            self.enqueue(stat, v / self.flush_time, stime)
            stat = "stats_counts.%s" % k
            self.enqueue(stat, v, stime)
            self.counters[k] = 0
            ret += 1
        return ret

    def handle(self, data):
        # Adding a bit of extra sauce so clients can
        # send multiple samples in a single UDP
        # packet.
        for line in data.splitlines():
            self.line = line
            if not line.strip():
                continue
            self.handle_line(line)

    def handle_line(self, line):
        bits = line.split(":", 1)
        key = self.handle_key(bits.pop(0))

        if len(bits) == 0:
            self.bad_line()
            return

        # I'm not sure if statsd is doing this on purpose
        # but the code allows for name:v1|t1:v2|t2 etc etc.
        # In the interest of compatibility, I'll maintain
        # the behavior.
        for sample in bits:
            fields = sample.split("|")
            if len(fields) < 2:
                self.bad_line()
                continue
            if fields[1] == "ms":
                self.handle_timer(key, fields)
            elif fields[1] == "g":
                self.handle_gauge(key, fields)
            else:
                self.handle_counter(key, fields)

    def handle_key(self, key):
        for (rexp, repl) in self.key_res:
            key = rexp.sub(repl, key)
        return key

    def handle_timer(self, key, fields):
        try:
            val = float(fields[0] or 0)
            with self.lock:
                self.timers.setdefault(key, []).append(val)
        except:
            self.bad_line()

    def handle_gauge(self, key, fields):
        try:
            val = int(fields[0] or 0)
        except:
            self.bad_line()
            return
        with self.lock:
            if key not in self.gauges:
                self.gauges[key] = 0
            self.gauges[key] = val

    def handle_counter(self, key, fields):
        rate = 1.0
        if len(fields) > 2 and fields[2][:1] == "@":
            try:
                rate = float(fields[2][1:].strip())
            except:
                rate = 1.0
        try:
            val = int(float(fields[0] or 0) / rate)
        except:
            self.bad_line()
            return
        with self.lock:
            if key not in self.counters:
                self.counters[key] = 0
            self.counters[key] += val

    def bad_line(self):
        log.error("StatsD: Invalid line: '%s'" % self.line.strip())


class StatsDServer(udpserver.UDPServer):
    def __init__(self, queue, cfg):
        super(StatsDServer, self).__init__(cfg.statsd_ip, cfg.statsd_port)
        self.handler = StatsDHandler(queue, flush_time=cfg.statsd_flush_time)
        self.handler.start()

    def handle(self, data, addr):
        self.handler.handle(data)
        if not self.handler.is_alive():
            return False
        return True
