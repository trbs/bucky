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
import re
import socket
import sys
import threading
import time


log = logging.getLogger(__name__)


class StatsDError(Exception):
    def __init__(self, mesg):
        self.mesg = mesg
    def __str__(self):
        return self.mesg


class BindError(StatsDError):
    pass


class StatsDHandler(threading.Thread):
    def __init__(self, queue, flush_time=10):
        super(StatsDHandler, self).__init__()
        self.setDaemon(True)
        self.queue = queue
        self.lock = threading.Lock()
        self.counters = {}
        self.timers = {}
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
                self.queue.put(("stats.numStats", num_stats, stime))

    def enqueue_timers(self, stime):
        ret = 0
        for k, v in self.timers.iteritems():
            v.sort()
            pct_thresh = 0.9
            count = len(v)
            vmin, vmax = v[0], v[-1]
            mean, vthres = vmin, vmax

            if count > 1:
                thresh_idx = int((100.0 - pct_thresh) / 100.0 * float(count))
                v = v[:thresh_idx]
                vthresh = v[-1]
                vsum = sum(v)
                mean = vsum / float(len(v))

            self.queue.put(("stats.timers.%s.mean" % k, mean, stime))
            self.queue.put(("stats.timers.%s.upper" % k, vmax, stime))
            t = int(pct_thresh * 100)
            self.queue.put(("stats.timers.%s.upper_%s" % (k,t), vthresh, stime))
            self.queue.put(("stats.timers.%s.lower" % k, vmin, stime))
            self.queue.put(("stats.timers.%s.count" % k, count, stime))
            self.timers[k] = []
            ret += 1

        return ret

    def enqueue_counters(self, stime):
        ret = 0
        for k, v in self.counters.iteritems():
            stat = "stats.%s" % k
            v = v / self.flush_interval
            self.queue.put((stat, v / self.flush_interval, stime))
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


class StatsDServer(threading.Thread):
    def __init__(self, queue, cfg):
        super(StatsDServer, self).__init__()
        self.setDaemon(True)
        self.handler = StatsDHandler(queue, flush_time=cfg["statsd_flush_time"])
        self.handler.start()
        self.sock = self.init_socket(cfg["statsd_ip"], cfg["statsd_port"])

    def init_socket(self, ip, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((ip, port))
            log.info("Opened statsd socket %s:%s" % (ip, port))
            return sock
        except Exception:
            log.error("Error opening statsd socket %s:%s." % (ip, port))
            sys.exit(1)

    def run(self):
        while True:
            data, addr = self.sock.recvfrom(65535)
            self.handler.handle(data)
            if not self.handler.is_alive():
                return
