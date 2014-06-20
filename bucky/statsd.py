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

import os
import re
import six
import math
import time
import json
import logging
import threading
import bucky.udpserver as udpserver

log = logging.getLogger(__name__)

try:
    from io import open
except ImportError:
    # Python <2.6
    _open = open

    def open(*args, **kwargs):
        """
        Wrapper around open which does not support 'encoding' keyword in
        older versions of Python
        """
        kwargs.pop("encoding")
        return _open(*args, **kwargs)


if six.PY3:
    def read_json_file(gauges_filename):
        with open(gauges_filename, mode='r', encoding='utf-8') as f:
            return json.load(f)

    def write_json_file(gauges_filename, gauges):
        with open(gauges_filename, mode='w', encoding='utf-8') as f:
            json.dump(gauges, f)
else:
    def read_json_file(gauges_filename):
        with open(gauges_filename, mode='rb') as f:
            return json.load(f)

    def write_json_file(gauges_filename, gauges):
        with open(gauges_filename, mode='wb') as f:
            json.dump(gauges, f)


def make_name(parts):
    name = ""
    for part in parts:
        if part:
            name = name + part + "."
    return name


class StatsDHandler(threading.Thread):
    def __init__(self, queue, cfg):
        super(StatsDHandler, self).__init__()
        self.daemon = True
        self.queue = queue
        self.cfg = cfg
        self.lock = threading.Lock()
        self.timers = {}
        self.gauges = {}
        self.counters = {}
        self.flush_time = cfg.statsd_flush_time
        self.legacy_namespace = cfg.statsd_legacy_namespace
        self.global_prefix = cfg.statsd_global_prefix
        self.prefix_counter = cfg.statsd_prefix_counter
        self.prefix_timer = cfg.statsd_prefix_timer
        self.prefix_gauge = cfg.statsd_prefix_gauge
        self.key_res = (
            (re.compile("\s+"), "_"),
            (re.compile("\/"), "-"),
            (re.compile("[^a-zA-Z_\-0-9\.]"), "")
        )

        if self.legacy_namespace:
            self.name_global = 'stats.'
            self.name_legacy_rate = 'stats.'
            self.name_legacy_count = 'stats_counts.'
            self.name_timer = 'stats.timers.'
            self.name_gauge = 'stats.gauges.'
        else:
            self.name_global = make_name([self.global_prefix])
            self.name_counter = make_name([self.global_prefix, self.prefix_counter])
            self.name_timer = make_name([self.global_prefix, self.prefix_timer])
            self.name_gauge = make_name([self.global_prefix, self.prefix_gauge])

        self.statsd_persistent_gauges = cfg.statsd_persistent_gauges
        self.gauges_filename = os.path.join(self.cfg.directory, self.cfg.statsd_gauges_savefile)

    def load_gauges(self):
        if not self.statsd_persistent_gauges:
            return
        if not os.path.isfile(self.gauges_filename):
            return
        log.info("StatsD: Loading saved gauges %s", self.gauges_filename)
        try:
            gauges = read_json_file(self.gauges_filename)
        except IOError:
            log.exception("StatsD: IOError")
        else:
            self.gauges.update(gauges)

    def save_gauges(self):
        if not self.statsd_persistent_gauges:
            return
        try:
            write_json_file(self.gauges_filename, self.gauges)
        except IOError:
            log.exception("StatsD: IOError")

    def run(self):
        name_global_numstats = self.name_global + "numStats"
        while True:
            time.sleep(self.flush_time)
            stime = int(time.time())
            with self.lock:
                num_stats = self.enqueue_timers(stime)
                num_stats += self.enqueue_counters(stime)
                num_stats += self.enqueue_gauges(stime)
                self.enqueue(name_global_numstats, num_stats, stime)

    def enqueue(self, name, stat, stime):
        # No hostnames on statsd
        self.queue.put((None, name, stat, stime))

    def enqueue_timers(self, stime):
        ret = 0
        iteritems = self.timers.items() if six.PY3 else self.timers.iteritems()
        for k, v in iteritems:
            # Skip timers that haven't collected any values
            if not v:
                continue
            v.sort()
            pct_thresh = 90
            count = len(v)
            vmin, vmax = v[0], v[-1]
            mean, vthresh = vmin, vmax

            if count > 1:
                thresh_idx = int(math.floor(pct_thresh / 100.0 * count))
                v = v[:thresh_idx]
                vthresh = v[-1]
                vsum = sum(v)
                mean = vsum / float(len(v))

            self.enqueue("%s%s.mean" % (self.name_timer, k), mean, stime)
            self.enqueue("%s%s.upper" % (self.name_timer, k), vmax, stime)
            t = int(pct_thresh)
            self.enqueue("%s%s.upper_%s" % (self.name_timer, k, t), vthresh, stime)
            self.enqueue("%s%s.lower" % (self.name_timer, k), vmin, stime)
            self.enqueue("%s%s.count" % (self.name_timer, k), count, stime)
            self.timers[k] = []
            ret += 1

        return ret

    def enqueue_gauges(self, stime):
        ret = 0
        iteritems = self.gauges.items() if six.PY3 else self.gauges.iteritems()
        for k, v in iteritems:
            self.enqueue("%s%s" % (self.name_gauge, k), v, stime)
            ret += 1
        return ret

    def enqueue_counters(self, stime):
        ret = 0
        iteritems = self.counters.items() if six.PY3 else self.counters.iteritems()
        for k, v in iteritems:
            if self.legacy_namespace:
                stat_rate = "%s%s" % (self.name_legacy_rate, k)
                stat_count = "%s%s" % (self.name_legacy_count, k)
            else:
                stat_rate = "%s%s.rate" % (self.name_counter, k)
                stat_count = "%s%s.count" % (self.name_counter, k)
            self.enqueue(stat_rate, v / self.flush_time, stime)
            self.enqueue(stat_count, v, stime)
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
        bits = line.split(":")
        key = self.handle_key(bits.pop(0))

        if not bits:
            self.bad_line()
            return

        # I'm not sure if statsd is doing this on purpose
        # but the code allows for name:v1|t1:v2|t2 etc etc.
        # In the interest of compatibility, I'll maintain
        # the behavior.
        for sample in bits:
            if "|" not in sample:
                self.bad_line()
                continue
            fields = sample.split("|")
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
        valstr = fields[0] or "0"
        try:
            val = float(valstr)
        except:
            self.bad_line()
            return
        delta = valstr[0] in ["+", "-"]
        with self.lock:
            if delta and key in self.gauges:
                self.gauges[key] = self.gauges[key] + val
            else:
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
        log.error("StatsD: Invalid line: '%s'", self.line.strip())


class StatsDServer(udpserver.UDPServer):
    def __init__(self, queue, cfg):
        super(StatsDServer, self).__init__(cfg.statsd_ip, cfg.statsd_port)
        self.handler = StatsDHandler(queue, cfg)

    def pre_shutdown(self):
        self.handler.save_gauges()

    def run(self):
        self.handler.load_gauges()
        self.handler.start()
        super(StatsDServer, self).run()

    if six.PY3:
        def handle(self, data, addr):
            self.handler.handle(data.decode())
            if not self.handler.is_alive():
                return False
            return True
    else:
        def handle(self, data, addr):
            self.handler.handle(data)
            if not self.handler.is_alive():
                return False
            return True
