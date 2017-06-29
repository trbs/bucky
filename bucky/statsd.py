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


class StatsDServer(udpserver.UDPServer):
    def __init__(self, queue, cfg):
        super(StatsDServer, self).__init__(cfg.statsd_ip, cfg.statsd_port)
        self.daemon = True
        self.queue = queue
        self.cfg = cfg
        self.lock = threading.Lock()
        self.timers = {}
        self.gauges = {}
        self.counters = {}
        self.sets = {}
        self.flush_time = cfg.statsd_flush_time
        self.legacy_namespace = cfg.statsd_legacy_namespace
        self.global_prefix = cfg.statsd_global_prefix
        self.prefix_counter = cfg.statsd_prefix_counter
        self.prefix_timer = cfg.statsd_prefix_timer
        self.prefix_gauge = cfg.statsd_prefix_gauge
        self.prefix_set = cfg.statsd_prefix_set
        metadata = {}
        if cfg.metadata:
            metadata.update(cfg.metadata)
        if cfg.system_stats_metadata:
            metadata.update(cfg.system_stats_metadata)
        self.metadata = tuple((k, metadata[k]) for k in metadata.keys())
        self.key_res = (
            (re.compile("\s+"), "_"),
            (re.compile("\/"), "-"),
            (re.compile("[^a-zA-Z_\-0-9\.]"), "")
        )

        self.enqueue = self.enqueue_with_dotted_names
        if cfg.statsd_metadata_namespace:
            self.name_global = self.global_prefix
            self.name_counter = self.global_prefix + self.prefix_counter
            self.name_timer = self.global_prefix + self.prefix_timer
            self.name_gauge = self.global_prefix + self.prefix_gauge
            self.name_set = self.global_prefix + self.prefix_set
            self.enqueue = self.enqueue_with_metadata_names
        elif self.legacy_namespace:
            self.name_global = 'stats.'
            self.name_legacy_rate = 'stats.'
            self.name_legacy_count = 'stats_counts.'
            self.name_timer = 'stats.timers.'
            self.name_gauge = 'stats.gauges.'
            self.name_set = 'stats.sets.'
        else:
            self.name_global = make_name([self.global_prefix])
            self.name_counter = make_name([self.global_prefix, self.prefix_counter])
            self.name_timer = make_name([self.global_prefix, self.prefix_timer])
            self.name_gauge = make_name([self.global_prefix, self.prefix_gauge])
            self.name_set = make_name([self.global_prefix, self.prefix_set])

        self.statsd_persistent_gauges = cfg.statsd_persistent_gauges
        self.gauges_filename = os.path.join(self.cfg.directory, self.cfg.statsd_gauges_savefile)

        self.pct_thresholds = cfg.statsd_percentile_thresholds

        self.keys_seen = set()
        self.delete_idlestats = cfg.statsd_delete_idlestats
        self.delete_counters = self.delete_idlestats and cfg.statsd_delete_counters
        self.delete_timers = self.delete_idlestats and cfg.statsd_delete_timers
        self.delete_sets = self.delete_idlestats and cfg.statsd_delete_sets
        self.onlychanged_gauges = self.delete_idlestats and cfg.statsd_onlychanged_gauges
        self.ignore_datadog_extensions = cfg.statsd_ignore_datadog_extensions
        self.ignore_internal_stats = cfg.statsd_ignore_internal_stats

        self.enable_timer_mean = cfg.statsd_timer_mean
        self.enable_timer_upper = cfg.statsd_timer_upper
        self.enable_timer_lower = cfg.statsd_timer_lower
        self.enable_timer_count = cfg.statsd_timer_count
        self.enable_timer_count_ps = cfg.statsd_timer_count_ps
        self.enable_timer_sum = cfg.statsd_timer_sum
        self.enable_timer_sum_squares = cfg.statsd_timer_sum_squares
        self.enable_timer_median = cfg.statsd_timer_median
        self.enable_timer_std = cfg.statsd_timer_std

    def pre_shutdown(self):
        self.save_gauges()

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
            for gauge_name, gauge_metadata, gauge_value in gauges:
                k = (gauge_name, gauge_metadata)
                self.gauges[k] = gauge_value
                self.keys_seen.add(k)

    def save_gauges(self):
        if not self.statsd_persistent_gauges:
            return
        try:
            gauges = []
            for k in self.gauges.keys():
                gauge_name, gauge_metadata = k
                gauges.append((gauge_name, gauge_metadata, self.gauges[k]))
            write_json_file(self.gauges_filename, gauges)
        except IOError:
            log.exception("StatsD: IOError")

    def tick(self):
        stime = int(time.time())
        with self.lock:
            if self.delete_timers:
                rem_keys = set(self.timers.keys()) - self.keys_seen
                for k in rem_keys:
                    del self.timers[k]
            if self.delete_counters:
                rem_keys = set(self.counters.keys()) - self.keys_seen
                for k in rem_keys:
                    del self.counters[k]
            if self.delete_sets:
                rem_keys = set(self.sets.keys()) - self.keys_seen
                for k in rem_keys:
                    del self.sets[k]
            num_stats = self.enqueue_timers(stime)
            num_stats += self.enqueue_counters(stime)
            num_stats += self.enqueue_gauges(stime)
            num_stats += self.enqueue_sets(stime)
            if not self.ignore_internal_stats:
                self.enqueue(self.name_global, None, {"numStats": num_stats}, stime)
            self.keys_seen = set()

    def run(self):
        def flush_loop():
            while True:
                time.sleep(self.flush_time)
                self.tick()
        self.load_gauges()
        threading.Thread(target=flush_loop).start()
        super(StatsDServer, self).run()

    def enqueue_with_dotted_names(self, bucket, name, value, stime, metadata=None):
        # No hostnames on statsd
        if name:
            bucket += name
        if metadata:
            self.queue.put((None, bucket, value, stime, metadata))
        else:
            self.queue.put((None, bucket, value, stime))

    def enqueue_with_metadata_names(self, bucket, name, value, stime, metadata=None):
        # No hostnames on statsd
        if metadata:
            if name:
                metadata = metadata + (('name', name),)
            self.queue.put((None, bucket, value, stime, metadata))
        else:
            if name:
                self.queue.put((None, bucket, value, stime, (('name', name),)))
            else:
                self.queue.put((None, bucket, value, stime))

    def enqueue_timers(self, stime):
        ret = 0
        iteritems = self.timers.items() if six.PY3 else self.timers.iteritems()
        for k, v in iteritems:
            timer_name, timer_metadata = k
            timer_stats = {}

            # Skip timers that haven't collected any values
            if not v:
                timer_stats['count'] = 0
                timer_stats['count_ps'] = 0.0
            else:
                v.sort()
                count = len(v)
                vmin, vmax = v[0], v[-1]

                cumulative_values = [vmin]
                cumul_sum_squares_values = [vmin * vmin]
                for i, value in enumerate(v):
                    if i == 0:
                        continue
                    cumulative_values.append(value + cumulative_values[i - 1])
                    cumul_sum_squares_values.append(
                        value * value + cumul_sum_squares_values[i - 1])

                for pct_thresh in self.pct_thresholds:
                    thresh_idx = int(math.floor(pct_thresh / 100.0 * count))
                    if thresh_idx == 0:
                        continue
                    vsum = cumulative_values[thresh_idx - 1]

                    t = int(pct_thresh)
                    t_suffix = "_%s" % (t,)
                    if self.enable_timer_mean:
                        mean = vsum / float(thresh_idx)
                        timer_stats["mean" + t_suffix] = mean

                    if self.enable_timer_upper:
                        vthresh = v[thresh_idx - 1]
                        timer_stats["upper" + t_suffix] = vthresh

                    if self.enable_timer_count:
                        timer_stats["count" + t_suffix] = thresh_idx

                    if self.enable_timer_sum:
                        timer_stats["sum" + t_suffix] = vsum

                    if self.enable_timer_sum_squares:
                        vsum_squares = cumul_sum_squares_values[thresh_idx - 1]
                        timer_stats["sum_squares" + t_suffix] = vsum_squares

                vsum = cumulative_values[count - 1]
                mean = vsum / float(count)

                if self.enable_timer_mean:
                    timer_stats["mean"] = mean

                if self.enable_timer_upper:
                    timer_stats["upper"] = vmax

                if self.enable_timer_lower:
                    timer_stats["lower"] = vmin

                if self.enable_timer_count:
                    timer_stats["count"] = count

                if self.enable_timer_count_ps:
                    timer_stats["count_ps"] = float(count) / self.flush_time

                if self.enable_timer_median:
                    mid = int(count / 2)
                    median = (v[mid - 1] + v[mid]) / 2.0 if count % 2 == 0 else v[mid]
                    timer_stats["median"] = median

                if self.enable_timer_sum:
                    timer_stats["sum"] = vsum

                if self.enable_timer_sum_squares:
                    vsum_squares = cumul_sum_squares_values[count - 1]
                    timer_stats["sum_squares"] = vsum_squares

                if self.enable_timer_std:
                    sum_of_diffs = sum(((value - mean) ** 2 for value in v))
                    stddev = math.sqrt(sum_of_diffs / count)
                    timer_stats["std"] = stddev

            if timer_stats:
                self.enqueue(self.name_timer, timer_name, timer_stats, stime, timer_metadata)

            self.timers[k] = []
            ret += 1

        return ret

    def enqueue_sets(self, stime):
        ret = 0
        iteritems = self.sets.items() if six.PY3 else self.sets.iteritems()
        for k, v in iteritems:
            set_name, set_metadata = k
            self.enqueue(self.name_set, set_name, {"count": len(v)}, stime, set_metadata)
            ret += 1
            self.sets[k] = set()
        return ret

    def enqueue_gauges(self, stime):
        ret = 0
        iteritems = self.gauges.items() if six.PY3 else self.gauges.iteritems()
        for k, v in iteritems:
            gauge_name, gauge_metadata = k
            # only send a value if there was an update if `delete_idlestats` is `True`
            if not self.onlychanged_gauges or k in self.keys_seen:
                self.enqueue(self.name_gauge, gauge_name, v, stime, gauge_metadata)
                ret += 1
        return ret

    def enqueue_counters(self, stime):
        ret = 0
        iteritems = self.counters.items() if six.PY3 else self.counters.iteritems()
        for k, v in iteritems:
            counter_name, counter_metadata = k
            if self.legacy_namespace:
                self.enqueue(self.name_legacy_rate, counter_name, v / self.flush_time, stime, counter_metadata)
                self.enqueue(self.name_legacy_count, counter_name, v, stime, counter_metadata)
            else:
                stats = {
                    'rate': v / self.flush_time,
                    'count': v
                }
                self.enqueue(self.name_counter, counter_name, stats, stime, counter_metadata)
            self.counters[k] = 0
            ret += 1
        return ret

    def handle(self, data, addr):
        # Adding a bit of extra sauce so clients can
        # send multiple samples in a single UDP
        # packet.
        if six.PY3:
            data = data.decode()
        for line in data.splitlines():
            self.line = line
            if not line.strip():
                continue
            self.handle_line(line)
        return True

    def handle_tags(self, line):
        # http://docs.datadoghq.com/guides/dogstatsd/#datagram-format
        bits = line.split("#")
        if len(bits) < 2:
            return line, self.metadata
        tags = dict(self.metadata)
        for i in bits[1].split(","):
            kv = i.split("=")
            if len(kv) > 1:
                tags[kv[0]] = kv[1]
            else:
                kv = i.split(":")
                if len(kv) > 1:
                    tags[kv[0]] = kv[1]
                else:
                    tags[kv[0]] = None
        return bits[0], tuple((k, tags[k]) for k in sorted(tags.keys()))

    def handle_line(self, line):
        if self.ignore_datadog_extensions:
            if line.startswith('sc|') or line.startswith('_e{'):
                return
        line, tags = self.handle_tags(line)
        bits = line.split(":")
        key = self.handle_key(bits.pop(0), tags)

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
            elif fields[1] == "s":
                self.handle_set(key, fields)
            else:
                self.handle_counter(key, fields)

    def handle_key(self, key, tags):
        for (rexp, repl) in self.key_res:
            key = rexp.sub(repl, key)
        key = (key, tags)
        self.keys_seen.add(key)
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

    def handle_set(self, key, fields):
        valstr = fields[0] or "0"
        with self.lock:
            if key not in self.sets:
                self.sets[key] = set()
            self.sets[key].add(valstr)

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
