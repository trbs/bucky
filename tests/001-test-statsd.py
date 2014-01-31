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

import t
import bucky.statsd


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8126)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_simple_counter(q, s):
    s.send("gorm:1|c")
    t.same_stat(None, "stats.gorm", 2, q.get())
    t.same_stat(None, "stats_counts.gorm", 1, q.get())
    t.same_stat(None, "stats.numStats", 1, q.get())


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8127)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_multiple_messages(q, s):
    s.send("gorm:1|c")
    s.send("gorm:1|c")
    t.same_stat(None, "stats.gorm", 4, q.get())
    t.same_stat(None, "stats_counts.gorm", 2, q.get())
    t.same_stat(None, "stats.numStats", 1, q.get())


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8128)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_larger_count(q, s):
    s.send("gorm:5|c")
    t.same_stat(None, "stats.gorm", 10, q.get())
    t.same_stat(None, "stats_counts.gorm", 5, q.get())
    t.same_stat(None, "stats.numStats", 1, q.get())


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8129)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_multiple_counters(q, s):
    s.send("gorm:1|c")
    s.send("gurm:1|c")
    stats = {
        "stats.gorm": 2,
        "stats_counts.gorm": 1,
        "stats.gurm": 2,
        "stats_counts.gurm": 1
    }
    for i in range(4):
        stat = q.get()
        t.isin(stat[1], stats)
        t.eq(stats[stat[1]], stat[2])
        t.gt(stat[2], 0)
        stats.pop(stat[1])
    t.same_stat(None, "stats.numStats", 2, q.get())


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8130)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_simple_timer(q, s):
    for i in range(9):
        s.send("gorm:1|ms")
    s.send("gorm:2|ms")
    t.same_stat(None, "stats.timers.gorm.mean", 1, q.get())
    t.same_stat(None, "stats.timers.gorm.upper", 2, q.get())
    t.same_stat(None, "stats.timers.gorm.upper_90", 1, q.get())
    t.same_stat(None, "stats.timers.gorm.lower", 1, q.get())
    t.same_stat(None, "stats.timers.gorm.count", 10, q.get())
    t.same_stat(None, "stats.numStats", 1, q.get())


@t.set_cfg("statsd_flush_time", 0.5)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_simple_gauge(q, s):
    s.send("gorm:5|g")
    t.same_stat(None, "stats.gauges.gorm", 5, q.get())
    t.same_stat(None, "stats.numStats", 1, q.get())
