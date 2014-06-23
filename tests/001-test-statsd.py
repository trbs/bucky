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
import os
import bucky.statsd


def test_make_name():
    assert bucky.statsd.make_name(["these", "are", "some", "parts"]) == "these.are.some.parts."
    assert bucky.statsd.make_name(["these", "are", None, "parts"]) == "these.are.parts."
    assert bucky.statsd.make_name(["these", "are", None, ""]) == "these.are."


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
    t.same_stat(None, "stats.timers.gorm.count_ps", 20, q.get())
    t.same_stat(None, "stats.numStats", 1, q.get())


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8131)
@t.set_cfg("statsd_legacy_namespace", False)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_simple_counter_not_legacy_namespace(q, s):
    s.send("gorm:1|c")
    t.same_stat(None, "stats.counters.gorm.rate", 2, q.get())
    t.same_stat(None, "stats.counters.gorm.count", 1, q.get())
    t.same_stat(None, "stats.numStats", 1, q.get())


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8132)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_simple_gauge(q, s):
    s.send("gorm:5|g")
    t.same_stat(None, "stats.gauges.gorm", 5, q.get())
    t.same_stat(None, "stats.numStats", 1, q.get())


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8133)
@t.set_cfg("statsd_persistent_gauges", True)
@t.set_cfg("directory", "/tmp/var_lib_bucky")
@t.udp_srv(bucky.statsd.StatsDServer)
def test_simple_persistent_gauges(q, s):
    if not os.path.isdir(t.cfg.directory):
        os.makedirs(t.cfg.directory)
    if os.path.isfile(os.path.join(t.cfg.directory, t.cfg.statsd_gauges_savefile)):
        os.unlink(os.path.join(t.cfg.directory, t.cfg.statsd_gauges_savefile))
    try:
        s.handler.handle_line("gorm:5|g")
        assert s.handler.gauges["gorm"] == 5

        s.handler.save_gauges()

        s.handler.handle_line("gorm:1|g")
        assert s.handler.gauges["gorm"] == 1

        s.handler.load_gauges()
        assert s.handler.gauges["gorm"] == 5
    finally:
        if os.path.isfile(os.path.join(t.cfg.directory, t.cfg.statsd_gauges_savefile)):
            os.unlink(os.path.join(t.cfg.directory, t.cfg.statsd_gauges_savefile))
        os.removedirs(t.cfg.directory)
