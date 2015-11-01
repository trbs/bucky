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


TIMEOUT = 3


def test_make_name():
    assert bucky.statsd.make_name(["these", "are", "some", "parts"]) == "these.are.some.parts."
    assert bucky.statsd.make_name(["these", "are", None, "parts"]) == "these.are.parts."
    assert bucky.statsd.make_name(["these", "are", None, ""]) == "these.are."


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8126)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_simple_counter(q, s):
    s.send("gorm:1|c")
    t.same_stat(None, "stats.gorm", 2, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats_counts.gorm", 1, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.numStats", 1, q.get(timeout=TIMEOUT))


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8127)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_multiple_messages(q, s):
    s.send("gorm:1|c")
    s.send("gorm:1|c")
    t.same_stat(None, "stats.gorm", 4, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats_counts.gorm", 2, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.numStats", 1, q.get(timeout=TIMEOUT))


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8128)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_larger_count(q, s):
    s.send("gorm:5|c")
    t.same_stat(None, "stats.gorm", 10, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats_counts.gorm", 5, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.numStats", 1, q.get(timeout=TIMEOUT))


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
        stat = q.get(timeout=TIMEOUT)
        t.isin(stat[1], stats)
        t.eq(stats[stat[1]], stat[2])
        t.gt(stat[2], 0)
        stats.pop(stat[1])
    t.same_stat(None, "stats.numStats", 2, q.get(timeout=TIMEOUT))


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8130)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_simple_timer(q, s):
    for i in range(9):
        s.send("gorm:1|ms")
    s.send("gorm:2|ms")  # Out of the 90% threshold
    t.same_stat(None, "stats.timers.gorm.mean_90", 1, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.upper_90", 1, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.count_90", 9, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.sum_90", 9, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.sum_squares_90", 9, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.mean", 1.1, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.upper", 2, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.lower", 1, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.count", 10, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.count_ps", 20.0, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.median", 1, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.sum", 11, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.sum_squares", 13, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.std", 0.3, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.numStats", 1, q.get(timeout=TIMEOUT))


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8130)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_timer_unsorted(q, s):
    s.send("gorm:2|ms")
    s.send("gorm:5|ms")
    s.send("gorm:7|ms")  # Out of the 90% threshold
    s.send("gorm:3|ms")
    t.same_stat(None, "stats.timers.gorm.mean_90", 10 / 3.0, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.upper_90", 5, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.count_90", 3, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.sum_90", 10, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.sum_squares_90", 38, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.mean", 17 / 4.0, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.upper", 7, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.lower", 2, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.count", 4, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.count_ps", 8, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.median", 4, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.sum", 17, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.sum_squares", 87, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.std", 1.920286436967152, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.numStats", 1, q.get(timeout=TIMEOUT))


@t.set_cfg("statsd_flush_time", 0.1)
@t.set_cfg("statsd_port", 8130)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_timer_single_time(q, s):
    s.send("gorm:100|ms")
    t.same_stat(None, "stats.timers.gorm.mean", 100, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.upper", 100, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.lower", 100, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.count", 1, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.count_ps", 10, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.median", 100, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.sum", 100, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.sum_squares", 10000, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.std", 0, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.numStats", 1, q.get(timeout=TIMEOUT))


@t.set_cfg("statsd_flush_time", 0.1)
@t.set_cfg("statsd_port", 8130)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_timer_multiple_times(q, s):
    s.send("gorm:100|ms")
    s.send("gorm:200|ms")
    s.send("gorm:300|ms")  # Out of the 90% threshold
    t.same_stat(None, "stats.timers.gorm.mean_90", 150, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.upper_90", 200, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.count_90", 2, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.sum_90", 300, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.sum_squares_90", 50000, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.mean", 200, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.upper", 300, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.lower", 100, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.count", 3, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.count_ps", 30, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.median", 200, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.sum", 600, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.sum_squares", 140000, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.timers.gorm.std", 81.64965809277261, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.numStats", 1, q.get(timeout=TIMEOUT))


def queue_skip(q, number_of_elements):
    """
    Skip some elements from a Queue object
    """
    for _ in range(number_of_elements):
        q.get(timeout=TIMEOUT)


@t.set_cfg("statsd_flush_time", 0.1)
@t.set_cfg("statsd_port", 8130)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_timer_multiple_times_even(q, s):
    s.send("gorm:300|ms")
    s.send("gorm:200|ms")
    s.send("gorm:400|ms")
    s.send("gorm:100|ms")
    queue_skip(q, 10)
    t.same_stat(None, "stats.timers.gorm.median", 250, q.get(timeout=TIMEOUT))
    queue_skip(q, 4)


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8131)
@t.set_cfg("statsd_legacy_namespace", False)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_simple_counter_not_legacy_namespace(q, s):
    s.send("gorm:1|c")
    t.same_stat(None, "stats.counters.gorm.rate", 2, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.counters.gorm.count", 1, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.numStats", 1, q.get(timeout=TIMEOUT))


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8132)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_simple_gauge(q, s):
    s.send("gorm:5|g")
    t.same_stat(None, "stats.gauges.gorm", 5, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.numStats", 1, q.get(timeout=TIMEOUT))


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
