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
    t.same_stat(None, "stats.", {"numStats": 1}, q.get(timeout=TIMEOUT))


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8127)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_multiple_messages(q, s):
    s.send("gorm:1|c")
    s.send("gorm:1|c")
    t.same_stat(None, "stats.gorm", 4, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats_counts.gorm", 2, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.", {"numStats": 1}, q.get(timeout=TIMEOUT))


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8128)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_larger_count(q, s):
    s.send("gorm:5|c")
    t.same_stat(None, "stats.gorm", 10, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats_counts.gorm", 5, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.", {'numStats': 1}, q.get(timeout=TIMEOUT))


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
    t.same_stat(None, "stats.", {'numStats': 2}, q.get(timeout=TIMEOUT))


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8130)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_simple_timer(q, s):
    for i in range(9):
        s.send("gorm:1|ms")
    s.send("gorm:2|ms")  # Out of the 90% threshold
    expected_value = {
        "mean_90": 1,
        "upper_90": 1,
        "count_90": 9,
        "sum_90": 9,
        "sum_squares_90": 9,
        "mean": 1.1,
        "upper": 2,
        "lower": 1,
        "count": 10,
        "count_ps": 20.0,
        "median": 1,
        "sum": 11,
        "sum_squares": 13,
        "std": 0.3
    }
    t.same_stat(None, "stats.timers.gorm", expected_value, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.", {"numStats": 1}, q.get(timeout=TIMEOUT))


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8130)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_timer_unsorted(q, s):
    s.send("gorm:2|ms")
    s.send("gorm:5|ms")
    s.send("gorm:7|ms")  # Out of the 90% threshold
    s.send("gorm:3|ms")
    expected_value = {
        "mean_90": 10 / 3.0,
        "upper_90": 5,
        "count_90": 3,
        "sum_90": 10,
        "sum_squares_90": 38,
        "mean": 17 / 4.0,
        "upper": 7,
        "lower": 2,
        "count": 4,
        "count_ps": 8,
        "median": 4,
        "sum": 17,
        "sum_squares": 87,
        "std": 1.920286436967152
    }
    t.same_stat(None, "stats.timers.gorm", expected_value, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.", {"numStats": 1}, q.get(timeout=TIMEOUT))


@t.set_cfg("statsd_flush_time", 0.1)
@t.set_cfg("statsd_port", 8130)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_timer_single_time(q, s):
    s.send("gorm:100|ms")
    expected_value = {
        "mean": 100,
        "upper": 100,
        "lower": 100,
        "count": 1,
        "count_ps": 10,
        "median": 100,
        "sum": 100,
        "sum_squares": 10000,
        "std": 0
    }
    t.same_stat(None, "stats.timers.gorm", expected_value, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.", {"numStats": 1}, q.get(timeout=TIMEOUT))


@t.set_cfg("statsd_flush_time", 0.1)
@t.set_cfg("statsd_port", 8130)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_timer_multiple_times(q, s):
    s.send("gorm:100|ms")
    s.send("gorm:200|ms")
    s.send("gorm:300|ms")  # Out of the 90% threshold
    expected_value = {
        "mean_90": 150,
        "upper_90": 200,
        "count_90": 2,
        "sum_90": 300,
        "sum_squares_90": 50000,
        "mean": 200,
        "upper": 300,
        "lower": 100,
        "count": 3,
        "count_ps": 30,
        "median": 200,
        "sum": 600,
        "sum_squares": 140000,
        "std": 81.64965809277261
    }
    t.same_stat(None, "stats.timers.gorm", expected_value, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.", {"numStats": 1}, q.get(timeout=TIMEOUT))


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
    returned_value = q.get(timeout=TIMEOUT)
    returned_value = returned_value[:2] + (returned_value[2]["median"],) + returned_value[3:]
    t.same_stat(None, "stats.timers.gorm", 250, returned_value)


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8131)
@t.set_cfg("statsd_legacy_namespace", False)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_simple_counter_not_legacy_namespace(q, s):
    s.send("gorm:1|c")
    t.same_stat(None, "stats.counters.gorm", {"rate": 2, "count": 1}, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.", {"numStats": 1}, q.get(timeout=TIMEOUT))


@t.set_cfg("statsd_flush_time", 0.5)
@t.set_cfg("statsd_port", 8132)
@t.udp_srv(bucky.statsd.StatsDServer)
def test_simple_gauge(q, s):
    s.send("gorm:5|g")
    t.same_stat(None, "stats.gauges.gorm", 5, q.get(timeout=TIMEOUT))
    t.same_stat(None, "stats.", {"numStats": 1}, q.get(timeout=TIMEOUT))


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
        s.handle_line("gorm:5|g")
        assert s.gauges[("gorm", None)] == 5

        s.save_gauges()

        s.handle_line("gorm:1|g")
        assert s.gauges[("gorm", None)] == 1

        s.load_gauges()
        assert s.gauges[("gorm", None)] == 5
    finally:
        if os.path.isfile(os.path.join(t.cfg.directory, t.cfg.statsd_gauges_savefile)):
            os.unlink(os.path.join(t.cfg.directory, t.cfg.statsd_gauges_savefile))
        os.removedirs(t.cfg.directory)
