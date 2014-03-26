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
import time
import struct
import tempfile
try:
    import queue
except ImportError:
    import Queue as queue

import t
import bucky.collectd


def pkts(rfname):
    fname = os.path.join(os.path.dirname(__file__), rfname)
    with open(fname, 'rb') as handle:
        length = handle.read(2)
        while length:
            (dlen,) = struct.unpack("!H", length)
            yield handle.read(dlen)
            length = handle.read(2)


def test_pkt_reader():
    for pkt in pkts("collectd.pkts"):
        t.ne(len(pkt), 0)


@t.udp_srv(bucky.collectd.CollectDServer)
def test_simple_counter_old(q, s):
    s.send(next(pkts("collectd.pkts")))
    time.sleep(.1)
    s = q.get(True, .1)
    while s:
        print(s)
        try:
            s = q.get(True, .1)
        except queue.Empty:
            break


def temp_file(data):
    f = tempfile.NamedTemporaryFile(delete=False)
    filename = f.name
    f.write(data.encode('utf-8'))
    f.close()
    return filename


def cdtypes(typesdb):
    def types_dec(func):
        filename = temp_file(typesdb)
        return t.set_cfg("collectd_types", [filename])(func)
    return types_dec


def authfile(data):
    def authfile_dec(func):
        filename = temp_file(data)
        return t.set_cfg("collectd_auth_file", filename)(func)
    return authfile_dec


def send_get_data(q, s, datafile):
    for pkt in pkts(datafile):
        s.send(pkt)
    time.sleep(.1)
    while True:
        try:
            sample = q.get(True, .1)
        except queue.Empty:
            break
        yield sample


def check_samples(samples, seq_function, count, name):
    i = 0
    for sample in samples:
        if sample[1] != name:
            continue
        t.eq(sample[2], seq_function(i))
        i += 1
    t.eq(i, count)


TDB_GAUGE = "gauge value:GAUGE:U:U\n"
TDB_DERIVE = "derive value:DERIVE:U:U\n"
TDB_COUNTER = "counter value:COUNTER:U:U\n"
TDB_ABSOLUTE = "absolute value:ABSOLUTE:U:U\n"
TYPESDB = TDB_GAUGE + TDB_DERIVE + TDB_COUNTER + TDB_ABSOLUTE


@cdtypes(TYPESDB)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_simple_gauge(q, s):
    # raw values sent are i^2 for i in [0, 9]
    samples = send_get_data(q, s, 'collectd-squares.pkts')
    seq = lambda i: i ** 2
    check_samples(samples, seq, 10, 'test.squares.gauge')


@cdtypes(TYPESDB)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_simple_derive(q, s):
    # raw values sent are i^2 for i in [0, 9]
    # (i+1)^2-i^2=2*i+1, devided by 2 (time interval)
    samples = send_get_data(q, s, 'collectd-squares.pkts')
    seq = lambda i: (2 * i + 1) / 2.
    check_samples(samples, seq, 9, 'test.squares.derive')


@cdtypes(TYPESDB)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_simple_counter(q, s):
    # raw values sent are i^2 for i in [0, 9]
    # (i+1)^2-i^2=2*i+1, devided by 2 (time interval)
    samples = send_get_data(q, s, 'collectd-squares.pkts')
    seq = lambda i: (2 * i + 1) / 2.
    check_samples(samples, seq, 9, 'test.squares.counter')


@cdtypes("counters a:COUNTER:0:U, b:COUNTER:0:U\n")
@t.udp_srv(bucky.collectd.CollectDServer)
def test_counter_wrap_32(q, s):
    # counter growing 1024 per measurement, 2 seconds interval, expecting
    # 9 measurements with value 512
    samples = send_get_data(q, s, 'collectd-counter-wraps.pkts')
    seq = lambda i: 512
    check_samples(samples, seq, 9, 'test.counter-wraps.counters.a')


@cdtypes("counters a:COUNTER:0:U, b:COUNTER:0:U\n")
@t.udp_srv(bucky.collectd.CollectDServer)
def test_counter_wrap_64(q, s):
    # counter growing 1024 per measurement, 2 seconds interval, expecting
    # 9 measurements with value 512
    samples = send_get_data(q, s, 'collectd-counter-wraps.pkts')
    seq = lambda i: 512
    check_samples(samples, seq, 9, 'test.counter-wraps.counters.b')


@cdtypes(TYPESDB)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_simple_absolute(q, s):
    # raw values sent are i^2 for i in [0, 9], devided by 2 (time interval)
    samples = send_get_data(q, s, 'collectd-squares.pkts')
    seq = lambda i: (i + 1) ** 2 / 2.
    check_samples(samples, seq, 9, 'test.squares.absolute')


@cdtypes("gauge value:GAUGE:5:50\n" + TDB_DERIVE + TDB_COUNTER + TDB_ABSOLUTE)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_simple_gauge_bounds(q, s):
    # raw values sent are i^2 for i in [0, 9]
    samples = send_get_data(q, s, 'collectd-squares.pkts')
    seq = lambda i: (i + 3) ** 2
    check_samples(samples, seq, 5, 'test.squares.gauge')


@cdtypes("derive value:DERIVE:3:8\n" + TDB_GAUGE + TDB_COUNTER + TDB_ABSOLUTE)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_simple_derive_bounds(q, s):
    # raw values sent are i^2 for i in [0, 9]
    # (i+1)^2-i^2=2*i+1, devided by 2 (time interval)
    samples = send_get_data(q, s, 'collectd-squares.pkts')
    seq = lambda i: 3 + (2 * i + 1) / 2.
    check_samples(samples, seq, 5, 'test.squares.derive')


@cdtypes("counter value:COUNTER:3:8\n" + TDB_GAUGE + TDB_DERIVE + TDB_ABSOLUTE)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_simple_counter_bounds(q, s):
    # raw values sent are i^2 for i in [0, 9]
    # (i+1)^2-i^2=2*i+1, devided by 2 (time interval)
    samples = send_get_data(q, s, 'collectd-squares.pkts')
    seq = lambda i: 3 + (2 * i + 1) / 2.
    check_samples(samples, seq, 5, 'test.squares.counter')


@cdtypes("absolute value:ABSOLUTE:5:35\n" + TDB_GAUGE + TDB_DERIVE + TDB_COUNTER)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_simple_absolute_bounds(q, s):
    # raw values sent are i^2 for i in [0, 9], devided by 2 (time interval)
    samples = send_get_data(q, s, 'collectd-squares.pkts')
    seq = lambda i: (i + 4) ** 2 / 2.
    check_samples(samples, seq, 5, 'test.squares.absolute')


@t.set_cfg("collectd_security_level", 1)
@authfile("alice: 12345678")
@cdtypes(TYPESDB)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_net_auth(q, s):
    # raw values sent are i^2 for i in [0, 9]
    samples = send_get_data(q, s, 'collectd-squares-signed.pkts')
    seq = lambda i: (i ** 2)
    check_samples(samples, seq, 10, 'test.squares.gauge')


@t.set_cfg("collectd_security_level", 2)
@authfile("alice: 12345678")
@cdtypes(TYPESDB)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_net_enc(q, s):
    # raw values sent are i^2 for i in [0, 9]
    samples = send_get_data(q, s, 'collectd-squares-encrypted.pkts')
    seq = lambda i: (i ** 2)
    check_samples(samples, seq, 10, 'test.squares.gauge')
