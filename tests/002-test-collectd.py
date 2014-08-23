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
try:
    import queue
except ImportError:
    import Queue as queue

import t
import bucky.collectd
from bucky import cfg
from bucky.errors import ProtocolError


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


@t.set_cfg("collectd_port", 25825)
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


def cdtypes(typesdb):
    def types_dec(func):
        filename = t.temp_file(typesdb)
        return t.set_cfg("collectd_types", [filename])(func)
    return types_dec


def authfile(data):
    def authfile_dec(func):
        filename = t.temp_file(data)
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
@t.set_cfg("collectd_port", 25827)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_simple_gauge(q, s):
    # raw values sent are i^2 for i in [0, 9]
    samples = send_get_data(q, s, 'collectd-squares.pkts')
    seq = lambda i: i ** 2
    check_samples(samples, seq, 10, 'test.squares.gauge')


@cdtypes(TYPESDB)
@t.set_cfg("collectd_port", 25828)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_simple_derive(q, s):
    # raw values sent are i^2 for i in [0, 9]
    # (i+1)^2-i^2=2*i+1, devided by 2 (time interval)
    samples = send_get_data(q, s, 'collectd-squares.pkts')
    seq = lambda i: (2 * i + 1) / 2.
    check_samples(samples, seq, 9, 'test.squares.derive')


@cdtypes(TYPESDB)
@t.set_cfg("collectd_port", 25829)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_simple_counter(q, s):
    # raw values sent are i^2 for i in [0, 9]
    # (i+1)^2-i^2=2*i+1, devided by 2 (time interval)
    samples = send_get_data(q, s, 'collectd-squares.pkts')
    seq = lambda i: (2 * i + 1) / 2.
    check_samples(samples, seq, 9, 'test.squares.counter')


@cdtypes("counters a:COUNTER:0:U, b:COUNTER:0:U\n")
@t.set_cfg("collectd_port", 25830)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_counter_wrap_32(q, s):
    # counter growing 1024 per measurement, 2 seconds interval, expecting
    # 9 measurements with value 512
    samples = send_get_data(q, s, 'collectd-counter-wraps.pkts')
    seq = lambda i: 512
    check_samples(samples, seq, 9, 'test.counter-wraps.counters.a')


@cdtypes("counters a:COUNTER:0:U, b:COUNTER:0:U\n")
@t.set_cfg("collectd_port", 25831)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_counter_wrap_64(q, s):
    # counter growing 1024 per measurement, 2 seconds interval, expecting
    # 9 measurements with value 512
    samples = send_get_data(q, s, 'collectd-counter-wraps.pkts')
    seq = lambda i: 512
    check_samples(samples, seq, 9, 'test.counter-wraps.counters.b')


@cdtypes(TYPESDB)
@t.set_cfg("collectd_port", 25832)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_simple_absolute(q, s):
    # raw values sent are i^2 for i in [0, 9], devided by 2 (time interval)
    samples = send_get_data(q, s, 'collectd-squares.pkts')
    seq = lambda i: (i + 1) ** 2 / 2.
    check_samples(samples, seq, 9, 'test.squares.absolute')


@cdtypes("gauge value:GAUGE:5:50\n" + TDB_DERIVE + TDB_COUNTER + TDB_ABSOLUTE)
@t.set_cfg("collectd_port", 25833)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_simple_gauge_bounds(q, s):
    # raw values sent are i^2 for i in [0, 9]
    samples = send_get_data(q, s, 'collectd-squares.pkts')
    seq = lambda i: (i + 3) ** 2
    check_samples(samples, seq, 5, 'test.squares.gauge')


@cdtypes("derive value:DERIVE:3:8\n" + TDB_GAUGE + TDB_COUNTER + TDB_ABSOLUTE)
@t.set_cfg("collectd_port", 25834)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_simple_derive_bounds(q, s):
    # raw values sent are i^2 for i in [0, 9]
    # (i+1)^2-i^2=2*i+1, devided by 2 (time interval)
    samples = send_get_data(q, s, 'collectd-squares.pkts')
    seq = lambda i: 3 + (2 * i + 1) / 2.
    check_samples(samples, seq, 5, 'test.squares.derive')


@cdtypes("counter value:COUNTER:3:8\n" + TDB_GAUGE + TDB_DERIVE + TDB_ABSOLUTE)
@t.set_cfg("collectd_port", 25835)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_simple_counter_bounds(q, s):
    # raw values sent are i^2 for i in [0, 9]
    # (i+1)^2-i^2=2*i+1, devided by 2 (time interval)
    samples = send_get_data(q, s, 'collectd-squares.pkts')
    seq = lambda i: 3 + (2 * i + 1) / 2.
    check_samples(samples, seq, 5, 'test.squares.counter')


@cdtypes("absolute value:ABSOLUTE:5:35\n" + TDB_GAUGE + TDB_DERIVE + TDB_COUNTER)
@t.set_cfg("collectd_port", 25836)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_simple_absolute_bounds(q, s):
    # raw values sent are i^2 for i in [0, 9], devided by 2 (time interval)
    samples = send_get_data(q, s, 'collectd-squares.pkts')
    seq = lambda i: (i + 4) ** 2 / 2.
    check_samples(samples, seq, 5, 'test.squares.absolute')


@t.set_cfg("collectd_security_level", 1)
@authfile("alice: 12345678")
@cdtypes(TYPESDB)
@t.set_cfg("collectd_port", 25837)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_net_auth(q, s):
    samples = send_get_data(q, s, 'collectd-squares-signed.pkts')
    seq = lambda i: (i ** 2)
    check_samples(samples, seq, 10, 'test.squares.gauge')


@t.set_cfg("collectd_security_level", 2)
@authfile("alice: 12345678")
@cdtypes(TYPESDB)
@t.set_cfg("collectd_port", 25838)
@t.udp_srv(bucky.collectd.CollectDServer)
def test_net_enc(q, s):
    samples = send_get_data(q, s, 'collectd-squares-encrypted.pkts')
    seq = lambda i: (i ** 2)
    check_samples(samples, seq, 10, 'test.squares.gauge')


def test_counter_eq_derive():
    """Test parsing of counters when expecting derives and vice versa"""

    def run_parse(parse_func):
        def wrapped(data):
            return list(parse_func(data))

        return wrapped

    types = "false_counter value:COUNTER:U:U\nfalse_derive value:DERIVE:U:U\n"
    with t.unlinking(t.temp_file(TYPESDB + types)) as path:
        parser = bucky.collectd.CollectDParser(types_dbs=[path])
        for pkts_file in ('collectd-counter.pkts', 'collectd-derive.pkts'):
            for data in pkts(pkts_file):
                t.not_raises(ProtocolError, run_parse(parser.parse), data)
        for pkts_file in ('collectd-false-counter.pkts',
                          'collectd-false-derive.pkts'):
            for data in pkts(pkts_file):
                t.raises(ProtocolError, run_parse(parser.parse), data)

        parser = bucky.collectd.CollectDParser(types_dbs=[path],
                                               counter_eq_derive=True)
        for pkts_file in ('collectd-counter.pkts', 'collectd-derive.pkts',
                          'collectd-false-counter.pkts',
                          'collectd-false-derive.pkts'):
            for data in pkts(pkts_file):
                t.not_raises(ProtocolError, run_parse(parser.parse), data)


def cfg_crypto(sec_level, auth_file):
    sec_level_dec = t.set_cfg('collectd_security_level', sec_level)
    auth_file_dec = authfile(auth_file)
    return sec_level_dec(auth_file_dec(bucky.collectd.CollectDCrypto))(cfg)


def assert_crypto(state, testfile, sec_level, auth_file):
    crypto = cfg_crypto(sec_level, auth_file)
    i = 0
    if state:
        for data in pkts(testfile):
            data = crypto.parse(data)
            t.eq(bool(data), True)
            i += 1
    else:
        for data in pkts(testfile):
            t.raises(ProtocolError, crypto.parse, data)
            i += 1
    t.eq(i, 2)


def test_crypto_sec_level_0():
    assert_crypto(True, 'collectd-squares.pkts', 0, "")
    assert_crypto(True, 'collectd-squares-signed.pkts', 0, "")
    assert_crypto(False, 'collectd-squares-encrypted.pkts', 0, "")
    assert_crypto(True, 'collectd-squares.pkts', 0, "alice: 12345678")
    assert_crypto(True, 'collectd-squares-signed.pkts', 0, "alice: 12345678")
    assert_crypto(True, 'collectd-squares-encrypted.pkts', 0, "alice: 12345678")


def test_crypto_sec_level_1():
    assert_crypto(False, 'collectd-squares.pkts', 1, "bob: 123")
    assert_crypto(False, 'collectd-squares-signed.pkts', 1, "bob: 123")
    assert_crypto(False, 'collectd-squares-encrypted.pkts', 1, "bob: 123")
    assert_crypto(False, 'collectd-squares.pkts', 1, "alice: 12345678")
    assert_crypto(True, 'collectd-squares-signed.pkts', 1, "alice: 12345678")
    assert_crypto(True, 'collectd-squares-encrypted.pkts', 1, "alice: 12345678")


def test_crypto_sec_level_2():
    assert_crypto(False, 'collectd-squares.pkts', 2, "bob: 123")
    assert_crypto(False, 'collectd-squares-signed.pkts', 2, "bob: 123")
    assert_crypto(False, 'collectd-squares-encrypted.pkts', 2, "bob: 123")
    assert_crypto(False, 'collectd-squares.pkts', 2, "alice: 12345678")
    assert_crypto(False, 'collectd-squares-signed.pkts', 2, "alice: 12345678")
    assert_crypto(True, 'collectd-squares-encrypted.pkts', 2, "alice: 12345678")


def test_crypto_auth_load():
    auth_file = "alice: 123\nbob:456  \n\n  charlie  :  789"
    crypto = cfg_crypto(2, auth_file)
    db = {"alice": "123", "bob": "456", "charlie": "789"}
    t.eq(crypto.auth_db, db)


def test_crypto_auth_reload():
    crypto = cfg_crypto(1, "bob: 123\n")
    signed_pkt = next(pkts('collectd-squares-signed.pkts'))
    enc_pkt = next(pkts('collectd-squares-encrypted.pkts'))
    t.raises(ProtocolError, crypto.parse, signed_pkt)
    t.raises(ProtocolError, crypto.parse, enc_pkt)
    with open(crypto.auth_file, "a") as f:
        f.write("alice: 12345678\n")
    time.sleep(1)
    t.eq(bool(crypto.parse(signed_pkt)), True)
    t.eq(bool(crypto.parse(enc_pkt)), True)
