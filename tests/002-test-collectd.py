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
import Queue
import struct

import t
import bucky.collectd

def pkts():
    fname = os.path.join(os.path.dirname(__file__), "collectd.pkts")
    with open(fname) as handle:
        length = handle.read(2)
        while length:
            (dlen,) = struct.unpack("!H", length)
            yield handle.read(dlen)
            length = handle.read(2)


def test_pkt_reader():
    for pkt in pkts():
        t.ne(len(pkt), 0)


@t.udp_srv(bucky.collectd.CollectDServer)
def test_simple_counter(q, s):
    s.send(pkts().next())
    s = q.get()
    while s:
        print s
        try:
            s = q.get(False)
        except Queue.Empty:
            break
