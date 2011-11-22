#!/usr/bin/env python
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
#
# A simple script to collect some UDP packets from collectd for
# testing.

import socket
import struct
import sys


class LoggingServer(object):
    def __init__(self, ip, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((ip, port))

    def close(self):
        self.sock.close()

    def run(self, fname):
        with open(fname, "wb") as handle:
            for i in range(25):
                data, addr = self.sock.recvfrom(65535)
                self.write(handle, data)

    def write(self, dst, data):
        length = struct.pack("!H", len(data))
        dst.write(length)
        dst.write(data)


def main():
    ip, port = "127.0.0.1", 25826
    fname = "tests/collectd.pkts"
    if len(sys.argv) >= 2:
        ip = sys.argv[1]
    if len(sys.argv) >= 3:
        port = int(sys.argv[2])
    if len(sys.argv) >= 4:
        fname = sys.argv[3]
    server = LoggingServer(ip, port)
    try:
        server.run(fname)
    finally:
        server.close()


if __name__ == '__main__':
    main()
