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

import logging
import socket
import sys
import multiprocessing

import bucky.cfg as cfg

try:
    from setproctitle import setproctitle
except ImportError:
    def setproctitle(title):
        pass


log = logging.getLogger(__name__)


class UDPServer(multiprocessing.Process):
    def __init__(self, ip, port):
        super(UDPServer, self).__init__()
        self.daemon = True
        self.ip = ip
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.sock.bind((ip, port))
            log.info("Bound socket socket %s:%s", ip, port)
        except Exception:
            log.exception("Error binding socket %s:%s.", ip, port)
            sys.exit(1)

        self.sock_recvfrom = self.sock.recvfrom
        if cfg.debug:
            # When in debug mode replace the send and recvfrom functions to include
            # debug logging. In production mode these calls have quite a lot of overhead
            # for statements that will never do anything.
            import functools
            def debugsend(f):
                @functools.wraps(f)
                def wrapper(*args, **kwargs):
                    log.debug("Sending UDP packet to %s:%s", self.ip, self.port)
                    return f(*args, **kwargs)
                return wrapper
            self.send = debugsend(self.send)

            def debugrecvfrom(*args, **kwargs):
                data, addr = self.sock.recvfrom(65535)
                log.debug("Received UDP packet from %s:%s" % addr)
                return data, addr
            self.sock_recvfrom = debugrecvfrom

    def run(self):
        setproctitle("bucky: %s" % self.__class__.__name__)
        recvfrom = self.sock_recvfrom
        while True:
            data, addr = recvfrom(65535)
            if data == 'EXIT':
                return
            if not self.handle(data, addr):
                return

    def handle(self, data, addr):
        raise NotImplemented()

    def close(self):
        self.send('EXIT')

    def send(self, data):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(data, (self.ip, self.port))
