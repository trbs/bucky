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

import six
import sys
import socket
import logging
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
        addrinfo = socket.getaddrinfo(ip, port, socket.AF_UNSPEC, socket.SOCK_DGRAM)
        af, socktype, proto, canonname, addr = addrinfo[0]
        ip, port = addr[:2]
        self.ip = ip
        self.port = port
        self.sock = socket.socket(af, socket.SOCK_DGRAM)
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
            addr = addr[:2]  # for compatibility with longer ipv6 tuples
            if data == b'EXIT':
                break
            if not self.handle(data, addr):
                break
        try:
            self.pre_shutdown()
        except:
            log.exception("Failed pre_shutdown method for %s" % self.__class__.__name__)

    def handle(self, data, addr):
        raise NotImplemented()

    def pre_shutdown(self):
        """ Pre shutdown hook """
        pass

    def close(self):
        self.send('EXIT')

    if six.PY3:
        def send(self, data):
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            if not isinstance(data, bytes):
                data = data.encode()
            sock.sendto(data, 0, (self.ip, self.port))
    else:
        def send(self, data):
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(data, 0, (self.ip, self.port))
