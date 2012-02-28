"""
bmemcache.py - pulling metrics from the bucky serve queue and sending them
to memcache servers in the form of two keys for value and timestamp. Simple
thread pattern meant to be as fast as the python GIL can handle barring
IO wait.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.

2012, Karsten McMinn, Playfish-EA
"""

import logging
import sys
import threading
import Queue


log = logging.getLogger(__name__)


class DebugSocket(object):
    def sendall(self, data):
        sys.stdout.write(data)


class MemcacheClient(threading.Thread):
    def __init__(self, queue, cfg):
        """
        sets values needed by thread, connect to memcache
        """
        super(MemcacheClient, self).__init__()
        self.setDaemon(True)
        self.queue = queue
        self.debug = cfg.debug
        self.memcache_enabled = cfg.memcache_enabled
        self.memcache_ip = cfg.memcache_ip
        self.memcache_port = cfg.memcache_port
        self.keys = {}
        self.qsize = 5
        self.connect()

    def run(self):
        """
        thread exezcution, pull from queue, expand metric
        to two keys and write to memcache server(s)
        """
        while True:
            try:
                for i in range(0, self.qsize):
                    stat, value, time = self.queue.get(True, 1)
                    self.keys[str(stat + ".v")] = str(value)
                    self.keys[str(stat + ".t")] = str(time)
                self.memch.set_multi(self.keys, time=0)
                self.keys = {}
                continue
            except:
                # Queue.Empty, etc
                continue
            finally:
                pass

    def connect(self):
        """
        connect to memcache hosts
        """
        if self.debug:
            log.debug("Connected the debug socket.")
            self.sock = DebugSocket()

        try:
            import memcache
        except:
            log.error("Couldn't import memcache module. Exiting")
            sys.exit()

        self.memch = memcache.Client(self.memcache_ip, debug=0)
        log.info("Connected to memcache at %s" % (self.memcache_ip))

    def close(self):
        """
        exit thread on close, no close method in memcache module
        """
        try:
            sys.exit()
        except:
            pass
