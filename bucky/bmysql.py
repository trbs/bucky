"""
bmysql.py - pulling metrics from the bucky server queue and ram them into
mysql as fast as the module is capable.

Simple thread pattern meant to be as fast as the python GIL can handle barring
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


class MysqlClient(threading.Thread):
    def __init__(self, queue, cfg):
        """
        setup connection and values needed for thread to run
        """
        super(MysqlClient, self).__init__()
        self.setDaemon(True)
        self.debug = cfg.debug
        self.queue = queue
        self.mysql_ip = cfg.mysql_ip
        self.mysql_db = cfg.mysql_db
        self.mysql_port = cfg.mysql_port
        self.mysql_user = cfg.mysql_user
        self.mysql_pass = cfg.mysql_pass
        self.mysql_enabled = cfg.mysql_enabled
        self.mysql_query = cfg.mysql_query
        self.qsize = 10
        self.connect()

    def run(self):
        """
        thread execution, light as possible, pull
        form queue and update metrics
        """
        while True:
            try:
                cursor = self.mysql_con.cursor()
                for i in range(0, self.qsize):
                    stat, value, time = self.queue.get(True, 1)
                    cursor.execute(self.mysql_query % (str(stat)))
                self.mysql_con.commit()
            except:
                continue
            finally:
                cursor.close()

    def connect(self):
        """
        initiates connection to mysql server
        """
        if self.debug:
            log.debug("Connected the debug socket.")
            self.sock = DebugSocket()
            return

        if self.mysql_enabled:
            import MySQLdb as mdb
            try:
                self.mysql_con = mdb.connect(self.mysql_ip, self.mysql_user,
                    self.mysql_pass, self.mysql_db, int(self.mysql_port))
                log.info("Connected to MySQL at %s:%s" %
                    (self.mysql_ip, self.mysql_port))
                return
            except:
                log.error("Error Connecting to MySQL at %s:%s" %
                    (self.mysql_ip, self.mysql_port))
                sys.exit(1)

    def close(self):
        """
        closes a connection to mysql server
        """
        try:
            self.mysql_con.close()
        except:
            pass
