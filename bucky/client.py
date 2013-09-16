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
# Copyright 2012 Cloudant, Inc.

import multiprocessing

try:
    from setproctitle import setproctitle
except ImportError:
    def setproctitle(title):
        pass


class Client(multiprocessing.Process):
    def __init__(self, pipe):
        super(Client, self).__init__()
        self.daemon = True
        self.pipe = pipe

    def run(self):
        setproctitle("bucky: %s" % self.__class__.__name__)
        while True:
            self.send(*self.pipe.recv())

    def send(self, host, name, value, time):
        raise NotImplemented()
