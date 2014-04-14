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
import threading

import t

from bucky import cfg
from bucky.main import Bucky
from bucky.errors import BuckyError


def test_version_number():
    from bucky import version_info, __version__
    t.eq(__version__, ".".join(map(str, version_info)))


def test_sigterm_handling():
    alarm_thread = threading.Timer(2, os.kill, (os.getpid(), 15))
    alarm_thread.start()
    bucky = Bucky(cfg)
    t.not_raises(BuckyError, bucky.run)
