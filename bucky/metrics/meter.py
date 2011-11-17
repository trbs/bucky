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

import time

from bucky.metrics.metric import Metric, MetricValue as MV
from bucky.metrics.stats.ewma import EWMA

class Meter(Metric):
    def __init__(self, name):
        self.name = name
        self.count = 0
        self.m1_rate = EWMA.oneMinuteEWMA()
        self.m5_rate = EWMA.fiveMinuteEWMA()
        self.m15_rate = EWMA.fifteenMinuteEWMA()
        self.start_time = time.time()

    def update(self, value=1):
        self.count += value
        self.m1_rate.update(value)
        self.m5_rate.update(value)
        self.m15_rate.update(value)

    def metrics(self):
        for r in (self.m1_rate, self.m5_rate, self.m15_rate):
            r.tick()
        ret = []
        elapsed = time.time() - self.start_time
        ret.append(MV("%s.count" % self.name, self.count))
        ret.append(MV("%s.rate_avg" % self.name, float(self.count) / elapsed))
        ret.append(MV("%s.rate_1m" % self.name, self.m1_rate.rate()))
        ret.append(MV("%s.rate_5m" % self.name, self.m5_rate.rate()))
        ret.append(MV("%s.rate_15m" % self.name, self.m15_rate.rate()))
        return ret
