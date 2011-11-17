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

from bucky.metrics.histogram import Histogram
from bucky.metrics.meter import Meter
from bucky.metrics.metric import Metric


class Timer(Metric):
    def __init__(self, name):
        self.name = name
        self.meter = Meter("%s.calls" % name)
        self.histogram = Histogram("%s.histo" % name)

    def clear(self):
        self.histogram.clear()

    def update(self, value):
        self.meter.mark()
        self.histogram.update(value)

    def metrics(self):
        return self.meter.metrics() + self.histogram.metrics()
