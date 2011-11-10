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

import logging
import optparse as op
import Queue

import bucky.carbon as carbon
import bucky.collectd as collectd
import bucky.statsd as statsd


logging.basicConfig(format="[%(levelname)s] %(message)s", level=logging.DEBUG)


__usage__ = "%prog [OPTIONS]"


def options():
    return []


def main():
    parser = op.OptionParser(usage=__usage__, option_list=options())
    opts, args = parser.parse_args()

    sampleq = Queue.Queue()

    cdsrv = collectd.CollectDServer(sampleq)
    cdsrv.start()

    stsrv = statsd.StatsDServer(sampleq)
    stsrv.start()

    cli = carbon.CarbonClient()

    while True:
        try:
            stat, value, time = sampleq.get(True, 1)
            cli.send(stat, value, time)
        except Queue.Empty:
            pass
        if not cdsrv.is_alive():
            log.error("collectd server died")
            break
        if not stsrv.is_alive():
            log.error("statsd server died")
            break


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception, e:
        raise # debug
        print e
