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
import os
import Queue
import sys

import bucky
import bucky.carbon as carbon
import bucky.collectd as collectd
import bucky.statsd as statsd


log = logging.getLogger(__name__)
logfmt = "[%(levelname)s] %(module)s - %(message)s"
logging.basicConfig(format=logfmt, level=logging.DEBUG)


__usage__ = "%prog [OPTIONS] [CONFIG_FILE]"
__version__ = "bucky %s" % bucky.__version__

def options():
    return [
        op.make_option("--collectd-ip", dest="collectd_ip", metavar="IP",
            default="127.0.0.1",
            help="IP address to bind for the CollectD UDP socket [%default]"
        ),
        op.make_option("--collectd-port", dest="collectd_port", metavar="INT",
            type='int', default=25826,
            help="Port to bind for the CollectD UDP socket [%default]"
        ),
        op.make_option("--collectd-types", dest="collectd_types",
            metavar="FILE", default=None,
            help="Path to the collectd types.db file"
        ),
        op.make_option("--disable-collectd", dest="collectd_enabled",
            default=True, action="store_false",
            help="Disable the CollectD UDP server"
        ),
        op.make_option("--statsd-ip", dest="statsd_ip", metavar="IP",
            default="127.0.0.1",
            help="IP address to bind for the StatsD UDP socket [%default]"
        ),
        op.make_option("--statsd-port", dest="statsd_port", metavar="INT",
            type="int", default=8125,
            help="Port to bind for the StatsD UDP socket [%default]"
        ),
        op.make_option("--disable-statsd", dest="statsd_enabled",
            default=True, action="store_false",
            help="Disable the StatsD server"
        ),
        op.make_option("--graphite-ip", dest="graphite_ip", metavar="IP",
            default="127.0.0.1",
            help="IP address of the Graphite/Carbon server [%default]"
        ),
        op.make_option("--graphite-port", dest="graphite_port", metavar="INT",
            type="int", default=2003,
            help="Port of the Graphite/Carbon server [%default]"
        ),
        op.make_option("--full-trace", dest="full_trace",
            default=False, action="store_true",
            help="Display full error if config file fails to load"
        ),
    ]


def main():
    parser = op.OptionParser(usage=__usage__, version=__version__,
                                option_list=options())
    opts, args = parser.parse_args()

    if len(args) > 1:
        parser.error("Too many arguments.")
    if len(args) == 1 and not os.path.isfile(args[0]):
        parser.error("Invalid config file: %s" % opts.config)

    cfgfile = None
    if len(args) == 1:
        cfgfile = args[0]
    cfg = load_config(opts, cfgfile)

    sampleq = Queue.Queue()

    if opts.collectd_enabled:
        cdsrv = collectd.CollectDServer(sampleq, cfg)
        cdsrv.start()

    if opts.statsd_enabled:
        stsrv = statsd.StatsDServer(sampleq, cfg)
        stsrv.start()

    cli = carbon.CarbonClient(cfg)

    while True:
        try:
            stat, value, time = sampleq.get(True, 1)
            cli.send(stat, value, time)
        except Queue.Empty:
            pass
        if opts.collectd_enabled and not cdsrv.is_alive():
            log.error("collectd server died")
            break
        if opts.statsd_enabled and not stsrv.is_alive():
            log.error("statsd server died")
            break


def load_config(opts, cfgfile=None):
    cfg = {
        "__builtins__": __builtins__,
        "__name__": "__config__",
        "__file__": cfgfile,
        "__doc__": None,
        "__package__": None
    }
    param_names = """
        collectd_ip collectd_port collectd_types
        statsd_ip statsd_port
        graphite_ip graphite_port
    """.split()
    for name in param_names:
        cfg[name] = getattr(opts, name)
    cfg["collectd_converters"] = []
    cfg["collectd_use_entry_points"] = True
    cfg["statsd_flush_time"] = 10
    try:
        if cfgfile is not None:
            execfile(cfgfile, cfg, cfg)
    except Exception, e:
        log.error("Failed to read config file: %s" % cfgfile)
        log.error("Reason: %s" % e)
        if opts.full_trace:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    return cfg


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception, e:
        print e
