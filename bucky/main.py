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
import bucky.cfg as cfg
import bucky.carbon as carbon
import bucky.collectd as collectd
import bucky.metricsd as metricsd
import bucky.statsd as statsd


log = logging.getLogger(__name__)


__usage__ = "%prog [OPTIONS] [CONFIG_FILE]"
__version__ = "bucky %s" % bucky.__version__

def options():
    return [
        op.make_option("--debug", dest="debug", default=False,
            action="store_true",
            help="Put server into debug mode. [%default]"
        ),
        op.make_option("--metricsd-ip", dest="metricsd_ip", metavar="IP",
            default=cfg.metricsd_ip,
            help="IP address to bind for the MetricsD UDP socket [%default]"
        ),
        op.make_option("--metricsd-port", dest="metricsd_port", metavar="INT",
            type="int", default=cfg.metricsd_port,
            help="Port to bind for the MetricsD UDP socket [%default]"
        ),
        op.make_option("--disable-metricsd", dest="metricsd_enabled",
            default=cfg.metricsd_enabled, action="store_false",
            help="Disable the MetricsD UDP server"
        ),
        op.make_option("--collectd-ip", dest="collectd_ip", metavar="IP",
            default=cfg.collectd_ip,
            help="IP address to bind for the CollectD UDP socket [%default]"
        ),
        op.make_option("--collectd-port", dest="collectd_port", metavar="INT",
            type='int', default=cfg.collectd_port,
            help="Port to bind for the CollectD UDP socket [%default]"
        ),
        op.make_option("--collectd-types", dest="collectd_types",
            metavar="FILE", action='append', default=cfg.collectd_types,
            help="Path to the collectd types.db file, can be specified multiple times"
        ),
        op.make_option("--disable-collectd", dest="collectd_enabled",
            default=cfg.collectd_enabled, action="store_false",
            help="Disable the CollectD UDP server"
        ),
        op.make_option("--statsd-ip", dest="statsd_ip", metavar="IP",
            default=cfg.statsd_ip,
            help="IP address to bind for the StatsD UDP socket [%default]"
        ),
        op.make_option("--statsd-port", dest="statsd_port", metavar="INT",
            type="int", default=cfg.statsd_port,
            help="Port to bind for the StatsD UDP socket [%default]"
        ),
        op.make_option("--disable-statsd", dest="statsd_enabled",
            default=cfg.statsd_enabled, action="store_false",
            help="Disable the StatsD server"
        ),
        op.make_option("--graphite-ip", dest="graphite_ip", metavar="IP",
            default=cfg.graphite_ip,
            help="IP address of the Graphite/Carbon server [%default]"
        ),
        op.make_option("--graphite-port", dest="graphite_port", metavar="INT",
            type="int", default=cfg.graphite_port,
            help="Port of the Graphite/Carbon server [%default]"
        ),
        op.make_option("--full-trace", dest="full_trace",
            default=cfg.full_trace, action="store_true",
            help="Display full error if config file fails to load"
        ),
        op.make_option("--log-level", dest="log_level",
            metavar="NAME", default="INFO",
            help="Logging output verbosity [%default]"
        ),
    ]


def main():
    parser = op.OptionParser(
        usage=__usage__,
        version=__version__,
        option_list=options()
    )
    opts, args = parser.parse_args()

    # Logging have to be configured before load_config,
    # where it can (and should) be already used
    logfmt = "[%(levelname)s] %(module)s - %(message)s"
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(logfmt))
    handler.setLevel(logging.ERROR) # Overridden by configuration
    logging.root.addHandler(handler)
    logging.root.setLevel(logging.DEBUG)

    if args:
        try:
            cfgfile, = args
        except ValueError:
            parser.error("Too many arguments.")
    else:
        cfgfile = None
    load_config(cfgfile, full_trace=opts.full_trace)

    if cfg.debug:
        cfg.log_level = "DEBUG"

    # Mandatory second commandline
    # processing pass to override values in cfg
    parser.parse_args(values=cfg)

    handler.setLevel(cfg.log_level)

    sampleq = Queue.Queue()

    stypes = []
    if cfg.metricsd_enabled:
        stypes.append(metricsd.MetricsDServer)
    if cfg.collectd_enabled:
        stypes.append(collectd.CollectDServer)
    if cfg.statsd_enabled:
        stypes.append(statsd.StatsDServer)

    servers = []
    for stype in stypes:
        servers.append(stype(sampleq, cfg))
        servers[-1].start()

    clients = [cli(cfg) for cli in cfg.custom_clients + [carbon.CarbonClient]]

    while True:
        try:
            host, name, value, time = sampleq.get(True, 1)
            for cli in clients:
                cli.send(host, name, value, time)
        except Queue.Empty:
            pass
        for srv in servers:
            if not srv.is_alive():
                log.error("Server thread died. Exiting.")
                break


def load_config(cfgfile, full_trace=False):
    cfg_mapping = vars(cfg)
    try:
        if cfgfile is not None:
            execfile(cfgfile, cfg_mapping)
    except Exception, e:
        log.error("Failed to read config file: %s" % cfgfile)
        if full_trace:
            log.exception("Reason: %s" % e)
        else:
            log.error("Reason: %s" % e)
        sys.exit(1)
    for name in dir(cfg):
        if name.startswith("_"):
            continue
        if name in cfg_mapping:
            setattr(cfg, name, cfg_mapping[name])


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception, e:
        raise
        print e
