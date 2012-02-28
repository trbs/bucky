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

"""

    2012 Karsten McMinn - convert graphite to a threaded client
    adding mysql and memcache clients, fixed configuration bugs
    among other bits

"""

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
import bucky.bmysql as bmysql
import bucky.bmemcache as bmemcache


log = logging.getLogger(__name__)


__usage__ = "%prog [CONFIG_FILE] [OPTIONS]"
__version__ = "bucky %s" % bucky.__version__


def options():
    """
    sets optparse command line options
    """
    return [
        op.make_option("--debug", dest="debug",
            action="store_true",
            help="Put server into debug mode."
        ),
        op.make_option("--metricsd-ip", dest="metricsd_ip", metavar="IP",
            help="IP address to bind for the MetricsD UDP socket"
        ),
        op.make_option("--metricsd-port", dest="metricsd_port", metavar="INT",
            type="int", help="Port to bind for the MetricsD UDP socket"
        ),
        op.make_option("--disable-metricsd", dest="metricsd_enabled",
            action="store_false", help="Disable the MetricsD UDP server"
        ),
        op.make_option("--collectd-ip", dest="collectd_ip", metavar="IP",
            help="IP address to bind for the CollectD UDP socket"
        ),
        op.make_option("--collectd-port", dest="collectd_port", metavar="INT",
            type='int', help="Port to bind for the CollectD UDP socket"
        ),
        op.make_option("--collectd-types", dest="collectd_types",
            metavar="FILE", action='append',
            help="Path to the collectd types.db file, \
                can be specified multiple times"
        ),
        op.make_option("--disable-collectd", dest="collectd_enabled",
            action="store_false",
            help="Disable the CollectD UDP server"
        ),
        op.make_option("--statsd-ip", dest="statsd_ip", metavar="IP",
            help="IP address to bind for the StatsD UDP socket"
        ),
        op.make_option("--statsd-port", dest="statsd_port", metavar="INT",
            type="int", help="Port to bind for the StatsD UDP socket"
        ),
        op.make_option("--disable-statsd", dest="statsd_enabled",
            action="store_false", help="Disable the StatsD server"
        ),
        op.make_option("--graphite-ip", dest="graphite_ip", metavar="IP",
            help="IP address of the Graphite/Carbon server"
        ),
        op.make_option("--graphite-port", dest="graphite_port", metavar="INT",
            type="int", help="Port of the Graphite/Carbon server"
        ),
        op.make_option("--disable-graphite", dest="graphite_enabled",
            action="store_false", help="Disable sending stats to Graphite"
        ),
        op.make_option("--disable-mysql", dest="mysql_enabled",
            action="store_false", help="Disable sending stats to MySQL"
        ),
        op.make_option("--mysql-ip", dest="mysql_ip", metavar="IP",
            help="IP/Hostname of the MySQL Server"
        ),
        op.make_option("--mysql-port", dest="mysql_port", metavar="INT",
            help="Port of the MySQL server" 
        ),
        op.make_option("--mysql-db", dest="mysql_db",
            help="Database Name of the MySQL Server"
        ),
        op.make_option("--mysql-user", dest="mysql_user",
            help="Username for the MySQL Database"
        ),
        op.make_option("--mysql-password", dest="mysql_pass",
            help="Password for the MySQL Database"
        ),
        op.make_option("--mysql-query", dest="mysql_query",
            help="query to use for mysql client"
        ),
        op.make_option("--disable-memcache", dest="memcache_enabled",
            action="store_false", help="Disable Sending Stats to Memcache"
        ),
        op.make_option("--memcache-ip", dest="memcache_ip", metavar="IP",
            help="IP/Hostname of the Memcache Server to send stats to"
        ),
        op.make_option("--memcache-port", dest="memcache_port", metavar="INT",
            help="Port of the Memcache server"
        ),
        op.make_option("--full-trace", dest="full_trace",
            action="store_true", help="Display full error \
            if config file fails to load"
        ),
        op.make_option("--client-threads", dest="client_threads",
            help="Number of threads per client to use"
        ),
        op.make_option("--log-level", dest="log_level",
            metavar="NAME", help="Logging output verbosity"
        ),
    ]


def main():
    """
    parse the config, start the queues, servers, client threads
    and start main daemon loop
    """

    """ parse config and options, merge all to cfg """
    parser = op.OptionParser(
        usage=__usage__,
        version=__version__,
        option_list=options()
    )
    opts, args = parser.parse_args()

    if args:
        try:
            cfgfile, = args
        except ValueError:
            parser.print_help()
            parser.error("Too many arguments.")
    else:
        print "Error: no config file specified"
        parser.print_help()
        sys.exit(1)
 

    try:
        vars = load_config(cfgfile, full_trace=cfg.full_trace)
    except:
        print "Error parsing config file: " + str(cfgfile)
        parser.print_help()
        sys.exit(1)

    """ merge commandline options to cfg """
    for attr, value in opts.__dict__.iteritems():
        if attr in vars and value is not None:
            print "setting: "+str(attr)
            setattr(cfg, attr, value)

    configure_logging()

    """ Queue for servers """
    sampleq = Queue.Queue(maxsize=5000)

    """ why do I need so many lists? """
    stypes = []
    ccarbon = []
    cmemcache = []
    cmysql = []
    queues = []
    clients = []

    if cfg.metricsd_enabled:
        stypes.append(metricsd.MetricsDServer)
    if cfg.collectd_enabled:
        stypes.append(collectd.CollectDServer)
    if cfg.statsd_enabled:
        stypes.append(statsd.StatsDServer)

    if cfg.client_threads:
        cores = int(cfg.client_threads)

    if cfg.mysql_enabled:
        queues.append(Queue.Queue(maxsize=2000))
        for i in range(0, cores):
            cmysql.append(bmysql.MysqlClient)
        for client in cmysql:
            clients.append(client(queues[-1], cfg))
            clients[-1].start()

    if cfg.memcache_enabled:
        queues.append(Queue.Queue(maxsize=2000))
        for i in range(0, cores):
            cmemcache.append(bmemcache.MemcacheClient)
        for client in cmemcache:
            clients.append(client(queues[-1], cfg))
            clients[-1].start()

    if cfg.graphite_enabled:
        queues.append(Queue.Queue(maxsize=2000))
        for i in range(0, cores):
            ccarbon.append(carbon.CarbonClient)
        for client in ccarbon:
            clients.append(client(queues[-1], cfg))
            clients[-1].start()

    servers = []
    for stype in stypes:
        servers.append(stype(sampleq, cfg))
        servers[-1].start()

    while True:
        try:
            stat, value, time = sampleq.get(True, 1)
            for q in queues:
                q.put((stat, value, time), True, 1)
        except KeyboardInterrupt:
            raise
        except:
            """ Queue.Full, Queue.Empty, et.al """
            continue
        else:
            continue


def load_config(cfgfile, full_trace=False):
    """
    loads a configfile and evals data in it
    """
    cfg_mapping = vars(cfg)
    cfg_vars = []
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
        else:
            cfg_vars.append(name)
        if name in cfg_mapping:
            setattr(cfg, name, cfg_mapping[name])
    return cfg_vars


def configure_logging():
    """
    configures logging verbosity based on
    level specified in config file
    """
    levels = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL
    }
    logfmt = "[%(levelname)s] %(module)s - %(message)s"
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(logfmt))
    handler.setLevel(logging.ERROR)
    logging.root.addHandler(handler)
    logging.root.setLevel(logging.DEBUG)
    if cfg.debug:
        cfg.log_level = "debug"
    handler.setLevel(levels.get(cfg.log_level.lower(), logging.INFO))


if __name__ == '__main__':
    main()
