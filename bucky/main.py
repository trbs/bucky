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

import multiprocessing

import os
import six
import sys
import pwd
import grp
import signal
import logging
import optparse as op

try:
    import queue
except ImportError:
    import Queue as queue

import bucky
import bucky.cfg as cfg
import bucky.carbon as carbon
import bucky.collectd as collectd
import bucky.metricsd as metricsd
import bucky.statsd as statsd
import bucky.processor as processor
from bucky.errors import BuckyError


log = logging.getLogger(__name__)
levels = {
    'CRITICAL': logging.CRITICAL,
    'ERROR': logging.ERROR,
    'WARNING': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
}

__usage__ = "%prog [OPTIONS] [CONFIG_FILE]"
__version__ = "bucky %s" % bucky.__version__


def options():
    return [
        op.make_option(
            "--debug", dest="debug", default=False,
            action="store_true",
            help="Put server into debug mode. [%default]"
        ),
        op.make_option(
            "--metricsd-ip", dest="metricsd_ip", metavar="IP",
            default=cfg.metricsd_ip,
            help="IP address to bind for the MetricsD UDP socket [%default]"
        ),
        op.make_option(
            "--metricsd-port", dest="metricsd_port", metavar="INT",
            type="int", default=cfg.metricsd_port,
            help="Port to bind for the MetricsD UDP socket [%default]"
        ),
        op.make_option(
            "--disable-metricsd", dest="metricsd_enabled",
            default=cfg.metricsd_enabled, action="store_false",
            help="Disable the MetricsD UDP server"
        ),
        op.make_option(
            "--collectd-ip", dest="collectd_ip", metavar="IP",
            default=cfg.collectd_ip,
            help="IP address to bind for the CollectD UDP socket [%default]"
        ),
        op.make_option(
            "--collectd-port", dest="collectd_port", metavar="INT",
            type='int', default=cfg.collectd_port,
            help="Port to bind for the CollectD UDP socket [%default]"
        ),
        op.make_option(
            "--collectd-types", dest="collectd_types",
            metavar="FILE", action='append', default=cfg.collectd_types,
            help="Path to the collectd types.db file, can be specified multiple times"
        ),
        op.make_option(
            "--disable-collectd", dest="collectd_enabled",
            default=cfg.collectd_enabled, action="store_false",
            help="Disable the CollectD UDP server"
        ),
        op.make_option(
            "--statsd-ip", dest="statsd_ip", metavar="IP",
            default=cfg.statsd_ip,
            help="IP address to bind for the StatsD UDP socket [%default]"
        ),
        op.make_option(
            "--statsd-port", dest="statsd_port", metavar="INT",
            type="int", default=cfg.statsd_port,
            help="Port to bind for the StatsD UDP socket [%default]"
        ),
        op.make_option(
            "--disable-statsd", dest="statsd_enabled",
            default=cfg.statsd_enabled, action="store_false",
            help="Disable the StatsD server"
        ),
        op.make_option(
            "--graphite-ip", dest="graphite_ip", metavar="IP",
            default=cfg.graphite_ip,
            help="IP address of the Graphite/Carbon server [%default]"
        ),
        op.make_option(
            "--graphite-port", dest="graphite_port", metavar="INT",
            type="int", default=cfg.graphite_port,
            help="Port of the Graphite/Carbon server [%default]"
        ),
        op.make_option(
            "--full-trace", dest="full_trace",
            default=cfg.full_trace, action="store_true",
            help="Display full error if config file fails to load"
        ),
        op.make_option(
            "--log-level", dest="log_level",
            metavar="NAME", default="INFO",
            help="Logging output verbosity [%default]"
        ),
        op.make_option(
            "--nice", dest="nice",
            type="int", default=cfg.nice,
            help="Change default process priority"
        ),
        op.make_option(
            "--uid", dest="uid",
            type="str", default=cfg.uid,
            help="Drop privileges to this user"
        ),
        op.make_option(
            "--gid", dest="gid",
            type="str", default=cfg.gid,
            help="Drop privileges to this group"
        ),
    ]


def set_nice_level(priority):
    os.nice(priority)


def drop_privileges(user, group):
    if user is None:
        uid = os.getuid()
    elif user.lstrip("-").isdigit():
        uid = int(user)
    else:
        uid = pwd.getpwnam(user).pw_uid

    if group is None:
        gid = os.getgid()
    elif group.lstrip("-").isdigit():
        gid = int(group)
    else:
        gid = grp.getgrnam(group).gr_gid

    username = pwd.getpwuid(uid).pw_name
    # groupname = grp.getgrgid(gid).gr_name
    groups = [g for g in grp.getgrall() if username in g.gr_mem]

    os.setgroups(groups)
    if hasattr(os, 'setresgid'):
        os.setresgid(gid, gid, gid)
    else:
        os.setregid(gid, gid)
    if hasattr(os, 'setresuid'):
        os.setresuid(uid, uid, uid)
    else:
        os.setreuid(uid, uid)


def main():
    parser = op.OptionParser(
        usage=__usage__,
        version=__version__,
        option_list=options()
    )
    opts, args = parser.parse_args()

    # Logging have to be configured before load_config,
    # where it can (and should) be already used
    logfmt = "[%(asctime)-15s][%(levelname)s] %(module)s - %(message)s"
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(logfmt))
    handler.setLevel(logging.ERROR)  # Overridden by configuration
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
        cfg.log_level = logging.DEBUG

    # Mandatory second commandline
    # processing pass to override values in cfg
    parser.parse_args(values=cfg)

    lvl = levels.get(cfg.log_level, cfg.log_level)
    handler.setLevel(lvl)

    if cfg.nice:
        set_nice_level(cfg.nice)

    if cfg.uid or cfg.gid:
        drop_privileges(cfg.uid, cfg.gid)

    if cfg.directory and not os.path.isdir(cfg.directory):
        try:
            os.makedirs(cfg.directory)
        except:
            log.exception("Could not create directory: %s" % cfg.directory)

    bucky = Bucky(cfg)
    bucky.run()


class Bucky(object):
    def __init__(self, cfg):
        self.sampleq = multiprocessing.Queue()

        stypes = []
        if cfg.metricsd_enabled:
            stypes.append(metricsd.MetricsDServer)
        if cfg.collectd_enabled:
            stypes.append(collectd.CollectDServer)
        if cfg.statsd_enabled:
            stypes.append(statsd.StatsDServer)

        self.servers = []
        for stype in stypes:
            self.servers.append(stype(self.sampleq, cfg))

        if cfg.processor is not None:
            self.psampleq = multiprocessing.Queue()
            self.proc = processor.CustomProcessor(self.sampleq, self.psampleq,
                                                  cfg)
        else:
            self.proc = None
            self.psampleq = self.sampleq

        if cfg.graphite_pickle_enabled:
            carbon_client = carbon.PickleClient
        else:
            carbon_client = carbon.PlaintextClient

        self.clients = []
        for client in cfg.custom_clients + [carbon_client]:
            send, recv = multiprocessing.Pipe()
            instance = client(cfg, recv)
            self.clients.append((instance, send))

    def run(self):
        def sigterm_handler(signum, frame):
            log.info("Received SIGTERM")
            self.psampleq.put(None)

        for server in self.servers:
            server.start()
        if self.proc is not None:
            self.proc.start()
        for client, pipe in self.clients:
            client.start()

        signal.signal(signal.SIGTERM, sigterm_handler)

        while True:
            try:
                sample = self.psampleq.get(True, 1)
                if not sample:
                    break
                for instance, pipe in self.clients:
                    if not instance.is_alive():
                        self.shutdown("Client process died. Exiting.")
                    pipe.send(sample)
            except queue.Empty:
                pass
            except IOError as exc:
                # Probably due to interrupted system call by SIGTERM
                log.debug("Bucky IOError: %s", exc)
                continue
            for srv in self.servers:
                if not srv.is_alive():
                    self.shutdown("Server thread died. Exiting.")
            if self.proc is not None and not self.proc.is_alive():
                self.shutdown("Processor thread died. Exiting.")
        self.shutdown()

    def shutdown(self, err=''):
        log.info("Shutting down")
        for server in self.servers:
            log.info("Stopping server %s", server)
            server.close()
            server.join(1)
        if self.proc is not None:
            log.info("Stopping processor %s", self.proc)
            self.sampleq.put(None)
            self.proc.join(1)
        for client, pipe in self.clients:
            log.info("Stopping client %s", client)
            pipe.send(None)
            client.join(1)
        children = multiprocessing.active_children()
        for child in children:
            log.error("Child %s didn't die gracefully, terminating", child)
            child.terminate()
            child.join(1)
        if children and not err:
            err = "Not all children died gracefully"
        if err:
            raise BuckyError(err)


def load_config(cfgfile, full_trace=False):
    cfg_mapping = vars(cfg)
    try:
        if cfgfile is not None:
            if six.PY3:
                with open(cfgfile, 'rb') as file:
                    exec(compile(file.read(), cfgfile, 'exec'), cfg_mapping)
            else:
                execfile(cfgfile, cfg_mapping)  # noqa
    except Exception as e:
        log.error("Failed to read config file: %s", cfgfile)
        if full_trace:
            log.exception("Reason: %s", e)
        else:
            log.error("Reason: %s", e)
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
    except Exception:
        raise
