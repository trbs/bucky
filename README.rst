Bucky
-----

:info: Bucky Statsd and Collectd server for Graphite

.. image:: https://travis-ci.org/trbs/bucky.png?branch=master
   :target: https://travis-ci.org/trbs/bucky

.. image:: https://coveralls.io/repos/trbs/bucky/badge.png?branch=master
   :target: https://coveralls.io/r/trbs/bucky?branch=master

Bucky is a small server for collecting and translating metrics for
Graphite. It can current collect metric data from CollectD daemons
and from StatsD clients.

Installation
------------

You can install with `easy_install` or `pip` as per normal modus
operandi::

    $ easy_install bucky
    # or
    $ pip install bucky

After installing, you can run Bucky like::

    $ bucky

Bucky will try to install PyCrypto which requires the python-dev
package to be installed.

By default, Bucky will open a CollectD UDP socket on 127.0.0.1:25826,
a StatsD socket on 127.0.0.1:8125 as well as attempt to connect to a
local Graphite (Carbon) daemon on 127.0.0.1:2003.

These are all optional as illustrated below. You can also disable the
CollectD or StatsD servers completely if you so desire.

Process Names
-------------

If the py-setproctitle_ module is installed Bucky will use it to set
user readable process names. This will make the child processes of Bucky
easier to identify. Please note that this is completely optional.

To install py-setproctitle_ run::

    $ easy_install setproctitle
    # or
    $ pip install setproctitle

.. _py-setproctitle: https://github.com/dvarrazzo/py-setproctitle


Running Bucky For Real
----------------------

The astute observer will notice that Bucky has no flags for
daemonization. This is quite on purpose. The recommended way to
run Bucky in production is via runit. There's an example service
directory in Bucky's source repository.

Python 3 Support
----------------

Bucky supports Python 3. However this support is still very young
and we would like to hear from you if you are running Bucky on
Python3 and help us improve the support in real production environments.

Command Line Options
--------------------

The command line options are limited to controlling the network
parameters. If you want to configure some of the more intricate
workings you'll need to use a config file. Here's the `bucky -h`
output::

    Usage: main.py [OPTIONS] [CONFIG_FILE]
    
    Options:
      --debug               Put server into debug mode. [False]
      --metricsd-ip=IP      IP address to bind for the MetricsD UDP socket
                            [127.0.0.1]
      --metricsd-port=INT   Port to bind for the MetricsD UDP socket [23632]
      --disable-metricsd    Disable the MetricsD UDP server
      --collectd-ip=IP      IP address to bind for the CollectD UDP socket
                            [127.0.0.1]
      --collectd-port=INT   Port to bind for the CollectD UDP socket [25826]
      --collectd-types=FILE
                            Path to the collectd types.db file, can be specified
                            multiple times
      --disable-collectd    Disable the CollectD UDP server
      --statsd-ip=IP        IP address to bind for the StatsD UDP socket
                            [127.0.0.1]
      --statsd-port=INT     Port to bind for the StatsD UDP socket [8125]
      --disable-statsd      Disable the StatsD server
      --graphite-ip=IP      IP address of the Graphite/Carbon server [127.0.0.1]
      --graphite-port=INT   Port of the Graphite/Carbon server [2003]
      --full-trace          Display full error if config file fails to load
      --log-level=NAME      Logging output verbosity [INFO]
      --version             show program's version number and exit
      -h, --help            show this help message and exit


Config File Options
-------------------

The configuration file is a normal Python file that defines a number of
variables. Most of command line options can also be specified in this
file (remove the "--" prefix and replace "-" with "_") but if specified
in both places, the command line takes priority. The defaults as a
config file::


    # Standard debug and log level
    debug = False
    log_level = "INFO"

    # Whether to print the entire stack trace for errors encountered
    # when loading the config file
    full_trace = False

    # Basic metricsd conifguration
    metricsd_ip = "127.0.0.1"
    metricsd_port = 23632
    metricsd_enabled = True
    
    # The default interval between flushes of metric data to Graphite
    metricsd_default_interval = 10.0
    
    # You can specify the frequency of flushes to Graphite based on
    # the metric name used for each metric. These are specified as
    # regular expressions. An entry in this list should be a 3-tuple
    # that is: (regexp, frequency, priority)
    #
    # The regexp is applied with the match method. Frequency should be
    # in seconds. Priority is used to break ties when a metric name
    # matches more than one handler. (The largest priority wins)
    metricsd_handlers = []

    # Basic collectd configuration
    collectd_ip = "127.0.0.1"
    collectd_port = 25826
    collectd_enabled = True
    
    # A list of file names for collectd types.db
    # files.
    collectd_types = []
    
    # A mapping of plugin names to converter callables. These are
    # explained in more detail in the README.
    collectd_converters = {}
    
    # Whether to load converters from entry points. The entry point
    # used to define converters is 'bucky.collectd.converters'.
    collectd_use_entry_points = True

    # Cryptographic settings for collectd. Security level 1 requires
    # signed packets, level 2 requires encrypted communication.
    # Auth file should contain lines in the form 'user: password'
    collectd_security_level = 0
    collectd_auth_file = None

    # Basic statsd configuration
    statsd_ip = "127.0.0.1"
    statsd_port = 8125
    statsd_enabled = True
    
    # How often stats should be flushed to Graphite.
    statsd_flush_time = 10.0

    # If the legacy namespace is enabled, the statsd backend uses the
    # default prefixes except for counters, which are stored directly
    # in stats.NAME for the rate and stats_counts.NAME for the
    # absolute count.  If legacy names are disabled, the prefixes are
    # configurable, and counters are stored under
    # stats.counters.{rate,count} by default.  Any prefix can be set
    # to None to skip it.
    statsd_legacy_namespace = True
    statsd_global_prefix = "stats"
    statsd_prefix_counter = "counters"
    statsd_prefix_timer = "timers"
    statsd_prefix_gauge = "gauges"

    # Basic Graphite configuration
    graphite_ip = "127.0.0.1"
    graphite_port = 2003
    
    # If the Graphite connection fails these numbers define how it
    # will reconnect. The max reconnects applies each time a
    # disconnect is encountered and the reconnect delay is the time
    # in seconds between connection attempts. Setting max reconnects
    # to a negative number removes the limit. The backoff factor
    # determines how much the reconnect delay will be multiplied with
    # each reconnect round. It can be limited with a maximum after which
    # the delay will not be multiplied anymore.
    graphite_max_reconnects = 3
    graphite_reconnect_delay = 5
    graphite_backoff_factor = 1.5
    graphite_backoff_max = 60

    # Configuration for sending metrics to Graphite via the pickle
    # interface. Be sure to edit graphite_port to match the settings
    # on your Graphite cache/relay.
    graphite_pickle_enabled = False
    graphite_pickle_buffer_size = 500

    # Bucky provides these settings to allow the system wide
    # configuration of how metric names are processed before
    # sending to Graphite.
    #    
    # Prefix and postfix allow to tag all values with some value.
    name_prefix = None
    name_postfix = None
    
    # The replacement character is used to munge any '.' characters
    # in name components because it is special to Graphite. Setting
    # this to None will prevent this step.
    name_replace_char = '_'
    
    # Optionally strip duplicates in path components. For instance
    # a.a.b.c.c.b would be rewritten as a.b.c.b
    name_strip_duplicates = True
    
    # Bucky reverses hostname components to improve the locality
    # of metric values in Graphite. For instance, "node.company.tld"
    # would be rewritten as "tld.company.node". This setting allows
    # for the specification of hostname components that should
    # be stripped from hostnames. For instance, if "company.tld"
    # were specified, the previous example would end up as "node".
    name_host_trim = []
    
    # processor is a callable that takes a (host, name, val, time)
    # tuple as input and is expected to return a tuple of the same
    # structure to forward the sample to the clients, or None to
    # drop it. processor_drop_on_error specifies if the sample is
    # dropped or forwarded to clients in case an exception is
    # raised by the processor callable.
    processor = None
    processor_drop_on_error = False


Configuring CollectD
--------------------

You should only need to add something like this to your collectd.conf::

    LoadPlugin "network"
    
    <Plugin "network">
      Server "127.0.0.1" "25826"
    </Plugin>

Obviously, you'll want to match up the IP addresses and ports and make
sure that your firewall's are configured to allow UDP packets through.


Configuring StatsD
------------------

Just point your StatsD clients at Bucky's IP/Port and you should be
good to go.


Configuring MetricsD
--------------------

TODO


A note on CollectD converters
-----------------------------

CollectD metrics aren't exactly directly translatable to Graphite
metric names. The default translator attempts to make a best guess
but this can result in slightly less than pretty Graphite trees.

For this reason, Bucky has configurable converters. These are
keyed off the CollectD plugin name. The input to these functions is
a representation of the CollectD metric that looks like such::

    {
      'host': 'toroid.local',
      'interval': 10.0,
      'plugin': 'memory',
      'plugin_instance': '',
      'time': 1320970329.175534,
      'type': 'memory',
      'type_instance': 'inactive',
      'value': 823009280.0,
      'value_name': 'value',
      'value_type': 1
    }

The result of this function should be a list of strings that represent
part of the Graphite metric name or `None` to drop sample
entirely. For instance, if a converter returned `["foo", "bar"]`, the
final metric name will end up as:
`$prefix.$hostname.foo.bar.$postfix`.

An example builtin converter looks like such::

    # This might be how you define a converter in
    # your config file

    class MemoryConverter(object):
        PRIORITY = 0
        def __call__(self, sample):
            return ["memory", sample["type_instance"]]

    collectd_converters = {"memory": MemoryConverter()}

Converters can either be declared and/or imported in the optional
config file, or they can be autodiscovered via entry points. The
entry point that is searched is "bucky.collectd.converters". The
entry point name should be the CollectD plugin name.

`collectd_converters` in config file should be a mapping of collectd
plugin name to converter instance. The default catch-all converter
(used when no special converter is defined for a plugin) can be
overidden by specifying `_default` as the plugin name.

Converters also have a notion of priority in order to resolve
conflicts. This is merely a property on the callable named
"PRIORITY" and larger priorities are preferred. I don't imagine
this will need to be used very often, but its there just in
case.


Configuring the Processor
-------------------------

A Processor is a process that recieves samples as they are parsed
by the servers and performs actions on them before handing them
over to the clients.

If a callable is defined in the `processor` configuration variable,
a Processor process will aply this callable to the sample recieved
`(host, name, val, time)` and expects back a tuple of the same
structure to forward to clients, or `None` to drop the sample.

This makes it easy to add all sorts of custom filtering and
modification on samples.

This might be how you define a processor in your config file::

    import time

    def timediff(host, name, val, timestamp):
        """Drop samples with large time offset
        
        Drop samples that are more than 2 minutes in the future
        or more than 5 minutes in the past.

        """

        future = 120  # 2 minutes
        past = 300  # 5 minutes
        now = time.time()
        if timestamp > now + future or timestamp < now - past:
            return None
        return host, name, val, timestamp

    processor = timediff

