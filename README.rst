Bucky
-----

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

By default, Bucky will open a CollectD UDP socket on 127.0.0.1:25826,
a StatsD socket on 127.0.0.1:8125 as well as attempt to connect to a
local Graphite (Carbon) daemon on 127.0.0.1:2003.

These are all optional as illustrated below. You can also disable the
CollectD or StatsD servers completely if you so desire.

Running Bucky For Real
----------------------

The astute observer will notice that Bucky has no flags for
daemonization. This is quite on purpose. The recommended way to
run Bucky in production is via runit. There's an example service
directory in Bucky's source repository.

Command Line Options
--------------------

The command line options are limited to controlling the network
parameters. If you want to configure some of the more intricate
workings you'll need to use a config file. Here's the `bucky -h`
output::

    Usage: main.py [OPTIONS] [CONFIG_FILE]

    Options:
      --collectd-ip=IP      IP address to bind for the CollectD UDP socket
                            [127.0.0.1]
      --collectd-port=INT   Port to bind for the CollectD UDP socket [25826]
      --collectd-types=FILE
                            Path to the collectd types.db file
      --disable-collectd    Disable the CollectD UDP server
      --statsd-ip=IP        IP address to bind for the StatsD UDP socket
                            [127.0.0.1]
      --statsd-port=INT     Port to bind for the StatsD UDP socket [8125]
      --disable-statsd      Disable the StatsD server
      --graphite-ip=IP      IP address of the Graphite/Carbon server [127.0.0.1]
      --graphite-port=INT   Port of the Graphite/Carbon server [2003]
      --full-trace          Display full error if config file fails to load
      --version             show program's version number and exit
      -h, --help            show this help message and exit


Config File Options
-------------------

The configuration file is a normal Python file that defines a number of
variables. Most of command line options can also be specified in this
file (remove the "--" prefix and replace "-" with "_") but if specified
in both places, the command line takes priority. The defaults as a
config file::

    # Prefix for collectd metric names
    collectd_conv_prefix = None
    
    # Postfix for collectd metric names
    collectd_conv_postfix = None
    
    # Replace periods (.) in metric names with this value
    collectd_replace = "_"
    
    # If a path has identical repeated components, collapse
    # them to a single instance. Ie, a.b.b.c becomes a.b.c
    collectd_strip_duplicates = True
    
    # These hostnames will be removed from hostnames that
    # are received. Ie, if "foo.bar.cloudant.com" comes in
    # and "cloudant.com" is listed, then the resulting
    # hostname will be "foo.bar".
    collectd_host_trim = []
    
    # CollectD metrics need to have a name generated for
    # use by Graphite. Here you can register a special
    # handler for metrics based on the CollectD plugin
    # name.
    #
    # For instance, the CollectD CPU plugin ends up with
    # metric names like "host.cpu.0.cpu.idle" to remove
    # the second instance CPU we can register a plugin
    # to generate the name.
    #
    # The dict key should be the CollectD plugin name
    # and the value should be an callable that accepts
    # a single argument and returns a list of strings.
    collectd_converters = {}
    
    # Optionally disable the system wide search for
    # converter plugins.
    # converters.
    collectd_use_entry_points = True
    
    # The number of seconds that the StatsD daemon should
    # wait before flushing values.
    statsd_flush_time = 10


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

The result of this function should be a list of strings that
represent part of the Graphite metric name. For instance, if a
converter returned `["foo", "bar"]`, the final metric name
will end up as: `$prefix.$hostname.foo.bar.$postfix`.

An example builtin converter looks like such::

    # This might be how you define a converter in
    # your config file

    class MemoryConverter(object):
        PRIORITY = 0
        def __call__(self, sample):
            return ["memory", sample["type_instance"]]

    collectd_converters = [MemoryConverter()]

Collectors also have a notion of priority in order to resolve
conflicts. This is merely a property on the callable named
"PRIORITY" and larger priorities are preferred. I don't imagine
this will need to be used very often, but its there just in
case.

Converters can either be declared and/or imported in the optional
config file, or they can be autodiscovered via entry points. The
entry point that is searched is "bucky.collectd.converters". The
entry point name should be the CollectD plugin name.
