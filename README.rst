Bucky
-----

Bucky is a small server for collecting and translating metrics.
It can current collect metric data from CollectD, StatsD and MetricsD
and push those metrics to a client: memcache, mysql or carbon-Graphite.

Installation
------------

You can install with `easy_install` or `pip` as per normal modus
operandi::

    $ easy_install bucky
    # or
    $ pip install bucky

After installing, you can run Bucky like::

    $ bucky /etc/your_config_file.conf

Bucky defines defaults in cfg.py but requires a config file. Copy
over one of the sample config files and edit it to enable the correct
bucky servers and clients for your installation and start bucky.

Bucky needs at least one server and one client to become useful.
The most typical usage is using collectd as the server and graphite
as the client, but you can run more servers and clients as well.

Running Bucky For Real
----------------------

The astute observer will notice that Bucky has no flags for
daemonization. This is quite on purpose. The recommended way to
run Bucky in production is via runit. There's an example service
directory in Bucky's source repository.

Alternately there is also an example init.d script in contrib/ 
for usage through regular init.


Bucky Performance
-----------------
The bucky servers all read packets in one thread, while the clients
you can configure to use as many threads as you want. By default
it will try to use one thread per cpu core for bucky clients. You
can increase the amount of threads to try to gain more performance.

In testing on 2-core servers bucky was able to handle around
1000 metrics/sec via collectd and sending via all three clients IO
permitting. Your mileage may vary based on network and disk capabilities.


Command Line Options
--------------------

The command line options are limited to controlling the network
parameters. If you want to configure some of the more intricate
workings you'll need to use a config file. Here's the `bucky -h`
output::

    Usage: bucky [CONFIG_FILE] [OPTIONS]

    Options:
      --debug               Put server into debug mode.
      --metricsd-ip=IP      IP address to bind for the MetricsD UDP socket
      --metricsd-port=INT   Port to bind for the MetricsD UDP socket
      --disable-metricsd    Disable the MetricsD UDP server
      --collectd-ip=IP      IP address to bind for the CollectD UDP socket
      --collectd-port=INT   Port to bind for the CollectD UDP socket
      --collectd-types=FILE
                            Path to the collectd types.db file,
                            can be specified multiple times
      --disable-collectd    Disable the CollectD UDP server
      --statsd-ip=IP        IP address to bind for the StatsD UDP socket
      --statsd-port=INT     Port to bind for the StatsD UDP socket
      --disable-statsd      Disable the StatsD server
      --graphite-ip=IP      IP address of the Graphite/Carbon server
      --graphite-port=INT   Port of the Graphite/Carbon server
      --disable-graphite    Disable sending stats to Graphite
      --disable-mysql       Disable sending stats to MySQL
      --mysql-ip=IP         IP/Hostname of the MySQL Server
      --mysql-port=INT      Port of the MySQL server
      --mysql-db=MYSQL_DB   Database Name of the MySQL Server
      --mysql-user=MYSQL_USER
                            Username for the MySQL Database
      --mysql-password=MYSQL_PASS
                            Password for the MySQL Database
      --mysql-query=MYSQL_QUERY
                            query to use for mysql client
      --disable-memcache    Disable Sending Stats to Memcache
      --memcache-ip=IP      IP/Hostname of the Memcache Server to send stats to
      --memcache-port=INT   Port of the Memcache server
      --full-trace          Display full error             if config file fails to
                            load
      --client-threads=CLIENT_THREADS
                            Number of threads per client to use
      --log-level=NAME      Logging output verbosity
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

    # Basic statsd configuration
    statsd_ip = "127.0.0.1"
    statsd_port = 8125
    statsd_enabled = True
    
    # How often stats should be flushed to Graphite.
    statsd_flush_time = 10.0

    # Basic Graphite Client configuration
    graphite_enabled = True
    graphite_ip = "127.0.0.1"
    graphite_port = 2003
    
    # If the Graphite connection fails these numbers define how it
    # will reconnect. The max reconnects applies each time a
    # disconnect is encountered and the reconnect delay is the time
    # in seconds between connection attempts. Setting max reconnects
    # to a negative number removes the limit.
    graphite_max_reconnects = 3
    graphite_reconnect_delay = 5


    # Basic Mysql Client Configuration
    # mysql client used to push metrics into db. it only pushes metric
    # names and not values. this is easily changed in the code however for
    # the daring.
    mysql_enabled = True
    mysql_ip = "127.0.0.1"
    mysql_port = 3306
    mysql_db = "metrics"
    mysql_user = "USERNAME"
    mysql_pass = "PASSWORD"
    mysql_query = "INSERT IGNORE INTO TABLENAME VALUES('%s', '0', '0', '0', '0');"

    # Memcache Client
    # memcache send stats to memcache, appending '.v' and '.t' to key names
    # representing value and timestamp respectively
    # multipel servers can be entered in the memcache_ip list such as
    # memcache_ip = ["200.200.200.200:11211", "100.100.100.100:11211"]
    memcache_enabled = True
    memcache_ip = ["127.0.0.1:11211"]

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


Configuring a CollectD Server
--------------------

You should only need to add something like this to your collectd.conf::

    LoadPlugin "network"
    
    <Plugin "network">
      Server "127.0.0.1" "25826"
    </Plugin>

Obviously, you'll want to match up the IP addresses and ports and make
sure that your firewall's are configured to allow UDP packets through.


Configuring a StatsD Server
------------------

Just point your StatsD clients at Bucky's IP/Port and you should be
good to go.


Configuring a MetricsD Server
------------------

TODO


Configuring a Bucky Client
--------------------------

After configuring one or more bucky servers enable a client, (graphite,
memcache or mysql) to begin sending stats somewhere.


Configuring a Graphite Client
-----------------------------

set "graphite_enabled = True" and configure the options
to send to the correct ip and port of your carbon line
port, typically tcp port 2003.


Configuring a Memcache Client
-----------------------------

set "memcache_enabled = True" in your config and specify as
many hosts as you want in the config along with their port,
typically 11211:

memcache_ip = ["10.202.142.175:11211", "10.40.75.126:11211"]


Configuring a MySQL Client
-------------------------

The mysql client requires you to specify a query of your own
based on a schema of a table of your choosing. Once setup
the '%s' in the query will become the name of the metric in
your query. The behind development of the mysql client 
is to have a index of metric keys available for easy querying.

Set "mysql_enabled = True" and configure the releveant options.
Some query examples include:

mysql_query = "INSERT IGNORE INTO table VALUES('%s', NOW());"
mysql_query = "INSERT INTO table VALUES('%s', '0', '0', '0', '0') \
		ON DUPLICATE KEY UPDATE column=value;"



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
