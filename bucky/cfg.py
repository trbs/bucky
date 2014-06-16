
debug = False
log_level = "INFO"
nice = None
uid = None
gid = None
directory = "/var/lib/bucky"

metricsd_ip = "127.0.0.1"
metricsd_port = 23632
metricsd_enabled = True
metricsd_default_interval = 10.0
metricsd_handlers = []

collectd_ip = "127.0.0.1"
collectd_port = 25826
collectd_enabled = True
collectd_types = []
collectd_converters = []
collectd_use_entry_points = True

collectd_security_level = 0
collectd_auth_file = None

statsd_ip = "127.0.0.1"
statsd_port = 8125
statsd_enabled = True
statsd_flush_time = 10.0
statsd_legacy_namespace = True
statsd_global_prefix = "stats"
statsd_prefix_counter = "counters"
statsd_prefix_timer = "timers"
statsd_prefix_gauge = "gauges"
statsd_persistent_gauges = False
statsd_gauges_savefile = "gauges.save"

graphite_ip = "127.0.0.1"
graphite_port = 2003
graphite_max_reconnects = 1000
graphite_reconnect_delay = 5
graphite_backoff_factor = 1.5
graphite_backoff_max = 60
graphite_pickle_enabled = False
graphite_pickle_buffer_size = 500

full_trace = False

name_prefix = None
name_prefix_parts = None
name_postfix = None
name_postfix_parts = None
name_replace_char = '_'
name_strip_duplicates = True
name_host_trim = []

custom_clients = []

processor = None
processor_drop_on_error = False
