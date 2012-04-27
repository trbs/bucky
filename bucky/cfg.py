
debug = False
log_level = "INFO"

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

statsd_ip = "127.0.0.1"
statsd_port = 8125
statsd_enabled = True
statsd_flush_time = 10.0

graphite_ip = "127.0.0.1"
graphite_port = 2003
graphite_max_reconnects = 3
graphite_reconnect_delay = 5

full_trace = False

name_prefix = None
name_postfix = None
name_replace_char = '_'
name_strip_duplicates = True
name_host_trim = []

custom_clients = []
