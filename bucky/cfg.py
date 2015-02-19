
debug = False
log_level = "INFO"
nice = None
uid = None
gid = None
directory = "/var/lib/bucky"
process_join_timeout = 2

sentry_enabled = False
sentry_dsn = None
sentry_log_level = "WARNING"
sentry_auto_log_stacks = False

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
collectd_counter_eq_derive = False
collectd_workers = 1

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
statsd_prefix_set = "sets"
statsd_prefix_gauge = "gauges"
statsd_persistent_gauges = False
statsd_gauges_savefile = "gauges.save"
statsd_delete_idlestats = False
# the following settings are only relevant if `statsd_delete_idlestats` is `True`
statsd_delete_counters = True
statsd_delete_timers = True
statsd_delete_sets = True
# statsd_delete_gauges = False
# `statsd_delete_gauges = True` would make gauges in practice useless,
# except if you get an absolute(!) value every flush-interval which would makes this setting irrelevant
statsd_onlychanged_gauges = True
# send gauge value to graphite only if there was a change


graphite_ip = "127.0.0.1"
graphite_port = 2003
graphite_max_reconnects = 60
graphite_reconnect_delay = 1
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


def ensure_value(attr, value):
    _vars = globals()
    if _vars.get(attr, None) is None:
        _vars[attr] = value
    return _vars[attr]
