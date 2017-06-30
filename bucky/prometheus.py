
import six
import time
import logging
import threading

try:
    import http.server as _http
except ImportError:
    import BaseHTTPServer as _http

import bucky.client as client


if six.PY3:
    xrange = range
    long = int


log = logging.getLogger(__name__)


class PrometheusClient(client.Client):
    def __init__(self, cfg, pipe):
        super(PrometheusClient, self).__init__(pipe)
        self.port = cfg.prometheus_port
        self.timeout = cfg.prometheus_timeout
        self.path = cfg.prometheus_path
        self.flush_timestamp = time.time()
        self.buffer = {}

    def run(self):
        def do_GET(req):
            if req.path.strip('/') != self.path:
                req.send_response(404)
                req.send_header("Content-type", "text/plain")
                req.end_headers()
            else:
                req.send_response(200)
                req.send_header("Content-Type", "text/plain; version=0.0.4")
                req.end_headers()
                response = ''.join(self.get_or_render_line(k) for k in self.buffer.keys())
                req.wfile.write(response.encode())

        handler = type('PrometheusHandler', (_http.BaseHTTPRequestHandler,), {'do_GET': do_GET})
        server = _http.HTTPServer(('0.0.0.0', self.port), handler)
        threading.Thread(target=lambda: server.serve_forever()).start()
        super(PrometheusClient, self).run()

    def close(self):
        pass

    def get_or_render_line(self, k):
        timestamp, value, line = self.buffer[k]
        if not line:
            # https://prometheus.io/docs/instrumenting/exposition_formats/
            name, metadata = k[0], k[1:]
            metadata_str = ','.join(str(k) + '="' + str(v) + '"' for k, v in metadata)
            # Lines MUST end with \n (not \r\n), the last line MUST also end with \n
            # Otherwise, Prometheus will reject the whole scrape!
            line = name + '{' + metadata_str + '} ' + str(value) + ' ' + str(long(timestamp) * 1000) + '\n'
            self.buffer[k] = timestamp, value, line
        return line

    def tick(self):
        now = time.time()
        if (now - self.flush_timestamp) > 10:
            keys_to_remove = []
            for k in self.buffer.keys():
                timestamp, value, line = self.buffer[k]
                if (now - timestamp) > self.timeout:
                    keys_to_remove.append(k)
            for k in keys_to_remove:
                del self.buffer[k]
            self.flush_timestamp = now

    def _send(self, name, value, mtime, value_name, metadata=None):
        metadata_dict = dict(value=value_name)
        if metadata:
            metadata_dict.update(metadata)
        metadata_tuple = (name,) + tuple((k, metadata_dict[k]) for k in sorted(metadata_dict.keys()))
        self.buffer[metadata_tuple] = mtime, value, None
        self.tick()

    def send(self, host, name, value, mtime, metadata=None):
        self._send(name, value, mtime, 'value', metadata)

    def send_bulk(self, host, name, value, mtime, metadata=None):
        for k in value.keys():
            self._send(name, value[k], mtime, k, metadata)
