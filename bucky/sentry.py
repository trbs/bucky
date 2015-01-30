import logging
from raven import Client
from raven.handlers.logging import SentryHandler
from raven.conf import setup_logging


def sentry_setup(dsn, level=None, auto_log_stacks=False):
    if level is None:
        level = logging.WARNING
    client = Client(dsn, auto_log_stacks=auto_log_stacks)
    handler = SentryHandler(client=client, level=level)
    setup_logging(handler)
