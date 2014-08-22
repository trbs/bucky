import time

import t
import os
import bucky.helpers


def test_file_monitor():
    with t.unlinking(t.temp_file('asd')) as path:
        monitor = bucky.helpers.FileMonitor(path)
        t.eq(monitor.modified(), False)
        with open(path, 'w') as f:
            f.write('bbbb')
        time.sleep(1)
        t.eq(monitor.modified(), True)
        t.eq(monitor.modified(), False)
