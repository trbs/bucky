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

import os
import sys

from setuptools import setup

from bucky import __version__

setup(
    name = 'bucky',
    version = __version__,

    description = 'CollectD and StatsD adapter for Graphite',
    long_description = file(
        os.path.join(
            os.path.dirname(__file__),
            'README.rst'
        )
    ).read(),
    author = 'Paul J. Davis',
    author_email = 'paul@cloudant.com',
    license = 'ASF2.0',
    url = 'http://github.com/cloudant/bucky.git',

    classifiers = [
        'Development Status :: 4 - Beta',
        'Environment :: Other Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Topic :: Internet :: Log Analysis',
        'Topic :: Utilities',
        'Topic :: System :: Networking :: Monitoring'
    ],
    zip_safe = False,
    packages = ['bucky', 'bucky.metrics', 'bucky.metrics.stats'],
    include_package_data = True,

    entry_points="""\
    [console_scripts]
    bucky=bucky.main:main
    """
)
