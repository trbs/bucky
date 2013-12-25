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
from setuptools import setup
from bucky2 import __version__

install_requires = [e.strip() for e in open("requirements.txt") if not e.startswith("#")]

setup(
    name='bucky2',
    version=__version__,

    description='StatsD and CollectD adapter for Graphite',
    long_description=open(
        os.path.join(
            os.path.dirname(__file__),
            'README.rst'
        )
    ).read(),
    author='Trbs',
    author_email='trbs@trbs.net',
    license='ASF2.0',
    url='http://github.com/trbs/bucky2.git',
    install_requires=install_requires,

    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Other Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Programming Language :: Python',
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        'Topic :: Internet :: Log Analysis',
        'Topic :: Utilities',
        'Topic :: System :: Networking :: Monitoring'
    ],
    zip_safe=False,
    packages=['bucky2', 'bucky2.metrics', 'bucky2.metrics.stats'],
    include_package_data=True,

    entry_points="""\
    [console_scripts]
    bucky2=bucky2.main:main
    """
)
