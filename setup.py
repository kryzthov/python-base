#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import shutil
import sys

from setuptools import setup


def Main():
  assert (sys.version_info[0] >= 3), \
      ('Python version >= 3 required, got %r' % sys.version_info)
  base_dir = os.path.dirname(os.path.abspath(__file__))

  setup(
    name = 'python-base',
    version = '1.0.11',
    packages = ['base', 'workflow'],
    package_dir = {
        'base': 'base',
        'workflow': 'workflow',
    },
    script_name = 'setup.py',
    scripts = [
        'scripts/daemonize.py',
    ],

    # metadata for upload to PyPI
    author = 'Christophe Taton',
    author_email = 'christophe.taton@gmail.com',
    description = 'General purpose library for Python.',
    license = 'Apache License 2.0',
    keywords = 'python base flags tools workflow',
    url = 'https://github.com/kryzthov/python-base',
  )


if __name__ == '__main__':
  Main()
