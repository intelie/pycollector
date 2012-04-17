#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
from distutils.core import setup
from distutils.file_util import *
import py2exe

sys.path.append(os.path.join(os.path.dirname(__file__), "src"))  
#sys.path.append(os.path.join(os.path.dirname(__file__), "conf"))  

setup(
    console=['src/logcollectorsvc.py'],
    zipfile=None,
    options={
                "py2exe":{
                        "unbuffered": True,
                        "optimize": 2,
                        "bundle_files": 1
                }
        }
)
copy_file('conf/daemon_conf.py', 'dist')
copy_file('conf/pattern_conf.py', 'dist')