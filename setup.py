#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup
import peafowl

setup(name='peafowl',
      version=peafowl.__version__,
      description='A light weight server for reliable distributed message passing.',
      long_description="Peafowl is a powerful but simple messaging server that enables reliable distributed queuing with an absolutely minimal overhead. It speaks the MemCache protocol for maximum cross-platform compatibility. Any language that speaks MemCache can take advantage of Peafowl's queue facilities.",
      author='Timothee Peignier',
      author_email='tim@tryphon.org',
      url='http://wiki.github.com/cyberdelia/peafowl',
      license = 'BSD License',
      platforms = ["Unix",],
      keywords = "peafowl queue messaging distributed memcache starling",
      classifiers = [ "Development Status :: 4 - Beta",
                      "License :: OSI Approved :: BSD License",
                      "Operating System :: Unix",
                      "Programming Language :: Python" ],
      packages = ['peafowl'],
      scripts=['bin/peafowl'],
      test_suite='tests'
)
