#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup
#from distutils.core import setup

setup(name='peafowl',
      version='0.1',
      description='A light weight server for reliable distributed message passing.',
      author='Timothee Peignier',
      author_email='tim@tryphon.org',
      url='http://pypi.python.org/pypi/peafowl/',
      packages = ['peafowl'],
      scripts=['bin/peafowl'],
      test_suite='tests'
)
