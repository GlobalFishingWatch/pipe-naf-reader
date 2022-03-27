#!/usr/bin/env python

"""
Setup script for pipe-naf-reader
"""

from setuptools import find_packages
from setuptools import setup

package = __import__('pipe_naf_reader')

setup(
    author=package.__author__,
    author_email=package.__email__,
    description=package.__doc__.strip(),
    include_package_data=True,
    license="Apache 2.0",
    name='pipe-naf-reader',
    packages=find_packages(exclude=['test*.*', 'tests']),
    url=package.__source__,
    version=package.__version__,
    zip_safe=True
)

