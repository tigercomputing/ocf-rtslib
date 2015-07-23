#!/usr/bin/python

# This file is part of ocf-rtslib.
# Copyright (C) 2015  Tiger Computing Ltd. <info@tiger-computing.co.uk>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

from distutils.util import convert_path
from setuptools import setup, find_packages

# Read the version number from nrpe_ng/version.py. This avoids needing to
# query setuptools for the version at run-time.
main_ns = {}
ver_path = convert_path('ocf_rtslib/version.py')
with open(ver_path) as ver_file:
    exec(ver_file.read(), main_ns)

setup(
    name='ocf-rtslib',
    version=main_ns['__version__'],
    description='OCF Resource Agents for the Linux SCSI Target Framework',
    author='Tiger Computing Ltd.',
    author_email='info@tiger-computing.co.uk',
    license='GPL-2+',
    url='https://github.com/tigercomputing/ocf-rtslib/',
    install_requires=[
        'python_ocf',
    ],
    dependency_links=[
        'https://github.com/Datera/rtslib/tarball/master#egg=rtslib-3.0pre',
    ],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Other Environment',
        'Intended Audience :: System Administrators',
        ('License :: OSI Approved :: GNU General Public License v2 or later '
         '(GPLv2+)'),
        'Topic :: System :: Clustering',
        'Topic :: System :: Systems Administration',
    ],
    packages=find_packages(),
)
