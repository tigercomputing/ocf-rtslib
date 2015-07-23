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

all: flake8 sphinx nosetests

clean:
	./setup.py clean
	find . -name .git -prune -o -name \*.pyc -type f -print0 | \
		xargs -0 -r rm
	rm -f .coverage
	rm -rf build dist doc/build ocf_rtslib.egg-info

flake8:
	./setup.py flake8

nosetests:
	./setup.py nosetests

sphinx:
	./setup.py build_sphinx
