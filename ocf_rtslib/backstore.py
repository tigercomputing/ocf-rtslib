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

from __future__ import print_function

import fcntl
import ocf
import os
import platform
import rtslib
import rtslib.utils
import subprocess
import sys

from ocf.util import cached_property
from rtslib import RTSLibError


class LockFile(object):
    def __init__(self, path):
        self.path = path
        self.fd = None

    def write(self):
        self.fd = open(self.path, 'w+')
        fcntl.lockf(self.fd, fcntl.LOCK_EX)

    def close(self):
        if self.fd is None:
            return

        self.fd.close()
        self.fd = None

    def __enter__(self):
        self.write()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        self.close()


class BackStoreAgent(ocf.ResourceAgent):
    """
    Manages a Linux SCSI Target backing device (LUN)

    The backstore resource manages a Linux-IO (LIO) backing LUN. This LUN can
    then be exported to an initiator via a number of transports, including
    iSCSI, FCoE, Fibre Channel, etc...

    This resource can be run a a single primitive or as a multistate
    (master/slave) resource. When used in multistate mode, the resource agent
    manages ALUA attributes for multipathing.
    """

    hba_type = ocf.Parameter(
        required=True, shortdesc='Backing store type',
        longdesc="""
The backing store HBA type, for example 'iblock' or 'fileio'.
        """)

    name = ocf.Parameter(
        required=True, shortdesc='LUN name',
        longdesc="""
The name of the LUN. Will be used when exporting the LUN via a transport, and
may optionally be exposed to the initiator.
        """)

    device = ocf.Parameter(
        required=True, unique=True, shortdesc='Backing device or file',
        longdesc="""
The backing device or file for the LUN.
        """)

    unit_serial = ocf.Parameter(
        required=True, unique=True, shortdesc='Unit serial number',
        longdesc="""
The T10 serial number to use for the LUN. Should be a UUID. This is exposed to
the initiator as the vendor-specific unit serial number, and is used to
generate the NAA WWN.
        """)

    attrib = ocf.Parameter(
        shortdesc='Backing store attributes',
        longdesc="""
Backing store attributes to set, in key=value form, separated by spaces.
Attributes not listed here will use default values set in the kernel.
        """)

    alua_hosts = ocf.Parameter(
        shortdesc='List of hosts this resource may run on',
        longdesc="""
This attribute is required when running in multistate mode and ignored
otherwise. It is a space separated list of hostnames that this resource might
run on, which is used to generate consistent ALUA port group IDs.
        """)

    @cached_property
    def rtsroot(self):
        return rtslib.RTSRoot()

    @cached_property
    def storage_object(self):
        try:
            for bs in self.rtsroot.backstores:
                if bs.plugin != self.hba_type:
                    continue

                for so in bs.storage_objects:
                    if so.name == self.name:
                        return so
        except RTSLibError:
            # target core probably isn't loaded
            pass

        return None

    @cached_property
    def alua_ptgp_name(self):
        return platform.node()

    @cached_property
    def alua_ptgp_id(self):
        return self.alua_hosts.split().index(self.alua_ptgp_name) + 16

    @property
    def next_free_hba_index(self):
        indexes = [backstore.index for backstore in self.rtsroot.backstores
                   if backstore.plugin == self.hba_type]

        backstore_index = None
        for index in range(1048576):
            if index not in indexes:
                backstore_index = index
                break

        return backstore_index

    def _create_iblock_storage_object(self):
        # First, create the Backstore object (HBA in old speak)
        bs = rtslib.IBlockBackstore(self.next_free_hba_index, mode='create')

        try:
            # Now create the storage object on top
            so = bs.storage_object(self.name, dev=self.device,
                                   wwn=self.unit_serial)
        except RTSLibError:
            bs.delete()
            raise
        else:
            return so

    def _create_fileio_storage_object(self):
        devopts = {x[0]: x[1] for x in [x.split('=', 1) for x
                                        in self.device.split(',')]}

        dev_name = devopts.get('fd_dev_name')
        dev_size = devopts.get('fd_dev_size')
        bufio = (devopts.get('fd_buffered_io') == '1')

        # First, create the Backstore object (HBA in old speak)
        bs = rtslib.FileIOBackstore(self.next_free_hba_index, mode='create')

        try:
            # Now create the storage object on top
            so = bs.storage_object(self.name, dev=dev_name, size=dev_size,
                                   buffered_mode=bufio)
        except RTSLibError:
            bs.delete()
            raise
        else:
            return so

    # You'd think that RTSLib has a mapping like this somewhere, but you would
    # be wrong. No matter, this will double up as a useful way of restricting
    # which backing store objects this RA supports and abstracting their
    # creation.
    HBA_TYPE_MAP = {
        'iblock': _create_iblock_storage_object,
        'fileio': _create_fileio_storage_object,
    }

    def _create_storage_object(self):
        # Acquire a global lock for prodding RTSLib; the various storage
        # objects can get into a funny state if two instances poke the same
        # parts at the same time.
        lockname = "{tmp}/{typ}.lock".format(tmp=ocf.env.rsctmp,
                                             typ=ocf.env.resource_type)
        with LockFile(lockname):
            return self.HBA_TYPE_MAP[self.hba_type](self)

    def _setup(self):
        # Ensure ALUA and PR state directories exist
        for d in ['/var/target/alua', '/var/target/pr']:
            if not os.path.isdir(d):
                try:
                    os.mkdir(d)
                except OSError:
                    # FIXME: use HA logging
                    print("failed to create directory: {d}".format(d=d),
                          file=sys.stderr)
                    return ocf.OCF_ERR_INSTALLED

        # Ensure configfs is loaded
        if not os.path.isdir('/sys/kernel/config'):
            ret = subprocess.call(['modprobe', 'configfs'])
            if ret:
                # FIXME: use HA logging
                print("failed to modprobe configfs", file=sys.stderr)
                return ocf.OCF_ERR_INSTALLED

        # Ensure configfs is mounted. We do this by just trying to mount it and
        # expect a specific error exit from mount if it's already there. We
        # redirect stdout and stderr to /dev/null while we do this, as it is
        # noisy otherwise
        with open('/dev/null', 'w') as devnull:
            ret = subprocess.call(
                ['mount', '-t', 'configfs', 'configfs', '/sys/kernel/config'],
                stdout=devnull, stderr=devnull)
            if ret not in [0, 32]:
                # 0 = mounted OK, 32 = already mounted
                # FIXME: use HA logging
                print("failed to mount configfs: {ret}".format(ret=ret),
                      file=sys.stderr)
                return ocf.OCF_ERR_INSTALLED

        return ocf.OCF_SUCCESS

    @ocf.Action(timeout=40)
    def start(self):
        # Make sure our basic infrastructure is present
        ret = self._setup()
        if ret != ocf.OCF_SUCCESS:
            return ret

        # Check whether we need to do anything
        ret = self.monitor()
        if ret == ocf.OCF_SUCCESS:
            # FIXME: use HA logging
            print("Resource is already running", file=sys.stderr)
            return ret

        so = self._create_storage_object()

        # FIXME: use HA logging
        print("Created storage object: {so.path}".format(so=so),
              file=sys.stderr)

        if ocf.env.is_ms:
            # FIXME: configure ALUA
            raise NotImplementedError()

        # Now set all the attributes as requested
        for attr in self.attrib.split():
            (name, value) = attr.split('=', 1)
            so.set_attribute(name, value)

    @ocf.Action(timeout=120)
    def stop(self):
        # Try the find our storage object
        so = self.storage_object
        if so is None:
            return ocf.OCF_SUCCESS

        # Now delete the device and the HBA
        so.delete()
        so.backstore.delete()

        return ocf.OCF_SUCCESS

    @ocf.Action(timeout=20, depth=0, interval=10)
    # @ocf.Action(timeout=20, depth=0, interval=20, role='Slave')
    # @ocf.Action(timeout=20, depth=0, interval=10, role='Master')
    def monitor(self):
        # If there isn't a storage object with the given type and name, the
        # resource can't be running.
        so = self.storage_object
        if so is None:
            return ocf.OCF_NOT_RUNNING

        # If we get this far, the resource is either running or "failed"
        # because it is only part configured.
        if not so.is_configured():
            return ocf.OCF_ERR_GENERIC

        if not ocf.env.is_ms:
            return ocf.OCF_SUCCESS

        # FIXME: check ALUA state for master/slave resources
        raise NotImplementedError()

    # @ocf.Action(timeout=90)
    # def promote(self):
    #     pass

    # @ocf.Action(timeout=90)
    # def demote(self):
    #     pass

    # @ocf.Action(timeout=90)
    # def notify(self):
    #     pass

    def validate_all(self):
        ret = super(BackStoreAgent, self).validate_all()
        if ret != ocf.OCF_SUCCESS:
            return ret

        if ocf.env.is_clone:
            if not ocf.env.is_ms:
                # FIXME: use HA logging
                print("This RA may only be used as a primitive or "
                      "master/slave resource, not a clone.", file=sys.stderr)
                return ocf.OCF_ERR_CONFIGURED

            if int(ocf.env.reskey.get('CRM_meta_clone_max', 0)) != 2 or \
               int(ocf.env.reskey.get('CRM_meta_clone_node_max', 0)) != 1 or \
               int(ocf.env.reskey.get('CRM_meta_master_node_max', 0)) != 1 or \
               int(ocf.env.reskey.get('CRM_meta_master_max', 0)) != 1:
                # FIXME: use HA logging
                print("Clone options misconfigured. (expect: clone_max=2,"
                      "clone_node_max=1,master_node_max=1,master_max=1)",
                      file=sys.stderr)
                return ocf.OCF_ERR_CONFIGURED

            if not self.alua_hosts:
                # FIXME: use HA logging
                print("alua_hosts parameter required for multistate resources",
                      file=sys.stderr)
                return ocf.OCF_ERR_CONFIGURED

            # Make sure our alua_ptgp_name is included in the alua_hosts list
            try:
                self.alua_ptgp_id
            except ValueError:
                # FIXME: use HA logging
                print("alua_hosts does not include {node}".format(
                    node=self.alua_ptgp_name), file=sys.stderr)
                return ocf.OCF_ERR_CONFIGURED

            print("Running as a multi-state resource", file=sys.stderr)

        # Ensure the HBA type is in our list of allowable types
        if self.hba_type not in self.HBA_TYPE_MAP:
            # FIXME: use HA logging
            print("Unknown hba_type: {hba}".format(hba=self.hba_type))
            return ocf.OCF_ERR_CONFIGURED

        if self.hba_type == 'iblock':
            # Check that the given device is a suitable block device for RTSLib
            if rtslib.utils.get_block_type(self.device) != 0:
                # FIXME: use HA logging
                print("Device is not a TYPE_DISK block device: {dev}".format(
                    dev=self.device), file=sys.stderr)
                return ocf.OCF_ERR_CONFIGURED
        elif self.hba_type == 'fileio':
            # device is a parameter string that can contain multiple options
            # separated by commas:
            #   fd_dev_name     path to file on disk
            #   fd_dev_size     size of file on disk - required for files
            #                     but ignored/optional for block devices
            #   fd_buffered_io  whether IO should be buffered - default is
            #                     unbuffered/synchronous

            devopts = {x[0]: x[1] for x in [x.split('=', 1) for x
                                            in self.device.split(',')]}

            name = devopts.get('fd_dev_name')
            size = devopts.get('fd_dev_size')
            bufio = devopts.get('fd_buffered_io')

            if bufio is not None and bufio != '1':
                # FIXME: use HA logging
                print("fd_buffered_io must be '1' or not set", file=sys.stderr)
                return ocf.OCF_ERR_CONFIGURED

            if size is None and rtslib.utils.get_block_type(name) != 0:
                # FIXME: use HA logging
                print("fd_dev_size must be given unless fd_dev_name is a "
                      "block device", file=sys.stderr)
                return ocf.OCF_ERR_CONFIGURED
        else:
            raise NotImplementedError('Missing checks')

        return self._setup()

if __name__ == '__main__':
    BackStoreAgent.main()

# vi:tw=0:wm=0:nowrap:ai:et:ts=8:softtabstop=4:shiftwidth=4
