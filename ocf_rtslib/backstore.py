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

import errno
import fcntl
import ocf
import os
import platform
import rtslib
import rtslib.utils
import subprocess
import time

from ocf.util import cached_property
from rtslib import RTSLibError

#: List of kernel modules to load to bring up the target. This includes the
#: target core module as well as any relevant backstore modules.
TARGET_CORE_MODULES = [
    'target_core_mod',
    'target_core_file',
    'target_core_iblock',
    'target_core_pscsi',
]


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

    This resource can be run as a single primitive or as a multistate
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

    @property
    def storage_object(self):
        try:
            return self.__storage_object
        except AttributeError:
            pass

        try:
            for bs in self.rtsroot.backstores:
                # In Linux 4.7 kernels, the hba_info file in configfs
                # intermittently goes missing. If we get an ENOENT, retry a few
                # times until it maybe appears.
                plugin = None
                for i in range(1, 10):
                    try:
                        plugin = bs.plugin
                    except IOError as e:
                        if e.errno == errno.ENOENT:
                            continue
                        raise
                    else:
                        break

                if plugin is None:
                    break

                if plugin != self.hba_type:
                    continue

                for so in bs.storage_objects:
                    if so.name == self.name:
                        self.__storage_object = so
                        return so
        except RTSLibError:
            # target core probably isn't loaded
            pass

        return None

    @cached_property
    def alua_ptgp_name(self):
        if not ocf.env.is_ms:
            return 'default_tg_pt_gp'
        else:
            return platform.node()

    @cached_property
    def alua_ptgp_id(self):
        if not ocf.env.is_ms:
            return 0

        if not self.alua_hosts:
            raise ValueError('alua_hosts is not set')

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
                    ocf.log.error("failed to create directory: {d}"
                                  .format(d=d))
                    return ocf.OCF_ERR_INSTALLED

        # Ensure configfs is loaded
        if not os.path.isdir('/sys/kernel/config'):
            ret = subprocess.call(['modprobe', 'configfs'])
            if ret:
                ocf.log.error('failed to modprobe configfs')
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
                ocf.log.error("failed to mount configfs: {ret}"
                              .format(ret=ret))
                return ocf.OCF_ERR_INSTALLED

        # Ensure the target modules are loaded
        if not os.path.isdir('/sys/kernel/config/target'):
            # Get a list of all currently loaded kernel modules
            with open('/proc/modules', 'r') as fp:
                loaded_modules = [x.split()[0] for x in fp]

            # Make sure each of the target modules is loaded
            for mod in TARGET_CORE_MODULES:
                # Skip if already loaded
                if mod in loaded_modules:
                    continue

                ret = subprocess.call(['modprobe', mod])
                if ret:
                    ocf.log.error("failed to modprobe {mod}".format(mod=mod))
                    return ocf.OCF_ERR_INSTALLED

            # Now that the modules are loaded, the directory may have already
            # appeared or we may have to create it, depending on the kernel
            # version.
            if not os.path.isdir('/sys/kernel/config/target'):
                try:
                    os.mkdir('/sys/kernel/config/target')
                except OSError:
                    ocf.log.error('failed to create target config directory')
                    return ocf.OCF_ERR_INSTALLED

        return ocf.OCF_SUCCESS

    def _create_alua_ptgp(self, pt_gp_name=None):
        if pt_gp_name is None:
            pt_gp_name = self.alua_ptgp_name

        pt_gp_id = self.alua_hosts.split().index(pt_gp_name) + 16
        so_path = self.storage_object.path
        alua_dir = os.path.join(so_path, 'alua', pt_gp_name)

        ocf.log.debug("Creating ALUA TPG {name}; ID {id}".format(
            name=pt_gp_name, id=pt_gp_id))

        if not os.path.isdir(alua_dir):
            os.mkdir(alua_dir)

        with open(os.path.join(alua_dir, 'tg_pt_gp_id'), 'w') as fd:
            fd.write(str(pt_gp_id) + "\n")

    def get_alua(self, prop, pt_gp_name=None):
        if pt_gp_name is None:
            pt_gp_name = self.alua_ptgp_name

        so_path = self.storage_object.path
        prop_path = os.path.join(so_path, 'alua', pt_gp_name, prop)

        with open(prop_path, 'r') as fd:
            return fd.read()

    def set_alua(self, prop, value, pt_gp_name=None):
        if pt_gp_name is None:
            pt_gp_name = self.alua_ptgp_name

        so_path = self.storage_object.path
        prop_path = os.path.join(so_path, 'alua', pt_gp_name, prop)

        with open(prop_path, 'w') as fd:
            fd.write(value)

    def _set_master_score(self, score):
        if score is None:
            subprocess.check_call(
                ['/usr/sbin/crm_master', '-l', 'reboot', '-D'])
        else:
            subprocess.check_call(
                ['/usr/sbin/crm_master', '-Q', '-l', 'reboot', '-v',
                 str(score)])

    def _update_master_score(self, status):
        # Only update master score if this is a master/slave resource
        if not ocf.env.is_ms:
            return

        if status == ocf.OCF_NOT_RUNNING or self.storage_object is None:
            # We are stopped; we should not offer to become master at all
            self._set_master_score(None)
        elif status == ocf.OCF_SUCCESS or status == ocf.OCF_RUNNING_MASTER:
            # We are in slave or master mode

            # Count how many target ports this backing device is a member of
            ports = [x for x in self.get_alua('members').splitlines() if x]
            num_target_ports = len(ports)

            # Our score is simply 1000 times the number of ports we are a
            # member of. This works pretty well: until our fabric is configured
            # we refuse to become master on this node. If there are multiple
            # fabrics, the node with the most configured fabrics is preferred.
            score = num_target_ports * 1000

            ocf.log.debug("Setting master score to: {score}"
                          .format(score=score))

            self._set_master_score(score)
        else:
            # Some kind of error; we should not offer to become master at all
            self._set_master_score(None)

    @ocf.Action(timeout=40)
    def start(self):
        # Make sure our basic infrastructure is present
        ret = self._setup()
        if ret != ocf.OCF_SUCCESS:
            return ret

        # Check whether we need to do anything
        ret = self._monitor()
        if ret == ocf.OCF_SUCCESS:
            ocf.log.warning("Resource is already running")
            return ret

        so = self._create_storage_object()

        ocf.log.debug("Created storage object: {so.path}".format(so=so))

        # Configure ALUA
        if ocf.env.is_ms:
            # Create ALUA target port group
            self._create_alua_ptgp()

            # Set up Implicit ALUA (controlled by target only)
            self.set_alua('alua_access_type', '1\n')

            # Start up in 'slave' mode: ALUA_ACCESS_STATE_STANDBY and no pref
            self.set_alua('alua_access_state', '2\n')
            self.set_alua('preferred', '0\n')
        else:
            # Disable ALUA completely
            self.set_alua('alua_access_type', '0\n')
            self.set_alua('preferred', '0\n')

        # Now set all the attributes as requested
        if self.attrib:
            for attr in self.attrib.split():
                (name, value) = attr.split('=', 1)
                so.set_attribute(name, value)

        self._update_master_score(ocf.OCF_SUCCESS)

    @ocf.Action(timeout=120)
    def stop(self):
        # Try the find our storage object
        so = self.storage_object
        if so is None:
            return ocf.OCF_SUCCESS

        # Remove all the ALUA target port groups
        for alua_dir, pt_gp_names, _ in os.walk(os.path.join(so.path, 'alua')):
            for pt_gp_name in pt_gp_names:
                # Skip the default port group; we can't remove it
                if pt_gp_name == 'default_tg_pt_gp':
                    continue

                # Remove the port group
                os.rmdir(os.path.join(alua_dir, pt_gp_name))

            # Break out of the walk; we only want to walk the alua_dir
            break

        # Now delete the device and the HBA
        so.delete()
        so.backstore.delete()

        self._update_master_score(ocf.OCF_NOT_RUNNING)

        return ocf.OCF_SUCCESS

    def _monitor(self):
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

        alua_state = int(self.get_alua('alua_access_state').strip())
        alua_pref = int(self.get_alua('preferred').strip())

        if alua_state == 0 and alua_pref == 1:
            # ALUA_ACCESS_STATE_ACTIVE_OPTIMIZED and preferred path
            return ocf.OCF_RUNNING_MASTER
        elif alua_state == 2 and alua_pref == 0:
            # ALUA_ACCESS_STATE_STANDBY and not preferred path
            return ocf.OCF_SUCCESS  # slave
        else:
            return ocf.OCF_FAILED_MASTER

    @ocf.Action(timeout=20, depth=0, interval=10)
    @ocf.Action(timeout=20, depth=0, interval=20, role='Slave')
    @ocf.Action(timeout=20, depth=0, interval=10, role='Master')
    def monitor(self):
        ret = self._monitor()
        self._update_master_score(ret)
        return ret

    @ocf.Action(timeout=90)
    def promote(self):
        ret = ocf.OCF_ERR_GENERIC
        first_try = True

        # Keep trying to promote the resource;
        # wait for the CRM to time us out if this fails
        while True:
            status = self._monitor()

            if status == ocf.OCF_SUCCESS:  # in slave mode
                ocf.log.info('Attempting to promote.')

                # Set ALUA_ACCESS_STATE_ACTIVE_OPTIMIZED and preferred path
                self.set_alua('alua_access_state', '0\n')
                self.set_alua('preferred', '1\n')

                # Go around the loop again to make sure we come up as
                # OCF_RUNNING_MASTER
            elif status == ocf.OCF_NOT_RUNNING:
                ocf.log.error('Trying to promote a resource that was not '
                              'started!')
                break
            elif status == ocf.OCF_RUNNING_MASTER:
                ocf.log.info('Promotion successful.')
                ret = ocf.OCF_SUCCESS
                self._update_master_score(status)
                break

            # Avoid a busy loop
            if not first_try:
                time.sleep(1)
            first_try = False

        # avoid too tight pacemaker driven "recovery" loop if promotion keeps
        # failing for some reason
        if ret != ocf.OCF_SUCCESS:
            ocf.log.error('Promotion failed; sleeping 15s to prevent tight '
                          'recovery loop')
            time.sleep(15)

        return ret

    @ocf.Action(timeout=90)
    def demote(self):
        ret = ocf.OCF_ERR_GENERIC
        first_try = True

        # Keep trying to promote the resource;
        # wait for the CRM to time us out if this fails
        while True:
            status = self._monitor()

            if status == ocf.OCF_SUCCESS:  # in slave mode
                ocf.log.info('Demotion successful.')
                ret = ocf.OCF_SUCCESS
                self._update_master_score(status)
                break
            elif status == ocf.OCF_NOT_RUNNING:
                ocf.log.error('Trying to demote a resource that was not '
                              'started!')
                break
            elif status == ocf.OCF_RUNNING_MASTER:
                ocf.log.info('Attempting to demote.')

                # Set ALUA_ACCESS_STATE_STANDBY and no preference
                self.set_alua('alua_access_state', '2\n')
                self.set_alua('preferred', '0\n')

                # Go around the loop again to make sure we come up as a slave
                # (OCF_SUCCESS)

            # Avoid a busy loop
            if not first_try:
                time.sleep(1)
            first_try = False

        # avoid too tight pacemaker driven "recovery" loop if demotion keeps
        # failing for some reason
        if ret != ocf.OCF_SUCCESS:
            ocf.log.error('Demotion failed; sleeping 15s to prevent tight '
                          'recovery loop')
            time.sleep(15)

        return ret

    @ocf.Action(timeout=90)
    def notify(self):
        # TODO: use notifications to set state of all ALUA port groups at the
        # same time, so that initiators know the state of all port groups by
        # querying any single target port. This is required by certain
        # initiators like VMware.
        pass

    def validate_all(self):
        ret = super(BackStoreAgent, self).validate_all()
        if ret != ocf.OCF_SUCCESS:
            return ret

        if ocf.env.is_clone:
            if not ocf.env.is_ms:
                ocf.log.error('This RA may only be used as a primitive or '
                              'master/slave resource, not a clone.')
                return ocf.OCF_ERR_CONFIGURED

            if int(ocf.env.reskey.get('CRM_meta_clone_max', 0)) != 2 or \
               int(ocf.env.reskey.get('CRM_meta_clone_node_max', 0)) != 1 or \
               int(ocf.env.reskey.get('CRM_meta_master_node_max', 0)) != 1 or \
               int(ocf.env.reskey.get('CRM_meta_master_max', 0)) != 1:

                ocf.log.error('Clone options misconfigured. (expect: '
                              'clone_max=2,clone_node_max=1,master_node_max=1,'
                              'master_max=1)')
                return ocf.OCF_ERR_CONFIGURED

            if not self.alua_hosts:
                ocf.log.error('alua_hosts parameter required for multistate '
                              'resources')
                return ocf.OCF_ERR_CONFIGURED

            # Make sure our alua_ptgp_name is included in the alua_hosts list
            try:
                self.alua_ptgp_id
            except ValueError:
                ocf.log.error("alua_hosts does not include {node}".format(
                    node=self.alua_ptgp_name))
                return ocf.OCF_ERR_CONFIGURED

            ocf.log.debug('Running as a multi-state resource')

        # Ensure the HBA type is in our list of allowable types
        if self.hba_type not in self.HBA_TYPE_MAP:
            ocf.log.error("Unknown hba_type: {hba}".format(hba=self.hba_type))
            return ocf.OCF_ERR_CONFIGURED

        if self.hba_type == 'iblock':
            # Check that the given device is a suitable block device for RTSLib
            if rtslib.utils.get_block_type(self.device) != 0:
                ocf.log.error("Device is not a TYPE_DISK block device: {dev}"
                              .format(dev=self.device))
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
                ocf.log.error('fd_buffered_io must be "1" or not set')
                return ocf.OCF_ERR_CONFIGURED

            if size is None and rtslib.utils.get_block_type(name) != 0:
                ocf.log.error('fd_dev_size must be given unless fd_dev_name '
                              'is a block device')
                return ocf.OCF_ERR_CONFIGURED
        else:
            raise NotImplementedError('Missing checks')

        return self._setup()

if __name__ == '__main__':
    BackStoreAgent.main()

# vi:tw=0:wm=0:nowrap:ai:et:ts=8:softtabstop=4:shiftwidth=4
