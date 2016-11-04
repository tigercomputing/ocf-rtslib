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

import ocf
import os
import platform
import re
import rtslib
import rtslib.utils
import subprocess
import sys

try:
    import netaddr
except ImportError:
    use_netaddr = False
else:
    use_netaddr = True

from ocf.util import cached_property
from rtslib import RTSLibError

#: List of kernel modules to load to bring up the target. This includes the
#: target core module as well as any relevant backstore modules.
TARGET_ISCSI_MODULES = [
    'iscsi_target_mod',
]


class ISCSITargetAgent(ocf.ResourceAgent):
    """
    Manages a Linux SCSI iSCSI Target Port Group (TPG)

    The iscsi resource manages a Linux-IO (LIO) iSCSI target port group (TPG).
    This is used to export a LIO backstore device to initiators over iSCSI.

    This resource can be run as a single primitive or as a cloned resource,
    but not a multi-state (master/slave) resource.
    """

    iqn = ocf.Parameter(
        required=True, shortdesc='iSCSI target IQN', longdesc="""
The target iSCSI Qualified Name (IQN). Should follow the conventional
"iqn.yyyy-mm.<reversed domain name>[:identifier]" syntax.
        """)

    initiators = ocf.Parameter(
        required=True,
        shortdesc='iSCSI initiators allowed to connect to this target',
        longdesc="""
Allowed initiators. A space-separated list of initiator IQNs allowed to login
to this target. Initiators may be listed in any syntax the LIO target
implementation allows.
        """)

    luns = ocf.Parameter(
        required=True, shortdesc='Logical Units to export', longdesc="""
The logical units to create as part of this target. Each logical unit is
specified as a lun:hba/name triplet. Separate multiple logical units with
spaces. Use shell syntax to escape special characters. Example: 0:iblock/volume
        """)

    portals = ocf.Parameter(
        default='0.0.0.0:3260', shortdesc='iSCSI Portal addresses',
        longdesc="""
Space separated list of iSCSI network portal addresses. If unset, the default
is to create a portal that listens on 0.0.0.0:3260. If the netaddr Python
module is available IP network addresses (including netmasks) can be specified
and any matching local IP address on the system will be added instead.
        """)

    alua_tpg = ocf.Parameter(
        default='default_tg_pt_gp', shortdesc='ALUA Target Port Group Name',
        longdesc="""
ALUA Target Port Group Name to set on all exported LUNs via this RA. The string
"@hostname@" will be replaced with the current node's hostname. Use this if you
are pairing this RA with the ocf:rtslib:backstore RA running in master/slave
mode.
        """)

    @cached_property
    def rtsroot(self):
        return rtslib.RTSRoot()

    @cached_property
    def fabric(self):
        return rtslib.FabricModule('iscsi')

    @property
    def target(self):
        # Wrap the targets generator to swallow RTSLibNotInCFS which can happen
        # if something else deletes a target during iteration.
        def _wrapper(gen):
            while True:
                try:
                    yield next(gen)
                except StopIteration:
                    raise
                except rtslib.utils.RTSLibNotInCFS:
                    pass

        for tgt in _wrapper(self.fabric.targets):
            if tgt.wwn == self.iqn:
                return tgt

        return None

    @property
    def tpg(self):
        target = self.target
        if target is None:
            return None

        for tpg in target.tpgs:
            # We assume a single TPGT per IQN for now
            if tpg.tag == 1:
                return tpg

        return None

    @cached_property
    def storage_objects(self):
        """
        A dictionary of LUN number => storage object
        """

        try:
            result = {}
            for lun_entry in self.luns.split():
                (lun, hbaname) = lun_entry.split(':', 1)
                (hba_type, bs_name) = hbaname.split('/', 1)
                lun = int(lun)

                if lun in result:
                    raise ValueError("Duplicate LUN number: {0}".format(lun))

                for bs in self.rtsroot.backstores:
                    if bs.plugin != hba_type:
                        continue

                    for so in bs.storage_objects:
                        if so.name == bs_name:
                            result[lun] = so
                            break

                if lun not in result:
                    raise ValueError("Backstore not found: {0}".format(
                        hbaname))

            return result
        except RTSLibError:
            # target core probably isn't loaded
            return None

    @cached_property
    def alua_ptgp_name(self):
        if self.alua_tpg == '@hostname@':
            return platform.node()
        else:
            return self.alua_tpg

    IP_PORT_RE = re.compile(
        r'^(?:(?P<ipv4>[0-9.]+(?:/[0-9]+)?)|'
        r'\[(?P<ipv6>[0-9a-fA-F:]+(?:/[0-9]+)?)\])'
        r'(?::(?P<port>[0-9]+))?$')

    @cached_property
    def portal_addresses(self):
        """
        A list of IP addresses and port numbers to create iSCSI portals on.

        Inspects the ``portals`` parameter, validates the values and returns a
        list of (ip, port) tuples. If the input 'address' is in fact a network
        address (including netmask) and the netaddr package is available, look
        up the IP addresses on this system that fall within the network
        instead.
        """
        addresses = []

        if use_netaddr:
            avail_addrs = [netaddr.IPAddress(x) for x in
                           rtslib.utils.list_eth_ips()]

        # Inspect each portal address separately
        for portal in self.portals.split():
            match = self.IP_PORT_RE.search(portal)
            if not match:
                raise ValueError("Invalid portal address: {0}".format(portal))

            # Extract the IP address/network and port number
            ip = match.group('ipv4') or match.group('ipv6')
            port = int(match.group('port') or 3260)

            # Is this a subnet mask?
            if '/' in ip:
                # We can only handle subnets if we can use netaddr
                if not use_netaddr:
                    raise ValueError(
                        'Need python netaddr module to use subnets')

                # Add all the matching addresses to the list
                net = netaddr.IPNetwork(ip)
                for addr in (addr for addr in avail_addrs if addr in net):
                    addresses.append((str(addr), port))
            else:
                # Add the address and port to the list
                addresses.append((ip, port))

        return addresses

    def _setup(self):
        # Check that the target core is loaded
        if not os.path.isdir('/sys/kernel/config/target'):
            return ocf.OCF_ERR_INSTALLED

        # Ensure the target modules are loaded
        if not os.path.isdir('/sys/kernel/config/target/iscsi'):
            # Get a list of all currently loaded kernel modules
            with open('/proc/modules', 'r') as fp:
                loaded_modules = [x.split()[0] for x in fp]

            # Make sure each of the target modules is loaded
            for mod in TARGET_ISCSI_MODULES:
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
            if not os.path.isdir('/sys/kernel/config/target/iscsi'):
                try:
                    os.mkdir('/sys/kernel/config/target/iscsi')
                except OSError:
                    ocf.log.error('failed to create iSCSI target config '
                                  'directory')
                    return ocf.OCF_ERR_INSTALLED

        return ocf.OCF_SUCCESS

    @ocf.Action(timeout=40)
    def start(self):
        # Check whether we need to do anything
        ret = self.monitor()
        if ret == ocf.OCF_SUCCESS:
            ocf.log.warning('Resource is already running')
            return ret

        # Create the target if it doesn't exist
        target = self.target
        if target is None:
            target = rtslib.Target(self.fabric, wwn=self.iqn, mode='create')

        # Create the Target Port Group if it doesn't exist
        tpg = self.tpg
        if tpg is None:
            tpg = rtslib.TPG(target, 1, mode='create')

            # Enable the target as soon as possible. If something goes wrong
            # further down, rtslib will fail to remove a non-enabled TPG, and
            # Pacemaker will fence the node.
            tpg.enable = True

        # Add the backstore LUNs
        luns = {}
        for lun, so in self.storage_objects.iteritems():
            lun_obj = rtslib.LUN(tpg, lun, so)
            luns[lun] = lun_obj

            # Set the ALUA target port group name
            with open(os.path.join(lun_obj.path, 'alua_tg_pt_gp'), 'w') as fd:
                fd.write(self.alua_ptgp_name + "\n")

        # Add the Node ACLs
        for initiator in self.initiators.split():
            nacl = rtslib.NodeACL(tpg, initiator, mode='create')

            # Map all of the LUNs to this NACL
            for mapped_lun, tpg_lun in luns.iteritems():
                rtslib.MappedLUN(nacl, mapped_lun, tpg_lun)

        # FIXME: We should support authentication properly
        # Disable authentication
        tpg.set_attribute('authentication', '0')
        tpg.set_parameter('AuthMethod', 'None')

        # FIXME: Add support for setting parameters and attributes

        # Add all the network portals. Do this last so initiators can't login
        # before the target is fully configured.
        for ip, port in self.portal_addresses:
            rtslib.NetworkPortal(tpg, ip_address=ip, port=port, mode='create')

        return ocf.OCF_SUCCESS

    @ocf.Action(timeout=60)
    def stop(self):
        # Try to locate our TPG object
        tpg = self.tpg
        if tpg is None:
            return ocf.OCF_SUCCESS

        # RTSLib can only delete targets that are enabled, so try to enable it
        # if it isn't already.
        if not tpg.enable:
            tpg.enable = True

        # Just ask the TPG to delete itself, this takes care of all the
        # mapped LUNs, LUNs, Node ACLs, Network Portals, etc...
        tpg.delete()

        # Delete the target if this was the last TPG
        if len(list(self.target.tpgs)) == 0:
            self.target.delete()

        return ocf.OCF_SUCCESS

    @ocf.Action(timeout=10, depth=0, interval=10)
    def monitor(self):
        # Try to locate our TPG object
        tpg = self.tpg
        if tpg is None:
            return ocf.OCF_NOT_RUNNING

        if not tpg.enable:
            ocf.log.error("TPG is not enabled")
            return ocf.OCF_ERR_GENERIC

        # Check all the LUNs we want are in place
        storage_objects = self.storage_objects
        unseen_luns = set(storage_objects.keys())
        for lun in tpg.luns:
            # Ensure that we're supposed to have a LUN at this index
            try:
                unseen_luns.remove(lun.lun)
            except KeyError:
                ocf.log.error("Spurious LUN found: {0}".format(lun.lun))
                return ocf.OCF_ERR_GENERIC

            # Check that this LUN's storage object corresponds to the one we
            # expect in this position
            if lun.storage_object.path != storage_objects[lun.lun].path:
                ocf.log.error("Unexpected LUN at index: {0}".format(lun.lun))
                return ocf.OCF_ERR_GENERIC

        # Check whether we are missing any LUNs
        if unseen_luns:
            ocf.log.error("Missing LUN(s)")
            return ocf.OCF_ERR_GENERIC

        # Check all the Node ACLs are in place
        initiators = set(self.initiators.split())
        for nacl in tpg.node_acls:
            # Ensure we're supposed to have this NACL in place
            try:
                initiators.remove(nacl.node_wwn)
            except KeyError:
                ocf.log.error("Spurious Node ACL found: {0}".format(
                    nacl.node_wwn))
                return ocf.OCF_ERR_GENERIC

            # Check that all the LUNs are mapped
            unseen_luns = set(storage_objects.keys())
            for mlun in nacl.mapped_luns:
                idx = mlun.mapped_lun

                # Check for spurious LUN mappings
                try:
                    unseen_luns.remove(idx)
                except KeyError:
                    ocf.log.error("Spurious LUN mapping found: {0}".format(
                        idx))

                # Check that the LUN mapping is 1-1
                if idx != mlun.tpg_lun.lun:
                    ocf.log.error("LUN mapping not 1-1: {0} != {1}".format(
                        idx, mlun.tpg_lun.lun))
                    return ocf.OCF_ERR_GENERIC

                # Check that the LUN mapping points at the LUN we want
                so = mlun.tpg_lun.storage_object
                if so.path != storage_objects[idx].path:
                    ocf.log.error("Unexpected LUN mapping at index {0}".format(
                        idx))
                    return ocf.OCF_ERR_GENERIC

            # Check whether we are missing any LUN mappings
            if unseen_luns:
                ocf.log.error("Missing LUN mapping(s)")
                return ocf.OCF_ERR_GENERIC

        # Check for missing ACLs
        if initiators:
            ocf.log.error("Missing Node ACL(s)")
            return ocf.OCF_ERR_GENERIC

        # Check for Network Portals
        portals = set(self.portal_addresses)
        for portal in tpg.network_portals:
            try:
                portals.remove((portal.ip_address, portal.port))
            except KeyError:
                ocf.log.error("Spurious network portal found: {0} {1}".format(
                    portal.ip_address, portal.port))
                return ocf.OCF_ERR_GENERIC

        # Check for missing portals
        if portals:
            ocf.log.error("Missing network portal(s)")
            return ocf.OCF_ERR_GENERIC

        return ocf.OCF_SUCCESS

    def validate_all(self):
        ret = super(ISCSITargetAgent, self).validate_all()
        if ret != ocf.OCF_SUCCESS:
            return ret

        if not self.fabric.is_valid_wwn(self.iqn):
            ocf.log.error("Target WWN is not valid for fabric: {0}"
                          .format(self.iqn))
            return ocf.OCF_ERR_CONFIGURED

        for initiator in self.initiators.split():
            if not self.fabric.is_valid_wwn(initiator):
                ocf.log.error("Initiator WWN is not valid for fabric: {0}"
                              .format(initiator))
                return ocf.OCF_ERR_CONFIGURED

        try:
            self.portal_addresses
        except ValueError as e:
            ocf.log.error(str(e))
            return ocf.OCF_ERR_CONFIGURED
        else:
            if len(self.portal_addresses) == 0:
                ocf.log.error('No valid portal addresses found.')
                return ocf.OCF_ERR_CONFIGURED

        try:
            self.storage_objects
        except ValueError as e:
            ocf.log.error("LUNs list invalid: {0}".format(e))
            return ocf.OCF_ERR_CONFIGURED

        return ocf.OCF_SUCCESS

    def _validate_parameters(self):
        super(ISCSITargetAgent, self)._validate_parameters()

        # Make sure all the right bits of configfs are there before we try to
        # do anything. If this fails during a probe, we just tell Pacemaker
        # that nothing can be running.
        ret = self._setup()
        if ret != ocf.OCF_SUCCESS:
            if ocf.env.is_probe:
                sys.exit(ocf.OCF_NOT_RUNNING)
            else:
                sys.exit(ret)

if __name__ == '__main__':
    ISCSITargetAgent.main()

# vi:tw=0:wm=0:nowrap:ai:et:ts=8:softtabstop=4:shiftwidth=4
