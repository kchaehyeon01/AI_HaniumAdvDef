# -*- coding: utf-8 -*-
# *******************************************************
#   ____                     _               _
#  / ___|___  _ __ ___   ___| |_   _ __ ___ | |
# | |   / _ \| '_ ` _ \ / _ \ __| | '_ ` _ \| |
# | |__| (_) | | | | | |  __/ |_ _| | | | | | |
#  \____\___/|_| |_| |_|\___|\__(_)_| |_| |_|_|
#
#  Sign up for free at http://www.comet.ml
#  Copyright (C) 2015-2021 Comet ML INC
#  This file can not be copied and/or distributed
#  without the express permission of Comet ML Inc.
# *******************************************************

import logging

from comet_ml._typing import Dict, List, Tuple, Union
from comet_ml.oscontainer.cgroup_subsystem import CgroupInfo, CgroupSubsystem
from comet_ml.oscontainer.cgroup_v2_subsystem import (
    CgroupV2Controller,
    CgroupV2Subsystem,
)
from comet_ml.oscontainer.constants import (
    CGROUP_TYPE_V2,
    SUBSYS_CPU,
    SUBSYS_CPUACCT,
    SUBSYS_CPUSET,
    SUBSYS_MEMORY,
    SUBSYS_PIDS,
)
from comet_ml.oscontainer.errors import OSContainerError
from comet_ml.oscontainer.scanf import scanf

LOGGER = logging.getLogger(__name__)

# The common paths to the related cgroup files
PROC_SELF_MOUNTINFO = "/proc/self/mountinfo"
PROC_SELF_CGROUP = "/proc/self/cgroup"
PROC_CGROUPS = "/proc/cgroups"


def build_cgroup_subsystem(
    proc_self_cgroup=PROC_SELF_CGROUP,
    proc_self_mountinfo=PROC_SELF_MOUNTINFO,
    proc_cgroups=PROC_CGROUPS,
):
    # type: (str,str,str) -> CgroupSubsystem
    """
    Builds appropriate cgroup subsystem.
    :param proc_cgroups: the optional path to the '/proc/cgroups' file.
    :param proc_self_cgroup: the optional path to the '/proc/self/cgroup' file.
    :param proc_self_mountinfo: the optional path to the '/proc/self/mountinfo' file.
    :return: the cgroup subsystem
    """
    subsystem_info = determine_type(
        proc_self_cgroup=proc_self_cgroup,
        proc_self_mountinfo=proc_self_mountinfo,
        proc_cgroups=proc_cgroups,
    )
    if subsystem_info is None:
        raise OSContainerError("Required cgroup subsystem files not found")

    cgroup_type, cg_infos = subsystem_info
    if cgroup_type == CGROUP_TYPE_V2:
        info = cg_infos[SUBSYS_MEMORY]
        controller = CgroupV2Controller(info.mount_path, info.cgroup_path)
        return CgroupV2Subsystem(controller)

    raise OSContainerError("Only cgroup v2 supported")


def determine_type(proc_cgroups, proc_self_cgroup, proc_self_mountinfo):
    # type: (str, str, str) -> Union[None, Tuple[str, Dict[str, CgroupInfo]]]
    """
    Determines the type of the cgroups filesystem. Returns the type detected or None if failed.

    :param proc_cgroups: the path to the '/proc/cgroups' file.
    :param proc_self_cgroup: the path to the '/proc/self/cgroup' file.
    :param proc_self_mountinfo: the path to the '/proc/self/mountinfo' file.
    :return: the Tuple with cgroup filesystem type and dictionary of populated CgroupInfo
    """

    # Read /proc/cgroups to be able to distinguish cgroups v2 vs cgroups v1.
    subsys_controllers = [
        SUBSYS_PIDS,
        SUBSYS_MEMORY,
        SUBSYS_CPU,
        SUBSYS_CPUACCT,
        SUBSYS_CPUSET,
    ]
    cg_infos = _read_cgroup_infos(
        proc_cgroups=proc_cgroups, subsys_controllers=subsys_controllers
    )  # type: Dict[str, CgroupInfo]

    # True for cgroups v2 (unified hierarchy)
    is_cgroupsV2 = True
    # True if all required controllers, memory, cpu, cpuset, cpuacct are enabled
    all_required_controllers_enabled = True
    for k, v in cg_infos.items():
        # pids controller is optional. All other controllers are required
        if k != SUBSYS_PIDS:
            is_cgroupsV2 = is_cgroupsV2 and v.hierarchy_id == 0
            all_required_controllers_enabled = (
                all_required_controllers_enabled and v.enabled
            )

    if not all_required_controllers_enabled:
        # one or more of required controllers disabled, disable container support
        LOGGER.info("One or more required CGROUP controllers disabled at kernel level.")
        return None

    # Read /proc/self/cgroup into cg_infos
    _read_proc_self_cgroup(
        cg_infos=cg_infos, proc_self_cgroup=proc_self_cgroup, is_cgroups_v2=is_cgroupsV2
    )

    # Read /proc/self/mountinfo and find mount points
    cgroupv2_mount_point_found, any_cgroup_mounts_found = _read_mount_points(
        proc_self_mountinfo=proc_self_mountinfo,
        cg_infos=cg_infos,
        is_cgroups_v2=is_cgroupsV2,
        subsys_controllers=subsys_controllers,
    )

    # Neither cgroup2 nor cgroup filesystems mounted via /proc/self/mountinfo
    # No point in continuing.
    if not any_cgroup_mounts_found:
        LOGGER.debug("No relevant cgroup controllers mounted.")
        return None

    if is_cgroupsV2:
        if not cgroupv2_mount_point_found:
            LOGGER.warning("Mount point for cgroupv2 not found in /proc/self/mountinfo")
            return None
        return CGROUP_TYPE_V2, cg_infos

    # The rest is cgroups v1
    LOGGER.debug(
        "Detected potential cgroups v1 controllers. It is not supported, ignoring."
    )
    return None


def _read_mount_points(
    proc_self_mountinfo, cg_infos, is_cgroups_v2, subsys_controllers
):
    # type: (str, Dict[str, CgroupInfo], bool, List[str]) -> Tuple[bool, bool]
    """
    Finds various mount points by reading /proc/self/mountinfo file.
    mountinfo format is documented at https://www.kernel.org/doc/Documentation/filesystems/proc.txt

    :param proc_self_mountinfo: the path to the /proc/self/mountinfo file.
    :param cg_infos: the dictionary with control group controllers.
    :param is_cgroups_v2: if True it is cgroup v2 was detected before.
    :param subsys_controllers: the list with names of subsystem controllers.
    :return: (cgroupv2_mount_point_found, any_cgroup_mounts_found)
    """
    # Find various mount points by reading /proc/self/mountinfo
    # mountinfo format is documented at https://www.kernel.org/doc/Documentation/filesystems/proc.txt
    #
    # 496 495 0:30 / /sys/fs/cgroup ro,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw
    cgroupv2_mount_point_found = False
    any_cgroup_mounts_found = False
    LOGGER.debug("Reading mountinfo from: %s", proc_self_mountinfo)
    with open(proc_self_mountinfo, "r") as f:
        for line in f:
            LOGGER.debug(line)
            # Cgroup v2 relevant info. We only look for the mount_path if is_cgroupsV2
            # to avoid memory stomping of the mount_path later on in the cgroup v1
            # block in the hybrid case.
            if is_cgroups_v2:
                fields_cgroups_v2 = scanf("%*d %*d %*d:%*d %*s %s %*s - %s %*s %*s", line)  # type: ignore
                if fields_cgroups_v2 is not None and len(fields_cgroups_v2) == 2:
                    tmp_mount_point, tmp_fs_type = fields_cgroups_v2
                    if not cgroupv2_mount_point_found and tmp_fs_type == CGROUP_TYPE_V2:
                        cgroupv2_mount_point_found = True
                        any_cgroup_mounts_found = True
                        for k in cg_infos:
                            assert (
                                cg_infos[k].mount_path is None
                            ), "mount_path memory stomping"
                            cg_infos[k].mount_path = tmp_mount_point

    return cgroupv2_mount_point_found, any_cgroup_mounts_found


def _read_cgroup_infos(proc_cgroups, subsys_controllers):
    # type: (str, List[str]) -> Dict[str, CgroupInfo]
    """
    Read /proc/cgroups to be able to distinguish cgroups v2 vs cgroups v1.

    For cgroups v1 hierarchy (hybrid or legacy), cpu, cpuacct, cpuset, memory controllers
    must have non-zero for the hierarchy ID field and relevant controllers mounted.
    Conversely, for cgroups v2 (unified hierarchy), cpu, cpuacct, cpuset, memory
    controllers must have hierarchy ID 0 and the unified controller mounted.

    :param proc_cgroups: the path to the '/proc/cgroups' file.
    :param subsys_controllers: the list with names of subsys controllers.
    :return: the dictionary with info about controllers of control groups.
    """
    # subsys_name	hierarchy	num_cgroups	enabled
    # cpuset	        0	        36	        1
    cg_infos = dict()
    LOGGER.debug("Reading cgroups info from: %s", proc_cgroups)
    with open(proc_cgroups, "r") as f:
        for line in f:
            LOGGER.debug(line)
            res = scanf("%s %d %*d %d", line)
            if res is None or len(res) != 3:
                continue

            name, hierarchy_id, enabled = res
            if name in subsys_controllers:
                cg_infos[name] = CgroupInfo(name, hierarchy_id, bool(enabled))

    return cg_infos


def _read_proc_self_cgroup(proc_self_cgroup, cg_infos, is_cgroups_v2):
    # type: (str, Dict[str, CgroupInfo], bool) -> bool
    """
    Reads /proc/self/cgroup and determine:
     - the cgroup path for cgroups v2 or
     - on a cgroups v1 system, collect info for mapping
       the host mount point to the local one via /proc/self/mountinfo below.

    :param cg_infos: the dictionary with control group controllers.
    :param proc_self_cgroup: the file path to the '/proc/self/cgroup' file.
    :param is_cgroups_v2: if True it is cgroup v2 was detected before.
    :return: True if records was found
    """
    # 0::/ (cgroups v2)
    # 8:memory:/docker/container-sha (cgroups v1)
    LOGGER.debug("Reading self cgroups info from: %s", proc_self_cgroup)
    with open(proc_self_cgroup, "r") as f:
        for line in f:
            LOGGER.debug(line)
            vals = line.split(":")
            if len(vals) != 3:
                continue

            cgroup_path = vals[2].strip()

            if is_cgroups_v2:
                for k in cg_infos:
                    cg_infos[k].cgroup_path = cgroup_path
                return True

    return False
