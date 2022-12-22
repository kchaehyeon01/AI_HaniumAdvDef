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

# The value to indicate NO LIMIT parameter
NO_LIMIT = -1

# PER_CPU_SHARES has been set to 1024 because CPU shares' quota
# is commonly used in cloud frameworks like Kubernetes[1],
# AWS[2] and Mesos[3] in a similar way. They spawn containers with
# --cpu-shares option values scaled by PER_CPU_SHARES.
PER_CPU_SHARES = 1024

SUBSYS_MEMORY = "memory"
SUBSYS_CPUSET = "cpuset"
SUBSYS_CPU = "cpu"
SUBSYS_CPUACCT = "cpuacct"
SUBSYS_PIDS = "pids"

CGROUP_TYPE_V2 = "cgroup2"

DEFAULT_CPU_COUNT = 1
