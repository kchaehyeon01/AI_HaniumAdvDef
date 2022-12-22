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
#  This file can not be copied and/or distributed without the express
#  permission of Comet ML Inc.
# *******************************************************

import setuptools


def find_packages():
    packages = setuptools.find_packages()
    packages = [package for package in packages if package.startswith("comet_ml")]
    return packages


setuptools.setup(
    name="comet_ml",
    packages=find_packages(),
    package_data={"comet_ml": ["schemas/*.json"]},
    url="https://www.comet.ml",
    author="Comet ML Inc.",
    author_email="mail@comet.ml",
    description="Supercharging Machine Learning",
    long_description=open("README.rst").read(),
    long_description_content_type="text/x-rst",
    install_requires=[
        "websocket-client>=0.55.0",
        "requests>=2.18.4",
        "requests-toolbelt>=0.8.0",
        "six",
        "wurlitzer>=1.0.2",
        "nvidia-ml-py3>=7.352.0",
        "comet-git-pure>=0.19.11 ; python_version<'3.0'",
        "dulwich>=0.20.6, !=0.20.33 ; python_version>='3.0'",  # https://github.com/jelmer/dulwich/issues/950
        "everett==0.9 ; python_version<'3.0'",
        "everett[ini]>=1.0.1, < 1.0.3 ; python_version>='3.0' and python_version <='3.5'",
        "everett[ini]>=1.0.1; python_version>'3.5'",
        "jsonschema>=2.6.0,!=3.1.0,<4.0.0; python_version<'3.6'",
        "jsonschema>=2.6.0,!=3.1.0",
        "typing>=3.7.4; python_version<'3.5'",
        "wrapt>=1.11.2",
        "semantic-version>=2.8.0",
        "backports.functools_lru_cache ; python_version<'3.0'",
        "sentry-sdk>=1.1.0",
    ],
    extras_require={"cpu_logging": ["psutil>=5.6.3"]},
    entry_points={"console_scripts": ["comet = comet_ml.scripts.comet:main"]},
    python_requires=">=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*",
    classifiers=[
        "License :: Other/Proprietary License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    license="Proprietary",
    version="3.31.7",
)
