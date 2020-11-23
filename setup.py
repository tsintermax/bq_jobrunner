#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup

REQUIRES = [
    "graphviz",
    "protobuf",
    "setuptools",
    "google-cloud",
    "google-cloud-bigquery",
    "networkx",
    "pydotplus",
    "pydot",
]

CLASSIFIERS = """\
Intended Audience :: Science/Research
Intended Audience :: Developers
Programming Language :: Python
Programming Language :: Python :: 3.6
Programming Language :: Python :: 3.7
Programming Language :: Python :: 3.8
Topic :: Scientific/Engineering
Operating System :: Unix
Operating System :: MacOS
"""

MAJOR = 0
MINOR = 1
MICRO = 2
VERSION = "%d.%d.%d" % (MAJOR, MINOR, MICRO)


def setup_package():
    setup(
        name="bq_jobrunner",
        maintainer="JDSC",
        maintainer_email="takuto.sugisaki@jdsc.ai",
        version=VERSION,
        description="Enables to easily query multiple BigQuery queries with any dependencies.",
        author="Takuto Sugisaki",
        author_email="tsintermax@gmail.com",
        license="MIT",
        keywords="BigQuery automate query",
        packages=["bq_jobrunner"],
        install_requires=REQUIRES,
        classifiers=[_f for _f in CLASSIFIERS.split("\n") if _f],
        platforms=["Linux" "Mac OS-X", "Unix"],
    )


if __name__ == "__main__":
    setup_package()
