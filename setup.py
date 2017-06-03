#!/usr/bin/env python3
from setuptools import setup

setup(
    name="Meterer",
    version="0.3.0",
    packages=["meterer"],
    setup_requires=["nose>=1.0"],
    install_requires=["boto3", "redis"],
    tests_require=["coverage>=4.0", "moto>=0.4", "nose>=1.0", "testfixtures>=4"],
    test_suite="tests",

    # PyPI information
    author="David Cuthbert",
    author_email="dacut@kanga.org",
    description="Meter the user of various resources",
    license="Apache",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    keywords=['metering', 'aws', 's3'],
    url="https://github.com/dacut/meterer",
    zip_safe=False,
)
