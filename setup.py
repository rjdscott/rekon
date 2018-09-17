#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = ['Click>=6.0', 'pandas']

setup_requirements = [ ]

test_requirements = [ ]

setup(
    author="Rob Scott",
    author_email='rob@rjdscott.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description="rekon provides a suite of reconciliation tools for operations and finance",
    entry_points={
        'console_scripts': [
            'rekon=rekon.cli:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    package_data={
        # And include any *.dat files found in the 'data' subdirectory
        # of the 'rekon' package, also:
        'system1': ['docs/sample_data/system1.csv'],
        'system2': ['docs/sample_data/system2.csv'],
        'col_map': ['docs/sample_data/column_mapping.csv'],
        'row_map': ['docs/sample_data/row_mapping.csv'],
    },
    keywords='rekon',
    name='rekon',
    packages=find_packages(include=['rekon']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/rjdscott/rekon',
    version='0.1.1',
    zip_safe=False,
)
