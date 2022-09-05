# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name='utilities',
    version='0.1',
    maintainer='Michael',
    url='https://github.com/mmiranda2/utilities',
    description="Utility functions",
    long_description="Helper and utility functions to streamline common tasks",
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'abacus_clean_tables=abacus.clean_tables:main',
        ]
    },
    install_requires=[
        'requests'
    ],
    python_requires='~=3.7'
)