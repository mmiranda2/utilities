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
            'record_audio=utilities.audio:record',
        ]
    },
    install_requires=[
        'pyaudio',
        'requests',
        'stem',
        'wave'
    ],
    python_requires='~=3.9'
)