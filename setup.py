# -*- coding: utf-8 -*-
from distutils.core import setup

setup(
    name='smart-dispatch',
    version='2.0.1',
    author='Stanislas Lauly, Marc-Alexandre Côté, Mathieu Germain',
    author_email='smart-udes-dev@googlegroups.com',
    packages=['smartdispatch',
              'smartdispatch/workers'],
    scripts=['scripts/smart-dispatch',
             'scripts/sd-launch-pbs'],
    url='https://github.com/SMART-Lab/smartdispatch',
    license='LICENSE.txt',
    description='An easy to use job launcher for supercomputers with PBS compatible job manager.',
    long_description=open('README.md').read(),
    install_requires=['psutil>=1'],
    extras_require={
        'doc': ['sphinxcontrib-autoprogram>=0.1.3']
    },
    package_data={'smartdispatch': ['config/*.json']}
)
