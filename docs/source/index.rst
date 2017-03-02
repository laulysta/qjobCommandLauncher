Welcome to Smart Dispatch's documentation!
==========================================

Smart Dispatch is an easy to use job launcher for supercomputers with PBS compatible job manager.

Features
--------
  * Launch multiple jobs with a single line.
  * Automatically generate combinations of arguments. See :doc:`examples`.
  * Automatic resources management. Determine for you the optimal fit for your commands on nodes.
  * Resume batch of commands.
  * Easily manage logs.
  * Advanced mode for complete control.
  * Use automatic rescheduling of jobs that hit the walltime. See :doc:`autoresume`.


Installing
----------

Use ``pip`` package manager: ::

  pip install git+https://github.com/SMART-Lab/smartdispatch


Contents
--------

.. toctree::
  :maxdepth: 2

  usage
  examples
  autoresume


Indices and tables
==================

* :ref:`genindex`
* :ref:`search`
