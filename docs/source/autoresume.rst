====================
Automatic resumption
====================

Oftentimes, there is a hard limit on maximum amount of time you can run your
job for on the cluster (we refer to it as **walltime**). Smart Dispatch allows you
to partially overcome that and run your tasks for longer periods. This is done
by enchancing generated PBS files with additional code that reschedules your
tasks as soon as they hit the walltime. The caveat here is that your tasks
**must be resumable**, i.e. be capable of restoring their state after being
killed and rerun.

You can engage the autoresumption by passing ``-m`` or ``--autoresume`` during
``smart-dispatch`` execution. See :doc:`usage` for details. 
