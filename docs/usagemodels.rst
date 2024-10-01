.. _usage-models:

************
Usage Models
************

Usage models define the operations carried out during the simulation.
In each simulation step the usage model generated an operation to be carried out and the execution environment executes
this operation on the given file system.

There are usage models, which try to model some kind of user behavior, but there are also models, which simply generate
operations randomly.

The following sections document the currently available usage models.

.. contents::
   :local:
   :depth: 3

Playbook
========
The Playbook model is kind of a special usage model as it does not generate operations on the fly, but reads them
from an existing playbook file.
The main purpose of this model is to be able to execute a specific and fixed sequence of operations on a file system.
This can be either a hand-crafted playbook file or a playbook generated from a previous simulation run with a different
usage model (cf. the :ref:`cfg-write-playbook` option in the :ref:`configuration-files` documentation).

If you want to write a playbook manually, have a look at the TODO documentation.

The Playbook model is selected by specifying ``Playbook`` as the usage model in the configuration file:

.. code-block:: yaml

   usage_model:
     type: Playbook

The playbook has to be a file named ``playbook`` in the simulation directory.

There are no configuration options for this model.

The playbook file is validated before the simulation is started.
That is, fsstratify checks if all lines are valid playbook commands.
It is not checked if the command make sense (e.g. if there is a delete command for a non-existing file), though.
Only after that, the simulation is started. For very long playbooks, this validation may take some time.


Probabilistic
=============
This model generates operations randomly with equal weights for all operations.

Select the Probabilistic model by specifying ``Probabilistic`` as the usage model in the configuration file:

.. code-block:: yaml

   usage_model:
     type: Probabilistic
     steps: 10000
     file_size_min: 50 M
     file_size_max: 1 G

The parameters ``file_size_min`` and ``file_size_max`` define the minimal and maximal file size of files to create on
the file system. Note that these values define the size range for new files and influence the :ref:`extend` operation,
too. When extending an existing file, the ``file_size_max`` is never exceeded.

As mentioned before, all valid operations are chosen with equal probability. Note, however, that there are situations,
where not all operations are valid. In such cases, invalid operations will never be chosen. The following table shows
when which operations are valid.

.. list-table::
   :header-rows: 1

   * - File system contains
     - Valid operations
     - Invalid operations
   * - nothing
     - ``mkdir``, ``write``
     - ``cp``, ``extend``, ``mv``, ``rm``
   * - only directories
     - ``mkdir``, ``cp``, ``mv``, ``rm``, ``write``
     -  ``extend``
   * - only files
     - ``mkdir``, ``cp``, ``extend``, ``mv``, ``rm``, ``write``
     - `none`
   * - files and directories
     - ``mkdir``, ``cp``, ``extend``, ``mv``, ``rm``, ``write``
     - `none`

Moreover, the model ensures that no operations are generated, which would fill the file system beyond capacity.

When a file is copied, it creates either a new file or overwrites an existing file with equal probability.
The same holds for a file that is moved. It is ensured that invalid copy or move operations are never generated (e.g.
a directory overwriting a file).

steps
~~~~~
Number of operations to execute during the simulation.

:Required: ``true``


file_size_min
~~~~~~~~~~~~~
The minimal size of files on the file system.

:Required: ``true``


file_size_max
~~~~~~~~~~~~~
The maximal size of files on the file system.

:Required: ``true``


KAD
===
KAD stands for Karresand, Axelson, Dyrkolbotn. This model implements the experiments
the authors described in their publications
"`Using NTFS Cluster Allocation Behavior to Find the Location of User Data <https://doi.org/10.1016/j.diin.2019.04.018>`_" and
"`An Empirical Study of the NTFS Cluster Allocation Behavior Over Time <https://doi.org/10.1016/j.fsidi.2020.301008>`_".

Select this model by selected by specifying ``KAD`` as the usage model in the configuration file:

.. code-block:: yaml

   usage_model:
     type: KAD
       parameters:
         steps: 10000
         size_factors:
           - size: 8
             weight: 1
           - size: 2048
             weight: 1
         operation_factors:
           write: 10
           delete: 9
           increase: 11
           decrease: 10
         random_range:
           min: 1
           max: 1024
         chunk_size: 512
         write_limit:
           start: 0.05
           stop: 0.3
         delete_limit:
           start: 0.95
           stop: 0.7

The parameters shown above reproduce the experiments of the paper
"`Using NTFS Cluster Allocation Behavior to Find the Location of User Data <https://doi.org/10.1016/j.diin.2019.04.018>`_".

The model uses only four file operations: write, delete, increase and decrease.

* write: creates a new file
* delete: deletes an existing file
* increase: extends an existing file
* decrease: shrinks an existing file

Directories are not considered in this model.

An operation to perform is randomly chosen from the list above. Each operation is
associated with what is called a bias in the original publication. The bias is
basically a relative weight which determines the probability of the operation to be
selected. These weights are configured using the ``operation_factors`` key in the
parameters section of the model.

The file sizes are controlled using the parameters ``size_factors``, ``chunk_size``
and ``random_range``. When a new file is created its size is determined by choosing
a random number from the list of weighted factors defined under ``size_factors``.
In the example above, there are two size factors, 8 and 2048. Both have an equal
weight, which means that the chances for choosing one or the other are 50:50.
After having a size factor, it is multiplied with the ``chunk_size`` value and
in turn with a random value from ``random_range``. To sum things up: let `f` be
the randomly chosen size factor from ``size_factors``, `r` a randomly chosen value
from ``random_range`` and `s` the ``chunk_size``,  then the file size is computed
as `f` * `r` * `s`.

When increasing of decreasing the size of a file, the number of bytes to add or
remove is computed just as the size for new files. That is, the number of bytes
to add or remove is the result of `f` * `r` * `s`. Note that when decreasing
the size of a file, the model ensures that at least one block of the size of
``chunk_size`` remains. As a consequence, the ``random_range`` might be capped
implicitly to fulfill this constraint.

All operations writing data (i.e. writing and extending files) use chunked
writing. That is, data is written in chunks of ``chunk_size`` bytes of size
(cf. `Using NTFS Cluster Allocation Behavior to Find the Location of User Data <https://doi.org/10.1016/j.diin.2019.04.018>`_).

Finally, the model uses write and delete limits, which control the file system usage
based on the free space. Both define ranges (via the ``start`` and ``stop`` keys) of the
capacity of the file system that define when only write or delete operations are generated.

For example, when the ``write_limit`` is set to ``start`` = 0.05 and ``stop`` = 0.3, then
the model will start to generate only write operations as soon as the used space on the
file system is < 5 % (`start` value) of the overall capacity. It will continue to generate only write
operations until the file system is 30 % filled (`stop` value). Similarly, when the ``start``
and ``stop`` values for ``delete_limit`` are set to 0.95 and 0.7, then the model will
start to generate only delete operations as soon as the file system is 95 % filled. It will
continue to do so, until there are at least 30 % of free space available again.
Note that the start limits exclude the exact value, while the stop limits include this value.
This reflects the paper text (*"If the current amount of data [...] falls outside of the start
limit multiplied with the total size [...] it triggers write or delete operations until the
stop limit multiplied with the total size is reached."*)

If you don't want these limits in your simulations, you can turn them off by setting the
start value of ``write_limit`` to 0 and the start value of ``delete_limit`` to 1.

steps
~~~~~
Number of operations to execute during the simulation.

:Required: ``true``


size_factors
~~~~~~~~~~~~
Used to compute the size of new files or the number of bytes when extending or shrinking files.
The computation is `size factor` * `random number` * `chunk size`. The `size factor` comes from
the weighted list ``size_factors``. See above for more details.

:Required: ``true``


random_range
~~~~~~~~~~~~
Used to compute the size of new files or the number of bytes when extending or shrinking files.
The computation is `size factor` * `random number` * `chunk size`. The `random number` comes from
the range ``random_range`` with the start end end value included. See above for more details.

:Required: ``true``


chunk_size
~~~~~~~~~~
Used to compute the size of new files or the number of bytes when extending or shrinking files.
The computation is `size factor` * `random number` * `chunk size`. See above for more details.

:Required: ``false``
:Default: ``512``


operation_biases
~~~~~~~~~~~~~~~~
The keys under ``operation_biases`` define the relative weights of the different operation.

write_weight
^^^^^^^^^^^^
The relative weight of a write operation during the selection of a file operation.

:Required: ``true``


delete_weight
^^^^^^^^^^^^^
The relative weight of a delete operation during the selection of a file operation.

:Required: ``true``


increase_weight
^^^^^^^^^^^^^^^
The relative weight of an increase operation during the selection of a file operation.

:Required: ``true``


decrease_weight
^^^^^^^^^^^^^^^
The relative weight of a decrease operation during the selection of a file operation.

:Required: ``true``


write_limit
~~~~~~~~~~~
Defines when the model start and stops to generate only delete operations.

start
^^^^^
Defines the file system usage ratio when the model starts to generate only write
operations.

:Required: ``true``


stop
^^^^
Defines the file system usage ratio when the model stops to generate only write
operations.

:Required: ``true``


delete_limt
~~~~~~~~~~~

Defines when the model start and stops to generate only delete operations.


start
^^^^^
Defines the file system usage ratio when the model starts to generate only delete
operations.

:Required: ``true``


stop
^^^^
Defines the file system usage ratio when the model stops to generate only delete
operations.

:Required: ``true``
