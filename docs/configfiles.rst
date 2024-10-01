.. _configuration-files:

*******************
Configuration Files
*******************

Every simulation needs a configuration file specifying the required and optional simulation parameters.
A configuration file is a simple YAML file in the simulation directory with the name ``simulation.yml``.

Adding a Configuration File
===========================
When you create a new simulation directory, a sample configuration file is created for you.
You have to adjust this file to your need (e.g. by specifying the file system to use) before you can start the simulation.
The sample configuration looks like this:

.. code-block:: yaml
   :linenos:

   ---
   seed: 9146254746101366880
   write_playbook: no

   file_system:
     type: FILE_SYSTEM_TYPE
     formatting_parameters: "FS_FORMATTING_PARAMETERS"

   volume:
     type: file
     keep: no
     size: IMAGE_SIZE

   usage_model:
     type: USAGE_MODEL
     parameters:
       # usage model parameters below, e.g.:
       # first_parameter: abc
       # second_parameter: 123


As you can see, the configuration file already contains some valid pre-generated values (such as the ``seed``),
while other values have to be replaced with valid values (e.g. the ``type``of the ``file system``).

Supported Settings
==================
The configuration file is validated

.. contents::
   :local:
   :depth: 3


seed
----
This specifies the seed for the random number generator.

A configuration file created by ``fsstratify init`` already contains this entry with a automatically generated value.

:Required: ``true``

Example:

.. code-block:: yaml

   seed: 12345


.. _cfg-write-playbook:

write_playbook
--------------
Specifies whether a playbook of the simulation should be written or not.
If it is set to a true value, the playbook will be written to the current simulation directory and its name will be ``simulation.playbook``.

:Required: ``false``
:Default: ``true``

Example:

.. code-block:: yaml

   write_playbook: no

.. warning::

   If you set this value to true and you are using a custom playbook, make sure that your playbook is not named ``simulation.playbook``.


file_system
-----------
The ``file_system``key groups options for the file system to use in the simulation.
While the ``type``, ``formatting_options``, and ``populate_with`` options are shared by all file systems, valid keys under ``formatting_options`` depend on the selected file system.


type
^^^^
Specifies the file system to use in the simulation. For a list of supported file systems, see TODO.

:Required: ``true``

Example:

.. code-block::

   file_system:
     type: ntfs


formatting_options
^^^^^^^^^^^^^^^^^^
Specifies file system dependent formatting options.
Supported options vary from file system to file system.

:Required: ``false``
:Default: n/a

Example:

.. code-block:: yaml

   file_system:
     type: ntfs
     formatting_options:
       label: my-label
       sector_size: 512


populate_with
^^^^^^^^^^^^^
Specifies a prepoluation dataset to put on the file system before the simulation starts.
For more information about prepopulation datasets see :ref:`prepopulation`.

:Required: ``no``
:Default: none

Example:

.. code-block:: yaml

   file_system:
     type: ntfs
     prepopulate_with:
       dataset: Windows10TODO.parquet
       mutable: true


dataset
"""""""
Specifies the dataset to use for pre-populating the file system.

:Required: ``yes``

TODO: datasets shipped with fsstratify and custom datasets
TODO: search order


mutable
"""""""
Specifies if the files of the prepopulation dataset can be used by operations during the simulation.
If this is set to ``false`` (the default), operations will never delete, move, or otherwise modify the
files of the dataset. New files can possibly be created in directories created by the dataset, though.
If ``mutable`` is set to ``true``, the files of the dataset will be treated just like files created
during the simulation.

:Required: ``no``
:Default: ``True``


volume
------
The ``volume`` key groups options for the volume to use in the simulation.


usage_model
-----------
The ``usage_model`` key groups options for the usage model to use in the simulation.


