.. _prepopulation:

**********************
Prepopulation Datasets
**********************
Prepopulation datasets can be used to create files and directories on a file system before the actual simulation starts.
One main use case for these datasets is to create the basic file hierarchy structure of an operating system installation
on the file system. Due to copyright issues, it is typically not possible to distribute the original files copied to
disk during the installation of an operating system. Yet, using a prepopulation dataset, we can still create the same
directory structure and files of the same size, but with artificial content.

Other use cases for prepopulation datasets are simulations where the behavior of a file system already filled near to
capacity shall be evaluated.

Prepopulation datasets are basically just `Parquet files <https://parquet.apache.org/>`_ listing the directories and
files with their corresponding sizes. When a file system is prepopulated with a dataset, these files and directories
are created using the ``mkdir`` and ``create`` operations of fsstratify. The content of all files created during the
prepopulation phase is always the character ``X`` repeated until the requried file size is reached.
This is to make it at least somewhat easier to identify prepolulated areas in the file system.

.. contents::
   :local:
   :depth: 3

Using Prepoluation Datasets
===========================


Creating Prepolulation Datasets
===============================
fsstratify ships with the ``preserve`` command to create prepoluation datasets. Currently, ``preserve`` has one
sub-command named ``files``, which creates a datasets based on the files and folders in a given directory.


Example:

.. code-block:: console

   $ fsstratify perserve files /mnt/win10_files

This command would recursively traverse the directories under ``/mnt/win10_files`` and record all directories and files
with their names and sizes. The resulting dataset will be written to ``ẁin10_files.parquet``.
If you want a different name, use the ``--outfile`` argument:

.. code-block:: console

   $ fsstratify perserve files --outfile win10.parquet /mnt/win10_files

This will preserve the same file, but the resulting dataset will be named `win10.parquet``.

