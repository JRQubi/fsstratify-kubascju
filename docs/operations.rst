.. _operations:

**********
Operations
**********
Operations define what is done during a simulation. Simple examples for operations are creating a new file or
directory, copying or moving an existing file or deleting files.

This page describes how the various operations behave and how they are implemented. If you want to use them in
playbooks, have a look at the :ref:`playbook <playbooks>` documentation.

Copy
====
The ``Copy`` operation copies a source file or directory to a destination file or directory.

The source has to exists, otherwise an ``SimulationError`` is raised.

The following table shows the behavior of the operation. Note that unlike some command-line copy programs, such as
``cp`` on Linux and BSD, ``Copy`` automatically copies directories and their contents recursively.

.. list-table::
   :header-rows: 1

   * - Source
     - Target
     - Target Exists
     - Behavior
   * - file
     - file
     - no
     - source is copied to new file target
   * - file
     - file
     - yes
     - source overwrites existing file target
   * - file
     - directory
     - yes
     - source is copied into target directory
   * - directory
     - directory
     - yes
     - source directory (with its contents) is copied into target directory
   * - directory
     - file
     - yes
     - raises an error

When a file is copied, `shutil.copy <https://docs.python.org/3/library/shutil.html#shutil.copy>`_ of the Python
standard library is used; when a directory is copied,
`shutil.copytree <https://docs.python.org/3/library/shutil.html#shutil.copytree>`_ is used.

.. _extend:

Extend
======
This operation appends random data to an existing file. The file to extend has to exists, otherwise a
``SimulationError`` is raised.

The operation supports writing data in chunks of a given size as well as writing the data all in one chunk.
In the latter case, a maximal size of :math:`2^{28}-1` is supported for the data to append. If more data is requested,
``Extend`` will write it in chunks of :math:`2^{28}-1` bytes.


Mkdir
=====
``Mkdir`` create new directories.

The operation automatically creates missing parent directories. It raises a ``SimulationError`` if the target file
already exists.

The operation is implemented using `Path.mkdir <https://docs.python.org/3/library/pathlib.html#pathlib.Path.mkdir>`_
from the ``pathlib`` standard Python library.


Move
====
The ``Move`` operation moves a source file or directory to a destination file or directory.

The source has to exists, otherwise an ``SimulationError`` is raised.

The following table shows the behavior of the operation.

.. list-table::
   :header-rows: 1

   * - Source
     - Target
     - Target Exists
     - Behavior
   * - file
     - file
     - no
     - source is renamed to new file target
   * - file
     - file
     - yes
     - source overwrites existing file target
   * - file
     - directory
     - yes
     - source is moved into target directory
   * - directory
     - directory
     - yes
     - source directory (with its contents) is moved into target directory
   * - directory
     - file
     - yes
     - raises an error

Internally, the ``Move`` operation uses `shutil.move <https://docs.python.org/3/library/shutil.html#shutil.move>`_ of
the Python standard library.


Remove
======
``Remove`` deleted files and directories.

The file to remove has to exists, otherwise an ``SimulationError`` is raised.

The operation removes files and directories. If a directory to be removed contains files or other directories, these
will be deleted, too.

Internally, ``Remove`` uses `Path.unlink <https://docs.python.org/3/library/pathlib.html#pathlib.Path.unlink>`_ from
the Python standard ``pathlib`` library, when a file is removed. When an empty directory is removed,
`Path.rmdir <https://docs.python.org/3/library/pathlib.html#pathlib.Path.rmdir>`_ is used. Finally, when a non-empty
directory is removed, `shutil.rmtree <https://docs.python.org/3/library/shutil.html#shutil.rmtree>`_ is used.


Shrink
======
This operation shrinks an existing file by the given number of bytes.
The file to shrink must exists, otherwise a ``SimulationError`` is raised.


Write
=====
``Write`` creates new files of a given size. It can also be used to overwrite existing files.

The operation supports writing data in chunks of a given size as well as writing the data all in one chunk.
In the latter case, a maximal size of :math:`2^{28}-1` is supported for the data to append. If more data is requested,
the operation will write it in chunks of :math:`2^{28}-1` bytes.
