.. _usage:

****************
Using fsstratify
****************

The following section describe how to initialize and run a file system simulation using fsstratify.

.. contents::
   :local:
   :depth: 3

Quickstart
==========
In general, running a simulation consists of the following steps:

#. Initializing the simulation directory
#. Adjusting the ``simulation.yml`` configuration file
#. Starting the simulation

The simulation directory is initialized using ``fsstratify init``:

.. code-block:: console

   $ fsstratify init my-simulation

Running the ``init`` command without like above creates the directory ``my-simulation`` and a default simulation
configuration in ``my-simulation/simulation.yml``.

Since the default configuration needs some values to be adjusted (e.g. the file system type to use during the
simulation), the next step is to modify the file to your needs.

Once this is done, you can start the simulation with ``fsstratify run``:

.. code-block:: console

   # fssstratify run my-simulation

or, if you changed into the ``my-simulation`` directory:

.. code-block:: console

   # fsstratify run .

.. warning::

   ``fsstratify run`` requires administrative privileges!


Initializing a Simulation Directory
===================================
An fsstratify simulation is defined by its *simulation directory*. It contains the configuration file, possibly
additional files to be used during the simulation (e.g. :ref:`prepopulation` datasets), and all output files will be
stored here, too. The same hold for temporary files created during the simulation. This makes it easy to redistribute
simualtions: simply create an archive of the simulation directory, send it to your colleague, and they will be able to
run the same simulation on their systems!

In its most simple form, a simulation directory is created using the ``init`` subcommand without any further arguments:

.. code-block:: console

   $ fsstratify init my-simulation

.. warning::

   The simulation directory must not exists yet!

This creates the directory ``my-simulation`` and populates it with some default files. Most importantly, it creates the
``simulation.yml`` file, which is the configuration file for the simulation. This file has to be adjusted before
running the simulation. Have a look at the :ref:`configuration-files` documentation for valid keys and values.

The ``--model`` option can be used to add a section with the given usage model and its parameters (cf.
:ref:`usage-models`). This makes creating a valid configuration easier as they typically have a lot of parameters, which
no one wants to write manually. For example, if you want to use the model ``KarresandEtAl``, you could initialize the
simulation directory like so:

.. code-block:: console

   $ fsstratify init --model KarresandEtAl my-simulation

Instead of the section

.. code-block:: yaml

   usage_model:
     type: USAGE_MODEL
     parameters:
       # usage model parameters below, e.g.:
       # first_parameter: abc
       # second_parameter: 123

which is present in the default configuration file, you would get the following ``usage_model`` section:

.. code-block:: yaml

   usage_model:
     type: KarresandModel
     parameters:
       # steps: Int()
       # size_factors: Seq(Int())
       # random_range_limit: Int()
       # chunk_size: Int()
       # write_weight: Float()
       # delete_weight: Float()
       # increase_weight: Float()
       # decrease_weight: Float()
       # write_start: Float()
       # write_stop: Float()
       # delete_start: Float()
       # delete_stop: Float()

TODO: fix the listing above

For a list of supported usage models, have a look at their :ref:`documentation <usage-models>` or run
``fsstratify init --help``.

fsstratify also ships with some simulation configurations, which replicate experiments and results presented in the
literature. For instance, to replicate the experiments conducted by Karresand et al. in their paper `An Empirical Study
of the NTFS Cluster Allocation Behavior Over Time <https://doi.org/10.1016/j.fsidi.2020.301008>`_, you could use the
following command:

.. code-block:: console

   $ fsstratify init --replicate KarresandEtAl2020 my-simulation

Again, this would create the ``my-simulation`` directory, but this time with a completely valid configuration file,
which has the parameters set to replicate the experiments by Karresand et al.

For a list of predefined configurations, run ``fsstratify init --help``.

TODO: add documentation page for predefined experiments

Besides the configuration file, ``fsstratify init`` also creates an empty directory named ``prepopulation_datasets``.
You can use this directory to provide additional :ref:`prepopulation`. Make sure to include these, when providing your
experiment to other people who want to run your experiments.


Starting a Simulation
=====================
Once a simulation directory is initialized and the configuration is customized, the simulation can be started. This is
done with the ``run`` subcommand:

.. code-block:: console

   # fsstratify run my-simulation

Note that you have to have administrative privileges to run simulations using fsstratify. This is because creating and
formatting file systems and volumes is prohibited for normal users on most platforms. We are working a solution, which
does not require the whole simulation to run with administrative privileges.

During the simulation a progess bar will inform you about the current state of the simulation. The remaining time
reported there is to be taken with a grain of salt, though.

If you want to cancel the simulation, simply hit ``Ctrl+C``.


Cleaning a Simulation Directory
===============================
If a simulation crashed (which hopefully doesn't happen) or if you want to restart a simulation, it is necessary to
clean the remnant of an old simulation. For this, fsstratify provides the ``clean`` subcommand. If you run it without
any further options, it will remove possibly existent simulation image files and the mount point:

.. code-block:: console

   $ fsstratify clean my-simulation

This is typically enough cleanup to re-run an old simulation.

If you want to distribute your simulation directory without any result and log files, use the ``--all`` flag. This
will remove all files, which are not necessary to run the simulation (i.e. the ``simulation.yml`` and files in
``prepopulation_datasets`` are left untouched).

.. code-block:: console

   $ fsstratify clean --all my-simulation
