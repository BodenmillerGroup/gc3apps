.. Hey Emacs, this is -*- rst -*-

   This file follows reStructuredText markup syntax; see
   http://docutils.sf.net/rst.html for more information.

.. include:: ../../global.inc


.. _gqtl:

The `gqtl`:command: script
=============================

GC3apps provide a script drive execution of multiple ``CellineQTL``
jobs. It uses the generic `gc3libs.cmdline.SessionBasedScript`
framework.

``CellineQTL`` could be found at: https://github.com/BodenmillerGroup/celllineQTL.git


Introduction
------------

`gqtl`:command: driver script takes a list of Phenotype names and for
each and for each of them executes ``CellineQTL`` as independent
jobs. Job progress is monitored and, when a job is done, its output
files are retrieved back into the simulation directory itself.

The ``gqtl`` command keeps a record of jobs (submitted, executed
and pending) in a session file (set name with the ``-s`` option); at
each invocation of the command, the status of all recorded jobs is
updated, output from finished jobs is collected, and a summary table
of all known jobs is printed.  New jobs are added to the session if
new input files are added to the command line.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; ``gqtl`` will delay submission of
newly-created jobs so that this limit is never exceeded.

Options can specify a maximum number of jobs that should be in
'SUBMITTED' or 'RUNNING' state; `gqtl`:command: will delay submission
of newly-created jobs so that this limit is never exceeded.

In more detail, `gqtl`:command: does the following:
   
1. Reads the `session`:term: (specified on the command line with the
   ``--session`` option) and loads all stored jobs into memory.
   If the session directory does not exist, one will be created with
   empty contents.
   
2. For each Phenotype name runs a separate job. Each job is executed
   within a QTL Docker container references as ``bblab/qtl``

3. Updates the state of all existing jobs, collects output from
   finished jobs, and submits new jobs generated in step 2.

4. For each of the terminated jobs, a post-process routine is executed
   to check and validate the consistency of the generated output. If
   no ``_SUCCESSFUL_RUN`` or ``_FAILED_RUN`` file is found, the
   related job will be resubmitted together with the current input and
   output folders. 

   Finally, a summary table of all known jobs is printed.  (To control
   the amount of printed information, see the ``-l`` command-line
   option in the `session-based script`:ref: section.)

4. If the ``-C`` command-line option was given (see below), waits
   the specified amount of seconds, and then goes back to step 3.

   The program `gqtl`:command: exits when all jobs have run to
   completion, i.e., when all valid input folders have been computed.

Execution can be interrupted at any time by pressing :kbd:`Ctrl+C`.
If the execution has been interrupted, it can be resumed at a later
stage by calling `gqtl`:command: with exactly the same
command-line options.

Command-line invocation of `gqtl`:command:
----------------------------------------------

The `gqtl`:command: script is based on GC3Pie's `session-based
script <session-based script>`:ref: model; please read also the
`session-based script`:ref: section for an introduction to sessions
and generic command-line options.

A `gqtl`:command: command-line is constructed as follows:

1. Each argument (at least one should be specified) is considered as a
   Phenotype name.

**Example 1.** The following command-line invocation uses
`gqtl`:command: to run on 2 Phenotype::

   $ gqtl p-p90RSK_iMEK_9 p-p90RSK_EGF_9 --data /glusterfs/celllineQTL/data/marcotteGenotype_marcoCyTOF/ -o qtl_out

**Example 2.**
::
   
   $ gqtl p-p90RSK_iMEK_9 p-p90RSK_EGF_9 --data
   /glusterfs/celllineQTL/data/marcotteGenotype_marcoCyTOF/ -o
   qtl_out --scores 2 --permutations 2 -q 1.0.6 -c 4 -C 10

In this example, job information is stored into session
``gqtl`` (see the documentation of the ``--session`` option
in `session-based script`:ref:).  The command above creates the jobs,
submits them, and finally prints the following status report::

  Status of jobs in the 'gqtl' session: (at 10:53:46, 02/28/12)
  NEW   0/2    (0.0%)  
  RUNNING   0/2    (0.0%)  
  STOPPED   0/2    (0.0%)  
  SUBMITTED   2/2   (100.0%) 
  TERMINATED   0/2    (0.0%)  
  TERMINATING   0/2    (0.0%)  
  total   2/2   (100.0%) 

Calling `gqtl`:command: over and over again will result in the same jobs
being monitored; 

The ``-C`` option tells `gqtl`:command: to continue running until
all jobs have finished running and the output files have been
correctly retrieved.

Each job will be named after the Phenotype name.

For each job, the set of output files is automatically retrieved and
placed in the locations described below.

For more details on the ``gqtl`` options use::
      $ gqtl --help

Using GC3Pie utilities
----------------------

GC3Pie comes with a set of generic utilities that could be used as a
complement to the `gqtl`:command: command to better manage a entire
session execution.

https://github.com/uzh/gc3pie/blob/master/docs/users/gc3utils.rst

