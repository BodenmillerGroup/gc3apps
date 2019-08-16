#! /usr/bin/env python
#
#   gqtl_pipeline.py -- Run Cellprofiler in batch mode
#
#   Copyright (c) 2019, 2020 S3IT, University of Zurich, http://www.s3it.uzh.ch/
#
#   This program is free software: you can redistribute it and/or
#   modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

# summary of user-visible changes
__changelog__ = """
  2019-04-10:
  * adapted CLI interface
  2019-03-13:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'
__version__ = '1.0.2'

# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gqtl_pipeline
    gqtl_pipeline.GQTLScript().run()

import os
import json
import gc3apps
import gc3libs
from gc3libs import Application
from gc3apps import QTLApplication
from gc3libs.cmdline import SessionBasedScript, existing_file, \
    positive_int, existing_directory, nonnegative_int

BATCH_THRESHOLD = 1000


class GQTLScript(SessionBasedScript):
    """
    The ``gqtl_pipeline`` command keeps a record of jobs (submitted, executed
    and pending) in a session file (set name with the ``-s`` option); at
    each invocation of the command, the status of all recorded jobs is
    updated, output from finished jobs is collected, and a summary table
    of all known jobs is printed.
    
    Options can specify a maximum number of jobs that should be in
    'SUBMITTED' or 'RUNNING' state; ``gfittingaddm`` will delay submission of
    newly-created jobs so that this limit is never exceeded.

    Once the processing of all chunked files has been completed, ``gfittingaddm``
    aggregates them into a single larger output file located in 
    'self.params.output'.
    """

    def __init__(self):
        SessionBasedScript.__init__(
            self,
            version = __version__,
            application = Application,
            stats_only_for = Application,
            )

    def setup_options(self):
        self.add_param("-d", "--data", metavar="DIRECTORY",
                       type=existing_directory,
                       dest="data", default=os.getenv("PWD"),
                       help="Location of input/output data. " \
                       "Default: '%(default)s'.")

        self.add_param("-b", "--batches", metavar="NUM",
                       type=positive_int,
                       dest="batches", default=1000,
                       help="Number of permutation batches written to individual files. " \
                       "Default: '%(default)s'.")

        self.add_param("-p", "--permutations", metavar="NUM",
                       type=positive_int,
                       dest="permutations", default=100,
                       help="Number of permutations per batch (file). " \
                       "Default: '%(default)s'.")

        self.add_param("-i", "--imputations", metavar="NUM",
                       type=positive_int,
                       dest="imputations", default=10,
                       help="Number of imputations (forests) per permutation. " \
                       "Default: '%(default)s'.")

        self.add_param("-t", "--trees", metavar="NUM",
                       type=positive_int,
                       dest="trees", default=1000,
                       help="Number of trees per imputation (forest). " \
                       "Default: '%(default)s'.")

        self.add_param("--mafthres", metavar="NUM",
                       type=float,
                       dest="mafthres", default=0.9,
                       help="Major allele frequency (MAF) theshold. " \
                       "Default: '%(default)s'.")

        self.add_param("--last", metavar="NUM",
                       type=nonnegative_int,
                       dest="last", default=0,
                       help="Last permutation batch number. " \
                       "Default: '%(default)s'.")

        self.add_param("--version", metavar="VERSION",
                       type=str,
                       dest="version", default="1.1.0",
                       help="Docker version to be used. " \
                       "Default: '%(default)s'.")


    def parse_args(self):
    #     assert self.params.last < self.params.batches, "Last {0} cannot be higher than the whole batch {1}.".format(self.params.last,
    #                                                                                                                 self.params.batches)
    #
    #     # if `last` provided, skip the first `last` from batches
    #     self.params.batches -= self.params.last
        assert self.params.batches % BATCH_THRESHOLD == 0

    def new_tasks(self, extra):
        tasks = []
        for phenotype in self.params.args:
            for batch in range(0, (self.params.batches / BATCH_TRESHOLD)):
                extra_args = extra.copy()
                extra_args['jobname'] = "{0}_batch_{1}".format(phenotype,
                                                               batch)
                extra_args['output_dir'] = os.path.abspath(self.params.output.replace('NAME',
                                                                                      extra_args['jobname']))
                tasks.append(QTLApplication(phenotype,
                                            os.path.abspath(self.params.data),
                                            BATCH_TRESHOLD,
                                            self.params.permutations,
                                            self.params.imputations,
                                            self.params.trees,
                                            self.params.mafthres,
                                            self.params.last + (batch * BATCH_TRESHOLD),
                                            self.params.version,
                                            **extra_args))
        return tasks
