#! /usr/bin/env python
#
#   gqtl_pipeline.py -- Run Cellprofiler in batch mode
#
#   Copyright (c) 2018, 2019 S3IT, University of Zurich, http://www.s3it.uzh.ch/
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
"""
Step1: Generate groups .json file, used to get index size of batch images
Step2: generate batch and run cellprofiler in batch mode for each batch
"""

# summary of user-visible changes
__changelog__ = """
  2018-09-13:
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
from gc3apps.apps import QTLApplication
from gc3libs.quantity import Memory, kB, MB, MiB, GB, \
    Duration, hours, minutes, seconds
from gc3libs.cmdline import SessionBasedScript, existing_file, \
    positive_int, existing_directory

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
                       help="Location of input data." \
                       "Default: '%(default)s'.")

        self.add_param("-f", "--forests", metavar="NUM",
                       type=positive_int,
                       dest="forests", default=10,
                       help="number of independent imputations." \
                       "Default: '%(default)s'.")

        self.add_param("-t", "--trees", metavar="NUM",
                       type=positive_int,
                       dest="trees", default=1000,
                       help="total number of trees numForests X numTrees. " \
                       "Default: '%(default)s'.")

        self.add_param("-S", "--scores", metavar="NUM",
                       type=positive_int,
                       dest="scores", default=1000,
                       help="number of permutation batches written to individual files." \
                       "Default: '%(default)s'.")

        self.add_param("-p", "--permutations", metavar="NUM",
                       type=positive_int,
                       dest="permutations", default=100,
                       help="total number of permutations: numPermutedScoresFiles times numPermutations." \
                       "Default: '%(default)s'.")

        self.add_param("-t", "--threshold", metavar="NUM",
                       type=float,
                       dest="threshold", default=0.9,
                       help="only consider markers with less than n strains for the major allele." \
                       "Default: '%(default)s'.")

        self.add_param("-q", "--qtl_version", metavar="VERSION",
                       type=str,
                       dest="qtl_version", default="latest",
                       help="What QTL version should be used." \
                       "Default: '%(default)s'.")

        
    def new_tasks(self, extra):
        """
        """
        tasks = []

        for phenotypeName in self.params.args:
        
            extra_args = extra.copy()
            extra_args['jobname'] = phenotypeName

            extra_args['output_dir'] = os.path.join(os.path.abspath(self.session.path),
                                                    '.compute',
                                                    extra_args['jobname'])
            tasks.append(QTLApplication(phenotypeName,
                                        os.path.abspath(self.params.data),
                                        self.params.forests,
                                        self.params.trees,
                                        self.params.scores,
                                        self.params.permutations,
                                        self.params.threshold,
                                        self.params.qtl_version,
                                        **extra_args))
        return tasks


