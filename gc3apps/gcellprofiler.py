#! /usr/bin/env python
#
#   gcellprofiler.py -- Run Cellprofiler in batch mode
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

import os
import json
import gc3libs
from gc3libs import Application
from gc3libs.workflow import StagedTaskCollection, \
    ParallelTaskCollection, SequentialTaskCollection
from gc3libs.quantity import Memory, kB, MB, MiB, GB, \
    Duration, hours, minutes, seconds

def __get_chunks(lenght, chunk_size):
    """
    Given a lenght, split the range into chunks of chunk_size
    """

    l = range(1,lenght,chunk_size)
    if (lenght % chunk_size) > 0:
        l.append(lenght)
    for i in range(0,len(l)-1):
        yield(l[i],l[i+1])


#####################
# StagedTaskCollection class
#

class GCellprofilerPipeline(StagedTaskCollection):
    """
    Staged collection:
    Step1: Generate groups .json file, used to get index size of batch images
    Step2: generate batch and run cellprofiler in batch mode for each batch
    """
    def __init__(self, batch_file, **extra_args):

        self.batch_file =  batch_file
        self.extra = extra_args

        StagedTaskCollection.__init__(self)
        
    def stage0(self):
        """
        Step 0: Generate groups .json file, used to get index size of batch images
        """

        extra_args = self.extra.copy()
        extra_args['jobname'] = "cp_get_groups"

        return RunCellprofilerGetGroups(self.batch_file,
                                        extra_args)

    def stage1(self):
        """
        Extract image batch size from .json group file
        Generate batch and run cellprofiler in batch mode for each batch
        """

        # Check exit status from previous step.
        rc = self.tasks[0].execution.returncode
        if rc is not None and rc != 0:
            return rc

        if not os.path.isfile(self.tasks[0].output):
            gc3libs.log.error("Stage0 group file {0} not found.".format(self.tasks[0].output))
            return

        with open(self.tasks[0].output) as json_file:
            data = json.load(json_file)
            batch_size = len(data)

        tasks = []
        for start,end in __get_chunks(batch_size, self.extra['chunk_size']):
            jobname = self.extra["jobname"]
            extra_args = self.extra.copy()
            extra_args['jobname'] = "cp_run_{0}-{1}".format(start,end)
            tasks.append(RunCellprofiler(self.batch_file,
                                         start,
                                         end,
                                         extra_args))
        return ParallelTaskCollection(tasks)
