#! /usr/bin/env python
#
#   gcp_pipeline.py -- Run Cellprofiler in batch mode
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
    import gcp_pipeline_batch
    gcp_pipeline_batch.GCellprofilerPipelineScriptWithBatchFile().run()

import os
import json
import gc3apps
import gc3libs
from gc3libs import Application
from gc3apps.apps import RunCellprofiler, \
    RunCellprofilerGetGroupsWithBatchFile
from gc3libs.workflow import StagedTaskCollection, \
    ParallelTaskCollection, SequentialTaskCollection
from gc3libs.quantity import Memory, kB, MB, MiB, GB, \
    Duration, hours, minutes, seconds
from gc3libs.cmdline import SessionBasedScript, existing_file, \
    positive_int, existing_directory

def __group_by_limit(li, limit):
    """
    Helper:
    Concatenates consecutive sublists of a list
    until a size limit has been reached
    """
    if not li:
        return []
    out = [[]]
    for sublist in li:
        if len(out[-1]) < limit:
            out[-1].extend(sublist)
        else:
            out.append(sublist[:])

    # check for the last element length
    if len(out) > 1 and len(out[-1]) < limit:
        out[-2].extend(out.pop())

    return out


def _get_chunks(data, chunk_size):
    """
    Get chunk of images based on the chunk_size
    """
    images_list = [element[1] for element in data]
    chunks_list = __group_by_limit(images_list, chunk_size)
    for chunk in chunks_list:
        yield(chunk[0],chunk[-1])


#####################
# StagedTaskCollection class
#

class GCellprofilerPipelineWithBatchFile(StagedTaskCollection):
    """
    Staged collection:
    Step1: Generate groups .json file, used to get index size of batch images
    Step2: generate batch and run cellprofiler in batch mode for each batch
    """
    def __init__(self, batch_file, output_folder, chunks, plugins, **extra_args):

        self.batch_file = batch_file
        self.output_folder = output_folder
        self.chunks = chunks
        self.plugins = plugins
        self.extra = extra_args

        StagedTaskCollection.__init__(self)
        
    def stage0(self):
        """
        Step 0: Generate groups .json file, used to get index size of batch images
        """

        extra_args = self.extra.copy()
        extra_args['jobname'] = "cp_get_groups"
        extra_args['output_dir'] = os.path.join(extra_args['output_dir'],
                                                extra_args['jobname'])

        return RunCellprofilerGetGroupsWithBatchFile(self.batch_file,
                                        **extra_args)

    def stage1(self):
        """
        Extract image batch size from .json group file
        Generate batch and run cellprofiler in batch mode for each batch
        """

        # Check exit status from previous step.
        rc = self.tasks[0].execution.returncode
        if rc is not None and rc != 0:
            return rc

        with open(os.path.join(self.tasks[0].output_dir,
                              self.tasks[0].stdout)) as json_file:
            data = json.load(json_file)

        tasks = []
        for start,end in _get_chunks(data, self.chunks):
            jobname = self.extra["jobname"]
            extra_args = self.extra.copy()
            extra_args['jobname'] = "cp_run_{0}-{1}".format(start,end)
            extra_args['output_dir'] = os.path.join(extra_args['output_dir'],
                                                    extra_args['jobname'])
            output_folder_batch = os.path.join(self.output_folder,"output_{0}-{1}".format(start,end))
            if not os.path.exists(output_folder_batch):
                gc3libs.log.debug("Creating new batch folder at {0}.".format(output_folder_batch))
                os.makedirs(output_folder_batch)
                os.chmod(output_folder_batch, 0777)
            tasks.append(RunCellprofiler(self.batch_file,
                                         output_folder_batch,
                                         start,
                                         end,
                                         self.plugins,
                                         **extra_args))
        return ParallelTaskCollection(tasks)


class GCellprofilerPipelineScriptWithBatchFile(SessionBasedScript):
    """
    The ``gcp_pipeline`` command keeps a record of jobs (submitted, executed
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

    def setup_args(self):
        self.add_param('batch_file', type=existing_file,
                       help="Cellprofiler batch file in .h5 format")

        self.add_param("output_folder", type=str, help="Location of the results.")
    

    def setup_options(self):
        self.add_param("-K", "--chunks", metavar="[INT]",
                       type=positive_int,
                       dest="chunks", default=100,
                       help="Chunk size for each batch run. Default: '%(default)s'.")

        self.add_param("-P", "--plugins", metavar="[PATH]",
                       dest="plugins", default="$HOME",
                       help="Location of Cellprofiler plugins. Default: '%(default)s'.")

        self.add_param("--docker", metavar="[IMAGE NAME]",
                       type=str,
                       dest="docker_image",
                       help="Docker image that runs the gcp pipeline.")

    def parse_args(self):
	"""
	Declare command line arguments.
	"""
	self.params.batch_file = os.path.abspath(self.params.batch_file)
	self.params.output_folder = os.path.abspath(self.params.output_folder)

    def new_tasks(self, extra):
        """
        Chunk initial input file
        For each chunked fule, generate a new GfittingaddmTask
        """

        extra_args = extra.copy()
        extra_args['jobname'] = os.path.basename(self.params.batch_file)

        extra_args['output_dir'] = os.path.join(os.path.abspath(self.session.path),
                                                '.compute',
                                                extra_args['jobname'])
        extra_args['docker_image'] = self.params.docker_image

        return [GCellprofilerPipelineWithBatchFile(self.params.batch_file,
                                      self.params.output_folder,
                                      self.params.chunks,
                                      self.params.plugins,
                                      **extra_args)]
