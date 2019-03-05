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
    import gcp_pipeline
    gcp_pipeline.GCellprofilerPipelineScript().run()

import glob
import os
import json
import shutil
import gc3apps
import gc3libs
from gc3libs import Application
from gc3apps.apps import RunCellprofiler, \
    RunCellprofilerGetGroups
from gc3libs.workflow import StagedTaskCollection, \
    ParallelTaskCollection, SequentialTaskCollection
from gc3libs.quantity import Memory, kB, MB, MiB, GB, \
    Duration, hours, minutes, seconds
from gc3libs.cmdline import SessionBasedScript, existing_file, \
    positive_int, existing_directory

def _get_chunks(lenght, chunk_size):
    """
    Given a lenght, split the range into chunks of chunk_size
    """

    chunks = range(1,lenght+1,chunk_size)
    chunks.append(lenght+1)
    for i in range(0,len(chunks)-1):
        yield(chunks[i],chunks[i+1]-1)


def _copy_cp_file(path_source, fol_source, fol_target):
    """
    Copies a file from a source folder in a target folder, preserving the subfolder
    structure.
    If the file exists already, it is not overwritten but a warning is printed.
    If the file exists already and is a .csv file, it will be appended to the existing .csv
    without header
    
    Input:
        path_source: the full path to the source file
        fol_source: the base folder of the source file
        fol_target: the target folder 
    Output:
        True: if copied/appended
        False: if not copied
    """
    CSV_SUFFIX = '.csv'
    
    fn_source_rel = os.path.relpath(path_source,fol_source)
    path_target = os.path.join(fol_target, fn_source_rel)
    if os.path.exists(path_target):
        if path_source.endswith(CSV_SUFFIX):
            with open(path_target, 'ab') as outfile:
                with open(path_source, 'rb') as infile:
                    infile.readline()  # Throw away header on all but first file
                    # Block copy rest of file from input to output without parsing
                    shutil.copyfileobj(infile, outfile)
                    print(path_source + " has been appended.")
            return True
        else:
            print('File: ', path_target, 'present in multiple outputs!')
            return False
    else:
        subfol = os.path.dirname(path_target)
        if not os.path.exists(subfol):
            # create the subfolder if it does not yet exist
            os.makedirs(os.path.dirname(path_target))
        shutil.copy(path_source, path_target)
        return True
        
def _combine_directories(fols_input, fol_out):
    """
    Combines a list of cellprofiler ouput directories into one output
    folder.
    This .csv files present in multiple output directories are appended
    to each other, ignoring the header. Other files present in multiple directories
    are only copied once.
    Input:
        fols_input: list of cp ouput folders
        fol_out: folder to recombine the output folders into
    """
    for d_root in fols_input:
        for dp, dn, filenames in os.walk(d_root):
            for f in filenames:
                subfol = os.path.relpath(dp, start=d_root)
                _copy_cp_file(path_source=os.path.join(dp, f), fol_source=d_root, fol_target=fol_out)

#####################
# StagedTaskCollection class
#

class GCellprofilerPipeline(StagedTaskCollection):
    """
    Staged collection:
    Step1: Generate groups .json file, used to get index size of batch images
    Step2: generate batch and run cellprofiler in batch mode for each batch
    """
    def __init__(self, cppipe, input_folder, output_folder, chunks, plugins, **extra_args):

        self.cppipe =  cppipe
        self.input_folder = input_folder
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

        return RunCellprofilerGetGroups(self.cppipe,
                                        self.input_folder,
                                        self.plugins,
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

        assert os.path.isfile(self.tasks[0].json_file), "Stage0 group file not found."        

        with open(self.tasks[0].json_file) as json_file:
            data = json.load(json_file)
            batch_size = len(data)

        batch_file = self.tasks[0].batch_file

        tasks = []
        for start,end in _get_chunks(batch_size, self.chunks):
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
            tasks.append(RunCellprofiler(batch_file,
                                         output_folder_batch,
                                         start,
                                         end,
                                         self.plugins,
                                         **extra_args))
        return ParallelTaskCollection(tasks)


class GCellprofilerPipelineScript(SessionBasedScript):
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
        self.add_param('cppipe', type=existing_file,
                       help="Cellprofiler pipeline file")
        
        self.add_param('input_folder', type=str,
                       help="Location of the root folder containing the images")
        
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
	self.params.cppipe = os.path.abspath(self.params.cppipe)
	self.params.input_folder = os.path.abspath(self.params.input_folder)
	self.params.output_folder = os.path.abspath(self.params.output_folder)

    def new_tasks(self, extra):
        """
        Chunk initial input file
        For each chunked fule, generate a new GfittingaddmTask
        """

        extra_args = extra.copy()
        extra_args['jobname'] = os.path.basename(self.params.cppipe)

        extra_args['output_dir'] = os.path.join(os.path.abspath(self.session.path),
                                                '.compute',
                                                extra_args['jobname'])
        extra_args['docker_image'] = self.params.docker_image

        return [GCellprofilerPipeline(self.params.cppipe,
                                      self.params.input_folder,
                                      self.params.output_folder,
                                      self.params.chunks,
                                      self.params.plugins,
                                      **extra_args)]

    def after_main_loop(self):
        glob_infols = 'output_*'
        fol_out = self.params.output_folder
        fol_input = fol_out
        dirs_input = glob.glob(os.path.join(fol_input, glob_infols))
        _combine_directories(dirs_input, fol_out)
