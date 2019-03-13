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

# summary of user-visible changes
__changelog__ = """
  2018-03-08:dd
  * Initial version
"""
__author__ = 'Vito Zanotelli <vito.zanotelli@uzh.ch>'
__docformat__ = 'reStructuredText'
__version__ = '1.0.0'

# run script, but allow GC3Pie persistence module to access classes defined here;
# for details, see: https://github.com/uzh/gc3pie/issues/95
if __name__ == "__main__":
    import gilastik_pipeline
    gilastik_pipeline.GIlastikPipelineScript().run()

import glob
import os
import re
import json
import shutil
import gc3apps
import gc3libs
from gc3libs import Application
from gc3apps.apps import RunIlastik
from gc3libs.workflow import StagedTaskCollection, \
    ParallelTaskCollection, SequentialTaskCollection
from gc3libs.quantity import Memory, kB, MB, MiB, GB, \
    Duration, hours, minutes, seconds
from gc3libs.cmdline import SessionBasedScript, existing_file, \
    positive_int, existing_directory

#####################
# StagedTaskCollection class
#
def _get_images(input_folder, input_re):
    pattern = re.compile(input_re)
    return [os.path.join(root, name)
            for root, dirs, files in os.walk(input_folder)
            for name in files
            if pattern.match(name)
            ]

def _get_chunks(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

def _copy_ilastik_file(path_source, fol_source, fol_target):
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
                _copy_ilastik_file(path_source=os.path.join(dp, f), fol_source=d_root, fol_target=fol_out)

class GIlastikPipeline(ParallelTaskCollection):
    """
    Runs an ilastik pipeline in batches 
    """
    def __init__(self, ilastik_project, input_folder, input_re, output_folder, chunks,
            export_source, export_dtype, output_filename, **extra_args):

        self.ilastik_project = ilastik_project
        self.input_folder = input_folder
        self.input_re = input_re
        self.output_folder = output_folder
        self.chunks = chunks
        self.extra = extra_args

        tasks = []
        input_images = _get_images(input_folder, input_re)
        gc3libs.log.debug('{0} input images identified'
                .format(len(input_images)))
        gc3libs.log.debug("creating batches of {0} images"
                .format(self.chunks))
        for runnr, input_chunk in enumerate(
            _get_chunks(input_images, self.chunks)):
            jobname = self.extra["jobname"]
            extra_args = self.extra.copy()
            extra_args['jobname'] = "ilastik_run_{0}".format(runnr)
            extra_args['output_dir'] = os.path.join(extra_args['output_dir'],
                                                    extra_args['jobname'])
            output_folder_batch = os.path.join(self.output_folder,"output_{0}".format(runnr))
            if not os.path.exists(output_folder_batch):
                gc3libs.log.debug("Creating new batch folder at {0}.".format(output_folder_batch))
                os.makedirs(output_folder_batch)
                os.chmod(output_folder_batch, 0777)

            tasks.append(RunIlastik(ilastik_project,
                                         output_folder_batch,
                                         input_chunk,
                                         export_source,
                                         export_dtype,
                                         output_filename,
                                         **extra_args))
        self.tasks = tasks
        return ParallelTaskCollection.__init__(self, self.tasks)


class GIlastikPipelineScript(SessionBasedScript):
    """
    The ``gilastik_pipeline`` command keeps a record of jobs (submitted, executed
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
        self.add_param('project_file', type=existing_file,
                       help="A trained ilastik classifier project file")
        
        self.add_param('input_folder', type=str,
                       help="Location of the root folder containing the images")
        
        self.add_param('input_re', type=str,
                       help="A regular expression matching all the files that should be classified.")

	self.add_param("output_folder", type=str,
                help="Location of the results.")

    def setup_options(self):
        self.add_param("-K", "--chunks", metavar="[INT]",
                       type=positive_int,
                       dest="chunks", default=30,
                       help="Chunk size for each batch run. Default: '%(default)s'.")

        self.add_param("-dtype", "--export_dtype", metavar="[OUTPUT DTYPE]",
                       type=str,
                       dest="export_dtype",
                       default='uint16',
                       help="""Type of result to export (Ilastik --export_dtype argument). Default: '%(default)s'.""")

        self.add_param("-source", "--export_source", metavar="[OUTPUT TYPE]",
                       type=str,
                       dest="export_source",
                       default='"Probabilities"',
                       help="""Type of result to export (Ilastik --export_source argument). Default: '%(default)s'.""")

        self.add_param("-filename", "--output_filename_format", metavar="[OUTPUT Filename]",
                       type=str,
                       dest="output_filename",
                       default=None,
                       help="""Type of result to export (Ilastik --output_filename_format argument).
                                Default: {nickname}_{export_type}.tiff .""")

        self.add_param("--docker", metavar="[IMAGE NAME]",
                       type=str,
                       dest="docker_image",
                       help="Docker image that runs the gilastik pipeline.")

    def parse_args(self):
	"""
	Declare command line arguments.
	"""
	self.params.project_file = os.path.abspath(self.params.project_file)
	self.params.input_folder = os.path.abspath(self.params.input_folder)
	self.params.output_folder = os.path.abspath(self.params.output_folder)

    def new_tasks(self, extra):
        """
        Chunk initial input file
        For each chunked fule, generate a new GfittingaddmTask
        """

        extra_args = extra.copy()
        extra_args['jobname'] = os.path.basename(self.params.project_file)

        extra_args['output_dir'] = os.path.join(os.path.abspath(self.session.path),
                                                '.compute',
                                                extra_args['jobname'])
        extra_args['docker_image'] = self.params.docker_image
        print(self.params)

        return [GIlastikPipeline(self.params.project_file,
                                      self.params.input_folder,
                                      self.params.input_re,
                                      self.params.output_folder,
                                      self.params.chunks,
                                      self.params.export_source,
                                      self.params.export_dtype,
                                      self.params.output_filename,
                                      **extra_args)]

    def after_main_loop(self):
        glob_infols = 'output_*'
        fol_out = self.params.output_folder
        fol_input = fol_out
        dirs_input = glob.glob(os.path.join(fol_input, glob_infols))
        _combine_directories(dirs_input, fol_out)
