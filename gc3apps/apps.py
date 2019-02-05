#! /usr/bin/env python
#
#   apps.py -- TumorProfiler apps
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
Stage 0: parse metadata and create destination folder accordingly
Stage 1: Run preprocessing
"""

# summary of user-visible changes
__changelog__ = """
  2019-01-14:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'
__version__ = '1.0.0'

import os
import json
import gc3apps
import gc3libs
from gc3libs import Application
from gc3libs.quantity import Memory, \
    kB, MB, MiB, GB, Duration, hours,\
    minutes, seconds

#####################
# Configuration
TP_PREPROCESSING = "tp_preprocessing.py"
TP_IMC_STAGE1 = "processing_imc.py"
TP_IMC_STAGE1_BASH = "tp_imc_pre.sh"

#####################
# Utilities
#

whereami = os.path.dirname(os.path.abspath(__file__))

#####################
# Applications
#

class NotifyApplication(Application):
    """
    Test application to verify the pre-processing step
    To be used in conjunction with data_daemon and triggered
    when a new dataset is added to the dataset folder
    """

    def __init__(self, data_location, analysis_type, config_file, **extra_args):

        Application.__init__(
            self,
            arguments = ["/bin/true"],
            inputs = [],
            outputs = [],
            stdout = 'log',
            join=True,
            executables=[],
            **extra_args)

class TPPrepareFolders(Application):
    """
    parse metadata and create destination folder accordingly
    """
    def __init__(self, data_location, analysis_type, config_file, **extra_args):
        """
        """

        cmd = ["python",
                         os.path.basename(TP_PREPROCESSING),
                         data_location,
                         analysis_type,
                         config_file
               ]


        gc3libs.log.debug("dryrun setting: {0}".format(extra_args['dryrun']))
        if extra_args['dryrun']:
            cmd.append("--dryrun")
        
        Application.__init__(
            self,
            arguments = cmd,
            inputs = [os.path.join(whereami,TP_PREPROCESSING)],
            outputs = [],
            stdout = 'log',
            join=True,
            executables=[],
            **extra_args)


class TPRunIMC(Application):
    """
    Run first part of IMC preprocessing pipeline:
    * generarte subfolders in `derived`
    * convert input MCD in OMETiff
    * Run CP3 pipeline from ImcSegmentationPipeline
    """
    def __init__(self, data_location, config_file, **extra_args):
        """
        """

        cmd = ["./{0}".format(os.path.basename(TP_IMC_STAGE1_BASH)),
                         os.path.basename(TP_IMC_STAGE1),
                         data_location,
                         config_file,
            ]
        if extra_args['dryrun']:
            cmd.append("--dryrun")
        
        Application.__init__(
            self,
            arguments = cmd,
            inputs = [os.path.join(whereami,TP_IMC_STAGE1), os.path.join(whereami,TP_IMC_STAGE1_BASH)],
            outputs = [],
            stdout = 'log',
            join=True,
            executables=["./{0}".format(os.path.basename(TP_IMC_STAGE1_BASH))],
            **extra_args)

class RunCellprofiler(Application):
    """
    Run Cellprofiler in batch mode
    """

    application_name = 'runcellprofiler'

    def __init__(self, batch_file, input_folder, output_folder, start_index, end_index, cp_plugins, **extra_args):

        inputs = dict()
        outputs = []

        inputs[batch_file] = os.path.basename(batch_file)
        command = gc3apps.Default.CELLPROFILER_DOCKER_COMMAND.format(batch_file=batch_file,
                                                                     src_mount_point=input_folder,
                                                                     start=start_index,
                                                                     end=end_index,
                                                                     output_folder=output_folder,
                                                                     plugins=cp_plugins)

        Application.__init__(
            self,
            arguments = command,
            inputs = inputs,
            outputs = [],
            stdout = 'log',
            join=True,
            executables=[],
            **extra_args)

class RunCellprofilerGetGroups(Application):
    """
    Run Cellprofiler in batch mode and get images groups information
    @params: Cellprofiler HD5 batch file
    @returns: .json file containing image group information
    """

    application_name = 'runcellprofiler'

    def __init__(self, batch_file, **extra_args):

        inputs = dict()
        outputs = []

        command = gc3apps.Default.CELLPROFILER_GETGROUPS_COMMAND.format(
                                                                 batch_file=batch_file)

	gc3libs.log.debug("In RunCellprofilerGetGroups running {0}.".format(command))

        Application.__init__(
            self,
            arguments = command,
            inputs = inputs,
            outputs = [gc3apps.Default.CELLPROFILER_GROUPFILE],
            stdout = gc3apps.Default.CELLPROFILER_GROUPFILE,
            stderr = "log.err",
            join=False,
            executables=[],
            **extra_args)

    def terminated(self):
        """
        Check presence of output log and verify it is
        a legit .json file
        Do not trust cellprofiler exit code (exit with 1)
        """

	gc3libs.log.debug("In RunCellprofilerGetGroups running 'termianted'.")

        with open(os.path.join(self.output_dir,self.stdout),"r") as fd:
            try:
                data = json.load(fd)
                if len(data) > 0:
                    self.execution.returncode = 0
            except ValueError as vx:
                # No valid json
                gc3libs.log.error("Failed parsing {0}. No valid json.".format((os.path.join(self.output_dir,
                                                                                            self.stdout))))
                self.execution.returncode = (0,1)
