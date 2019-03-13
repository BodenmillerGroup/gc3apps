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
from gc3apps.h5parse import CPparser
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

    def __init__(self, batch_file, output_folder, start_index, end_index, cp_plugins, **extra_args):

        inputs = dict()
        outputs = []

        self.docker_image = gc3apps.Default.DEFAULT_CELLPROFILER_DOCKER
        cparser = CPparser(batch_file)
        inputs[batch_file] = os.path.basename(batch_file)
        if extra_args["docker_image"]:
            self.docker_image = extra_args["docker_image"]

        command = gc3apps.Default.CELLPROFILER_DOCKER_COMMAND.format(batch_file="$PWD/{0}".format(inputs[batch_file]),
                                                                     data_mount_point=gc3apps.Default.DEFAULT_BBSERVER_MOUNT_POINT,
                                                                     docker_image = self.docker_image,
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

    def __init__(self, pipeline, image_data_folder, plugins, **extra_args):

        inputs = dict()
        outputs = ["./output/"]

        inputs[pipeline] = os.path.basename(pipeline)
        inputs[os.path.join(whereami,
                            gc3apps.Default.GET_CP_GROUPS_FILE)] = gc3apps.Default.GET_CP_GROUPS_FILE
        
        self.json_file = os.path.join(extra_args['output_dir'],"result.json")
        self.batch_file = os.path.join(extra_args['output_dir'], "Batch_data.h5")
        self.docker_image = gc3apps.Default.DEFAULT_CELLPROFILER_DOCKER
        if extra_args["docker_image"]:
            self.docker_image = extra_args["docker_image"]

        cmd = gc3apps.Default.GET_CP_GROUPS_CMD.format(output="./output",
                                                       pipeline=inputs[pipeline],
                                                       image_data=image_data_folder,
                                                       cp_plugins=plugins,
                                                       docker_image=self.docker_image)

	gc3libs.log.debug("In RunCellprofilerGetGroups running {0}.".format(cmd))

        Application.__init__(
            self,
            arguments = cmd,
            inputs = inputs,
            outputs = outputs,
            stdout = "log.out",
            stderr = "log.err",
            join=False,
            executables=["./{0}".format(gc3apps.Default.GET_CP_GROUPS_FILE)],
            **extra_args)


    def terminated(self):
        """
        Check presence of output log and verify it is
        a legit .json file
        Do not trust cellprofiler exit code (exit with 1)
        """

	gc3libs.log.debug("In RunCellprofilerGetGroups running 'termianted'.")

        try:
            with open(self.json_file,"r") as fd:
                try:
                    data = json.load(fd)
                    if len(data) > 0:
                        self.execution.returncode = (0, 0)
                except ValueError as vx:
                    # No valid json
                    gc3libs.log.error("Failed parsing {0}. No valid json.".format(self.json_file))
                    self.execution.returncode = (0,1)
        except IOError as ix:
            # Json file not found 
            gc3libs.log.error("Required json file at {0} was not found".format(self.json_file))



class RunIlastik(Application):
    """
    Run Ilastik in batch mode
    """

    application_name = 'runilastik'

    def __init__(self, project_file, output_folder, input_files, export_source,
            export_dtype, output_filename, **extra_args):

        inputs = dict()
        outputs = []

        self.docker_image = gc3apps.Default.DEFAULT_ILASTIK_DOCKER
        inputs[project_file] = os.path.basename(project_file)

        if extra_args["docker_image"]:
            self.docker_image = extra_args["docker_image"]

        if output_filename is None:
            outtype = filter(str.isalnum, export_source)
            output_filename =  '{{nickname}}_{outtype}.tiff'.format(
                    outtype=outtype)

        input_file_string = ' '.join(input_files)

        command = gc3apps.Default.ILASTIK_DOCKER_COMMAND.format(
                project_file="$PWD/{0}".format(inputs[project_file]),
                                                                     data_mount_point=gc3apps.Default.DEFAULT_BBSERVER_MOUNT_POINT,
                                                                     docker_image = self.docker_image,
                                                                     input_files=input_file_string,
                                                                     output_folder=output_folder,
                                                                     export_source=export_source,
                                                                     export_dtype=export_dtype,
                                                                     output_filename=output_filename
                                                                     )
        Application.__init__(
            self,
            arguments = command,
            inputs = inputs,
            outputs = [],
            stdout = 'log',
            join=True,
            executables=[],
             **extra_args)
         
