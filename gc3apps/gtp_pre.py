#! /usr/bin/env python
#
#   gtp_pre.py -- TumorProfiler pre-processing script
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
Created once a new TumorProfiler acquisition is detected
Stage 0: parse metadata and create destination folder accordingly
Stage 1: if acquisition type sMC, run pre-processing pipeline in R
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
from pkg_resources import Requirement, resource_filename

import gc3libs
from gc3libs import Application
from gc3libs.workflow import StagedTaskCollection, \
    ParallelTaskCollection, SequentialTaskCollection
from gc3libs.quantity import Memory, kB, MB, MiB, GB, \
    Duration, hours, minutes, seconds

#####################
# Configuration
TP_PREPROCESSING = "tp_preprocessing.py"
TP_IMC_STAGE1 = "tp_imc_preprocessing.py"
TP_IMC_STAGE1_BASH = "tp_imc_pre.sh"

#####################
# Utility methods
#

whereami = os.path.dirname(os.path.abspath(__file__))

def _get_analysis_type(location):
    """
    Search in location for indicator of analysis type.
    Current algorithm:
    * if .fcs file then analysis type sMC
    * if .mcd file then analysis type IMC
    @param: location of raw data
    @ return: [IMC, sMC, None]
    """

    for data in os.listdir(location):
        if data.lower().endswith(".fcs"):
            return "sMC"
        if data.lower().endswith(".mcd"):
            return "IMC"
    return None


#####################
# Applications
#
class GTumorProfilerRunIMC(Application):
    """
    Run first part of IMC preprocessing pipeline:
    * generarte subfolders in `derived`
    * convert input MCD in OMETiff
    * Run CP3 pipeline from ImcSegmentationPipeline
    """
    def __init__(self, data_location, analysis_type, config_file, **extra_args):
        """
        """
        
        Application.__init__(
            self,
            arguments = ["./{0}".format(os.path.basename(TP_IMC_STAGE1_BASH)),
                         os.path.basename(TP_IMC_STAGE1),
                         data_location,
                         analysis_type,
                         config_file
            ],
            inputs = [os.path.join(whereami,TP_IMC_STAGE1), os.path.join(whereami,TP_IMC_STAGE1_BASH)],
            outputs = [],
            stdout = 'gtp_imc.log',
            join=True,
            executables=["./{0}".format(os.path.basename(TP_IMC_STAGE1_BASH))],
            **extra_args)

class GTumorProfilerPrepareFolders(Application):
    """
    parse metadata and create destination folder accordingly
    """
    def __init__(self, data_location, analysis_type, config_file, **extra_args):
        """
        """
        
        Application.__init__(
            self,
            arguments = ["python",
                         os.path.basename(TP_PREPROCESSING),
                         data_location,
                         analysis_type,
                         config_file
            ],
            inputs = [os.path.join(whereami,TP_PREPROCESSING)],
            outputs = [],
            stdout = 'gtp_pre0.log',
            join=True,
            executables=[],
            **extra_args)


#####################
# StagedTaskCollection class
#

class GTumorProfilerCollection(StagedTaskCollection):
    """
    Staged collection:
    Step 0: Parse metadata from input file and create destination folder structure
    Step 1: if 'sMC' run preprocessing R script and verify results
    """
    def __init__(self, data_location, config_file, **extra_args):

        self.data_location = data_location
        self.extra = extra_args
        self.config = config_file
        self.analysis_type = _get_analysis_type(data_location)
        assert self.analysis_type, "Failed getting analysis type from {0}".format(data_location)

        StagedTaskCollection.__init__(self)
        
    def stage0(self):
        """
        Step 0: Generate Dataset
        """

        extra_args = self.extra.copy()
        extra_args['jobname'] += "_{0}_stage0".format(self.analysis_type)

        return GTumorProfilerPrepareFolders(self.data_location,
                                            self.analysis_type,
                                            self.config,
                                            **extra_args)

    def stage1(self):
        """
        Step 1 Generate Dataset
        """

        # Check exit status from previous step.
        rc = self.tasks[0].execution.returncode
        if rc is not None and rc != 0:
            return rc

        jobname = self.extra["jobname"]
        extra_args = self.extra.copy()
        extra_args['jobname'] += "_{0}_stage1".format(self.analysis_type)

        if self.analysis_type == "IMC":
            return GTumorProfilerRunIMC(self.data_location,
                                        self.analysis_type,
                                        self.config,
                                        **extra_args)

