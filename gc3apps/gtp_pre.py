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
import gc3libs
import gc3apps
from gc3libs import Application
from gc3apps.apps import TPPrepareFolders, TPRunIMC
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
# StagedTaskCollection class
#

class GTumorProfilerIMC(StagedTaskCollection):
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

        return TPPrepareFolders(self.data_location,
                                self.analysis_type,
                                self.config,
                                **extra_args)

    def stage1(self):
        """
        Step 1 Generate Dataset
        """

        # Check exit status from previous step.
        rc = self.tasks[0].execution.returncode

        jobname = self.extra["jobname"]
        extra_args = self.extra.copy()
        extra_args['jobname'] += "_{0}_stage1".format(self.analysis_type)

        return TPRunIMC(self.data_location,
                        self.config,
                        **extra_args)

class GTumorProfilersMC(StagedTaskCollection):
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

        return TPPrepareFolders(self.data_location,
                                self.analysis_type,
                                self.config,
                                **extra_args)
