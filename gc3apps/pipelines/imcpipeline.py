#! /usr/bin/env python
#
#   imcpipeline.py -- IMC pre-processing script
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
  2019-04-05:
  * Initial version
"""
__author__ = 'Sergio Maffioletti <sergio.maffioletti@uzh.ch>'
__docformat__ = 'reStructuredText'
__version__ = '1.0.1'

import os
import gc3libs
from gc3libs import Run
import gc3apps
from gc3libs.workflow import StagedTaskCollection

#####################
# Configuration

#####################
# Utility methods
#

whereami = os.path.dirname(os.path.abspath(__file__))

#####################
# StagedTaskCollection class
#

class IMCPipeline(StagedTaskCollection):
    """
    Staged collection:
    Step 0: Parse metadata from input file and create destination folder structure
    Step 1: if 'sMC' run preprocessing R script and verify results
    """
    def __init__(self, data_location, dataset_name, instrument, config_file, **extra_args):

        self.data_location = data_location
        self.dataset_name = dataset_name
        self.instrument = instrument
        self.extra = extra_args
        self.analysis_type = "IMC"
        self.cfg = gc3apps.parse_config_file(config_file)
        self.destination = os.path.join(self.cfg['experiment']['raw_data_destination'],
                                        self.analysis_type,
                                        self.instrument,
                                        self.dataset_name)

        self.raw_data_destination = self.cfg['experiment']['subfolders']['raw']
        self.metadata = self.cfg['experiment']['subfolders']['scripts']
        self.subfolders = self.cfg['experiment']['subfolders'].values()        
        if self.cfg['IMC'].has_key('derived_subfolders'):
            self.subfolders += self.cfg['IMC']['derived_subfolders'].values()

        StagedTaskCollection.__init__(self)

    def stage0(self):
        """
        Step 0: Generate folders structure accroding to config file
        """

        extra_args = self.extra.copy()
        extra_args['jobname'] = "stage0"
        extra_args['output_dir'] = os.path.join(extra_args['output_dir'],extra_args['jobname'])

        return gc3apps.PrepareFolders(self.data_location,
                                      self.destination,
                                      self.subfolders,
                                      self.analysis_type,
                                      self.raw_data_destination,
                                      **extra_args)


    def stage1(self):
        """
        Parse .mcd and .txt files to extract metadata
        store metadata in .csv file
        convert images into ometiff
        """

        extra_args = self.extra.copy()
        extra_args['jobname'] = "stage1"
        extra_args['output_dir'] = os.path.join(extra_args['output_dir'],extra_args['jobname'])

        return gc3apps.IMCPreprocessing(self.destination,
                                        self.raw_data_destination,
                                        self.metadata,
                                        self.cfg['IMC'],
                                        **extra_args)

