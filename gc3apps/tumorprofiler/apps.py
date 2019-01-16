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
import gc3libs
from gc3libs import Application
from gc3libs.quantity import Memory, \
    kB, MB, MiB, GB, Duration, hours,\
    minutes, seconds

#####################
# Configuration
TP_PREPROCESSING = "preprocessing.py"
TP_IMC_STAGE1 = "processing_imc.py"
TP_IMC_STAGE1_BASH = "tp_imc_pre.sh"

#####################
# Applications
#

class TPPrepareFolders(Application):
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
        
        Application.__init__(
            self,
            arguments = ["./{0}".format(os.path.basename(TP_IMC_STAGE1_BASH)),
                         os.path.basename(TP_IMC_STAGE1),
                         data_location,
                         config_file
            ],
            inputs = [os.path.join(whereami,TP_IMC_STAGE1), os.path.join(whereami,TP_IMC_STAGE1_BASH)],
            outputs = [],
            stdout = 'log',
            join=True,
            executables=["./{0}".format(os.path.basename(TP_IMC_STAGE1_BASH))],
            **extra_args)

