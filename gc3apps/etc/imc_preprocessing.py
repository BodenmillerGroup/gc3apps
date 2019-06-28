#!/usr/bin/env python
#
#   Copyright (C) 2018, 2019 - bodenmillerlab, University of Zurich
#
#  This program is free software; you can redistribute it and/or modify it
#  under the terms of the GNU General Public License as published by the
#  Free Software Foundation; either version 2 of the License, or (at your
#  option) any later version.
#
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#  General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  59 Temple Place, Suite 330, Boston, MA 02111-1307 USA


"""
scripts_folder = os.path.join(folder_base,'scripts')
raw_folder = os.path.join(folder_base,'raw')
derived_folder = os.path.join(folder_base,'derived')
vsrs_folder = os.path.join(folder_base,'vsrs')

folder_analysis = os.path.join(derived_folder,'analysis')
folder_cp = os.path.join(erived_folder, 'cpout')
folder_ilastik = os.path.join(derived_folder, 'ilastik')
folder_ome = os.path.join(derived_folder,'OME')
folder_uncertainty = os.path.join(derived_folder, 'uncertainty')
folder_histocat = os.path.join(derived_folder, 'histocat')
"""

import os
import re
import sys
import yaml
import logging
import zipfile
import argparse
import subprocess
import logging.config
from yaml.parser import ParserError, ScannerError

# 3rd-part libraries
import imctools
from imctools.scripts import imc2tiff
from imctools.scripts import resizeimage
from imctools.scripts import ometiff2analysis
from imctools.scripts import exportacquisitioncsv
from imctools.scripts import convertfolder2imcfolder


# log setup
logging.basicConfig()
log = logging.getLogger()
log.propagate = True

__all__ = ['generate_ometiff', 'create_imc_subfolders', 'generate_analysis_stack', 'main']

# Defaults

csv_panel = None
csv_panel_metal = 'MetalTag'
csv_panel_ilastik = 'ilastik_cells'
csv_panel_full = 'full'

suffix_full = '_full'
suffix_ilastik = '_ilastik'
suffix_ilastik_scale = '_s2'
suffix_mask = '_mask.tiff'
suffix_probablities = '_Probabilities'

CP_SEGMENT_SIMPLE = "1_segment_simple.cppipe"
CP_MEASURE_MASK = "2_measure_mask.cppipe"
CP_BIN = "docker run {mount} cellprofiler/cellprofiler:2.3.1"
CP_PLUGINS = "$HOME/."

# ADDSUM: BOOL, should the sum of all channels be added as the first channel?
list_analysis_stacks =[
    (csv_panel_ilastik, suffix_ilastik, 1),
    (csv_panel_full, suffix_full, 0)
]

def generate_ometiff(raw_folder, ometiff_folder):

    log.debug("imctools convert_folder2imcfolder {0}".format(raw_folder))
    convertfolder2imcfolder.convert_folder2imcfolder(raw_folder,
                                                     out_folder=ometiff_folder,
                                                     dozip=False)

def create_imc_subfolders(folders):
    for folder in folders:
        try:
            if os.path.isdir(folder):
                log.warning("Folder {0} already exists. Ingoring and continuing".format(folder))
                continue
            os.mkdir(folder)
        except Exception, ex:
            log.error("Failed creating folder {0}. Error {1}:{2}".format(folder,
                                                                         type(ex),
                                                                         str(ex)))
            return False
    return True

def generate_analysis_stack(ome_location, analysis_location):
    # Generate the analysis stacks
    for fol in os.listdir(ome_location):
        sub_fol = os.path.join(ome_location, fol)
        for img in os.listdir(sub_fol):
            if not img.endswith('.ome.tiff'):
                continue
            basename = img.rstrip('.ome.tiff')
            for (col, suffix, addsum) in list_analysis_stacks:
                try:
                    ometiff2analysis.ometiff_2_analysis(os.path.join(sub_fol, img),
                                                        analysis_location,
                                                        basename + suffix,
                                                        pannelcsv=csv_panel,
                                                        metalcolumn=csv_panel_metal,
                                                        usedcolumn=col,
                                                        addsum=addsum,
                                                        bigtiff=False,
                                                        pixeltype='uint16')
                except Exception, ex:
                    log.error('Failed converting image {0}. Error {1}:{2}'.format(img,
                                                                                  type(ex),
                                                                                  str(ex)))



def main(data_location, raw_location, scripts_location, ome_location, analysis_location, cp_location):
    """
    Workflow:
    * setup: set initial paths
    * folders: create subfolders for image conversion
    * generate a folder with .tiff images in out_tiff_folder
    """
    # setup
    log.info("Starting IMC pre-processing pipeline in {0}".format(raw_location))

    # generate .tiff images
    log.info("generate .tiff images")
    convertfolder2imcfolder.convert_folder2imcfolder(raw_location,
                                                     out_folder=ome_location,
                                                     dozip=False)     

    # Generate analysis stacks
    if analysis_location and os.path.isdir(analysis_location):
        log.info("generate analysis stacks")
        generate_analysis_stack(ome_location, analysis_location)

    # Generate a csv with all the acquisition scripts
    if cp_location and os.path.isdir(cp_location):
        log.info("Generate a csv with all the acquisition scripts")
        exportacquisitioncsv.export_acquisition_csv(ome_location, fol_out=cp_location)

    # Done
    log.info("Done")

if __name__ == "__main__":
    # Setup the command line arguments
    parser = argparse.ArgumentParser(
        description='', prog='tp_IMC_preprocessing')

    parser.add_argument('data_location', type=str,
                        help='Prefix location of data.')

    parser.add_argument('-r', '--raw', type=str,
                        default='raw',
                        help='Where raw data are be stored.' \
                        'Default: %(default)s.')

    parser.add_argument('-s', '--scripts', type=str,
                        default='scripts',
                        help='Where scripts are stored.' \
                        'Default: %(default)s.')

    parser.add_argument('-o', '--ometiff', type=str,
                        default='ome',
                        help='Where OMETiffs will be stored.' \
                        'Default: %(default)s.')

    parser.add_argument('-a', '--analysis', type=str,
                        default='analysis',
                        help='Where analysis stack will be stored.' \
                        'Default: %(default)s.')

    parser.add_argument('-c', '--cellprofiler', type=str,
                        default='cpout',
                        help='Where Cellprofiler output will be stored.' \
                        'Default: %(default)s.')

    parser.add_argument("-v", "--verbose", help="increase output verbosity",
                        action="store_true")
    
    args = parser.parse_args()
    assert os.path.isdir(args.data_location)

    if args.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    raw_location = os.path.join(args.data_location,args.raw)
    scripts_location = os.path.join(args.data_location,args.scripts)
    ome_location = os.path.join(args.data_location,args.ometiff)
    analysis_location = os.path.join(args.data_location,args.analysis)
    cp_location = os.path.join(args.data_location,args.cellprofiler)

    # assert os.path.isdir(raw_location), "Folder {0} not found".format(raw_location)
    # assert os.path.isdir(scripts_location), "Folder {0} not found".format(scripts_location)
    # assert os.path.isdir(ome_location), "Folder {0} not found".format(ome_location)
    # assert os.path.isdir(analysis_location), "Folder {0} not found".format(analysis_location)
    # assert os.path.isdir(cp_location), "Folder {0} not found".format(cp_location)

    sys.exit(main(args.data_location, raw_location, scripts_location, ome_location, analysis_location, cp_location))
