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
Changelog:
* 2019-05-13
  * initil version

"""

import sys
import os
import re
import pwd
import yaml
import shutil
import zipfile
import logging
import argparse
import imctools
import logging.config
import imctools.scripts
from yaml.parser import ParserError, ScannerError

logging.basicConfig()
log = logging.getLogger()
log.propagate = True

# Defaults
FILE_FORMAT = dict()
FILE_FORMAT['sMC'] = ".fcs"
FILE_FORMAT['IMC'] = ".mcd"
FNAME_SPLIT_CRITERIA = "_"
ANALYSIS_TYPES = ['IMC','sMC']
SAMPLE_ANCHOR = "%(sample_id)"
IMC_VALID_SUFFIX = ["mcd","txt","jpg","schema","FCS","fcs"]


# Operation functions

def create_destination_folder_structure(destination, subfolders, dryrun=False):
    """
    Create folder structure according to passed `folder_list`
    """

    failure = 0

    for folder in subfolders:
        dest = os.path.join(destination,
                            folder)
        assert not os.path.isdir(dest), "Destination {0} already exists.".format(dest)
        if not dryrun:
            try:
                os.makedirs(dest)
            except OSError, osx:
                log.error("Failed while creating folder {0}."\
                          "Message {1}".format(dest,
                                               osx.message))
                failure = 1
                break
        else:
            log.info("DRYRUN: creating fodler {0}".format(dest))
    return failure


def extract_data(dataset, destination):
    """
    From input .zip file
    extract it into destination
    """
    with zipfile.ZipFile(dataset) as data:
        data.extractall(path=destination)

# MAIN

def main(source, destination, subfolders, analysis_type, raw_data_destination, dryrun=False):
    """
    Run the main workflow:
    * create experiment object
    * create destination folder
    * copy raw data
    * checkout scripts repo and create new branch
    """
    create_destination_folder_structure(destination, subfolders, dryrun)
    extract_data(source, os.path.join(destination, raw_data_destination))

if __name__ == "__main__":
    # Setup the command line arguments
    parser = argparse.ArgumentParser(
        description='', prog='preprocessing')

    parser.add_argument('data', type=str,
                        help='Dataset source') 

    parser.add_argument('destination', type=str,
                        help='Destination folder') 

    parser.add_argument('analysis_type', type=str,
                        help='Analysis type: {0}'.format(ANALYSIS_TYPES))

    parser.add_argument('-r', '--raw_data_destination', type=str,
                        default='raw',
                        help='From subfolders, where raw data will be made available') 

    parser.add_argument('-f', '--folders', type=str,
                        default=None,
                        help='`,` separated list of subfolders') 
    
    parser.add_argument('-d','--dryrun',
                        action='store_true',
                        default=False,
                        help='Enable dryrun. Default: %(default)s.')

    parser.add_argument("-v", "--verbose", help="increase verbosity",
                        action="store_true")

    args = parser.parse_args()

    assert os.path.isfile(args.data), "Dataset file {0} not found".format(args.data)
    assert args.analysis_type in ANALYSIS_TYPES, "Not valid analysis type {0}. Valid values are: {1}".format(args.analysis_type,
                                                                                                    ANALYSIS_TYPES)

    if args.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    if args.folders:
        subfolders = args.folders.split(",")
    else:
        subfolders = [args.raw_data_destination]

    assert not os.path.isdir(args.destination), "Experiment destination folder {0} already exists".format(args.destination)
    assert args.raw_data_destination in subfolders, "Raw data destination {0} not in the list of subfolders to be created {1}".format(args.raw_data_destination,
                                                                                                                                      subfolders)

    sys.exit(main(args.data, args.destination, subfolders, args.analysis_type, args.raw_data_destination, args.dryrun))
