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

# class TP_data():
#     """
#     Descriptive class.
#     Contains information of a given experiment.
#     """

#     def __init__(self, location, analysis_type):

#         self.location = location
#         self.analysis_type = analysis_type

#         # Get metadata associated to data files
#         self.raw_data = [(data,self._get_metadata(data)) for
#                          data in os.listdir(location)
#                          if self._get_metadata(data)]

#         self.metadata = [data for data in self.raw_data if data[0].lower().endswith(FILE_FORMAT[analysis_type])]

#         # check consistency
#         assert self.sample_id, "Failed getting sample id"
#         assert self.tumor_type, "Failed getting tumor type"
#         self._check_metadata_consistency()
        
#     @property
#     def sample_id(self):
#         """
#         Assume 1st found .mcd file as representer
#         of the whole dataset
#         """
#         return self.metadata[0][1][0]

    
#     @property
#     def tumor_type(self):
#         """
#         Assume 1st found .mcd file as representer
#         of the whole dataset
#         """
#         return self.metadata[0][1][1]

#     @property
#     def folder_path(self):
#         return os.path.join(self.analysis_type,
#                             self.tumor_type,
#                             self.sample_id)

#     def _check_metadata_consistency(self):
#         for data in self.raw_data:
#             assert data[1][0] == self.sample_id, "FATAL: Found data {0} with sample id {1}/ " \
#                 "Should be {2}.".format(data[0],
#                                         data[1][0],
#                                         self.sample_id)

#     def _get_metadata(self, raw_file):
#         """
#         experiment's metadata.
#         """
#         # Ignore files that have no .txt .jpg or .mcd or .schema format
#         if not raw_file.split(".")[-1] in IMC_VALID_SUFFIX:
#             log.warning("Skipping data {0}. Not in supported format".format(raw_file))
#         else:
#             try: 
#                 (id,analysis_ref) = self.__split_filename(raw_file)[0:2]
#                 metadata_list = re.findall(r"[a-zA-Z0-9]+",analysis_ref)
#                 metadata_list.insert(0,self._get_abpanel(raw_file))
#                 metadata_list.insert(0,id[0]) # tumor type
#                 metadata_list.insert(0,id) # user id
#                 return metadata_list
#             except Exception:
#                 log.warning("Failed parsing input data {0}. ignoring".format(raw_file))

#     def _get_abpanel(self, raw_file):
#         """
#         specific parser only to extract panel information
#         @param: filename
#         @return: panel reference as found within filename
#         """
#         return self.__split_filename(raw_file)[1][0]

#     def __split_filename(self, filename):
#         return filename.split(FNAME_SPLIT_CRITERIA)

# # Utility methods
# def is_append(folder_location):
#     """
#     Checks whether destination folder exists and
#     if it has been already pre-populated.
#     @parameter: location of experiment folder
#     @return: True|False
#     """
#     return os.path.isdir(folder_location)

# # Operation functions

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

# def checkout_git_repo(repo, destination, new_branch_name, remote_branch="master"):
#     """
#         clone intial repository for TP scripts.
#         Craete a new branch for the new experiment
#         Name the new branch after the `experiment name` provided
#         operations: 
#         git clone url
#         git branch -b sample_id
#     """
#     repo = Repo.clone_from(repo,destination)

#     git = repo.git
#     git.checkout(remote_branch)
#     git.checkout('HEAD', b=new_branch_name)

# def transfer_raw_data(raw_data_location, raw_data_list, destination_folder, panels=None, allow_append=False, move=True):
#     """
#     Populate destination folder's `raw` data.
#     @parameter: instance of TPdata
#     @parameter: destination folder where `raw` data will be transferred.
#     @parameter: config - global configuration file
#     @return: True|False
#     """

#     for fraw,metadata in raw_data_list:
#         if panels:
#             assert metadata[2] in panels.keys(), \
#                 "Panel reference {0}, not in list of valid panels: " \
#                 "{1}".format(panels.keys())
#             destination = os.path.join(destination_folder,
#                                        panels[metadata[2]],
#                                        fraw)
#         else:
#             destination = os.path.join(destination_folder,
#                                        "raw",
#                                        fraw)
            
#         try:
#             if not move:
#                 os.symlink(os.path.join(raw_data_location,fraw),destination)
#             else:
#                 shutil.move(os.path.join(raw_data_location,fraw),destination)
#         except (OSError, IOError) as ioe:
#             if not allow_append:
#                 log.error("Failed copying {0} into {1}. " \
#                           "Error {2}, message: {3}".format(fraw,
#                                                            destination,
#                                                            type(ioe),
#                                                            str(ioe)))
#                 raise
#             else:
#                 continue


# def save_data_location(output_file, location):
#     with open(output_file, "w") as fd:
#         fd.write(location)


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

    assert args.raw_data_destination in subfolders, "Raw data destination {0} not in the list of subfolders to be created {1}".format(args.raw_data_destination,
                                                                                                                                      subfolders)

    sys.exit(main(args.data, args.destination, subfolders, args.analysis_type, args.raw_data_destination, args.dryrun))
