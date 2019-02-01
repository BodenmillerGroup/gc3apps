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
* 2018-12-06
  * sMC: O3D3S-C__M-Ref5-r1-v1_0__xx.fcs
  * IMC: O3D3S-I-1__I-r1-v1_0_a.txt

* 2018-11-28
  * IMC: MOHUA-TS-2.I.I-r1-v1_0_f-36330-9883-800-800
  * sMC: BWI14-M4.C.I-Ref4-r1-v1_0__r2-v1

* 2018-11-27
  * IMC: MOHUA-TS-2.I__I-r1-v1_0_f-36330-9883-700-700
  * sMC: MOHUA-2.C__I-Ref2-r1-v1_1.6190.09.FCS

Workflow:
* get source experiment folder name (location of acquired images)
* derive metadata by parsing one of the .FCS files located in the experiment folder
* create project folder structure according to metadata
* move/copy original data into newly created folder structure
* checkout git repo into newly created folder structure

"""

import sys
import os
import re
import pwd
import yaml
import shutil
import logging
import argparse
from git import Repo
import logging.config
from yaml.parser import ParserError, ScannerError

from gc3apps import tp_data

logging.basicConfig()
log = logging.getLogger()
log.propagate = True


# Defaults
ANALYSIS_TYPES = ['IMC','sMC']

# Utility methods
def is_append(folder_location):
    """
    Checks whether destination folder exists and
    if it has been already pre-populated.
    @parameter: location of experiment folder
    @return: True|False
    """
    return os.path.isdir(folder_location)

# Operation functions

def create_destination_folder_structure(folder_path, folder_list, append_raw=False):
    """
    Create folder structure according to passed `folder_list`
    """
    failure = 0
        
    for folder in folder_list:
        dest = os.path.join(folder_path,
                            folder)
        if os.path.isdir(dest):
            log.warning("Destination {0} already exists.".format(dest))
            if not append_raw:
                failure = 1
                break
        else:
            try:
                os.makedirs(dest)
            except OSError, osx:
                log.error("Failed while creating folder {0}."\
                          "Message {1}".format(dest,
                                               osx.message))
                failure = 1
                break
    return failure

def checkout_git_repo(repo, destination, new_branch_name, remote_branch="master"):
    """
        clone intial repository for TP scripts.
        Craete a new branch for the new experiment
        Name the new branch after the `experiment name` provided
        operations: 
        git clone url
        git branch -b sample_id
    """
    repo = Repo.clone_from(repo,destination)

    for ref in repo.references:
        if ref.name == remote_branch:
            log.debug("Checking out remote branch {0}".format(ref.name))
            ref.checkout()
    branch = repo.create_head(new_branch_name)
    repo.head.reference = branch
    repo.heads[0].checkout()

def parse_config_file(config_file):
    """
    Parse config file and verify mandatrory
    fileds have been specified
    """
    with open(config_file, 'r') as fd:
        try:
            cfg = yaml.load(fd)
            assert cfg.has_key("experiment"), "Missing 'experiment' section in config file"
            assert cfg["experiment"].has_key("panels"), "Missing 'panels' section in config file"
            assert cfg["experiment"].has_key("folder_prefix"), "Missing 'folder_prefix' section in config file"
            assert cfg["experiment"].has_key("subfolders"), "Missing 'subfolders' section in config file"

            for key in ANALYSIS_TYPES:
                assert cfg.has_key(key), "Missing {0} section in config file".format(key)
                assert cfg[key].has_key("repo"), "Missing 'repo' section in config {0}".format(key)
                assert cfg[key]['repo'].has_key("source"), "Missing 'repo/source' section in config {0}".format(key)
                assert cfg[key]['repo'].has_key("branch"), "Missing 'repo/branch' section in config {0}".format(key)
                assert cfg[key]['repo'].has_key("location"), "Missing 'repo/location' section in config {0}".format(key)

            return cfg
        except Exception as ex:
            log.error("Failed parsing config file {0}. "
                      "Error {1}:{2}".format(config_file,
                                             type(ex),
                                             str(ex)))
            raise
    return None

def transfer_raw_data(raw_data_location, raw_data_list, destination_folder, panels, allow_append=False, move=True):
    """
    Populate destination folder's `raw` data.
    @parameter: instance of TPdata
    @parameter: destination folder where `raw` data will be transferred.
    @parameter: config - global configuration file
    @return: True|False
    """

    for fraw,metadata in raw_data_list:
        assert metadata[2] in panels.keys(), \
            "Panel reference {0}, not in list of valid panels: " \
            "{1}".format(cfg["experiment"]["panels"].keys())


        destination = os.path.join(destination_folder,
                                   panels[metadata[2]],
                                   fraw)
        try:
            if not move:
                os.symlink(os.path.join(raw_data_location,fraw),destination)
            else:
                shutil.move(os.path.join(raw_data_location,fraw),destination)
        except (OSError, IOError) as ioe:
            if not allow_append:
                log.error("Failed copying {0} into {1}. " \
                          "Error {2}, message: {3}".format(fraw,
                                                           destination,
                                                           type(ioe),
                                                           str(ioe)))
                raise
            else:
                continue

# MAIN

def main(location, analysis_type, configuration, dryrun=False):
    """
    Run the main workflow:
    * create experiment object
    * create destination folder
    * copy raw data
    * checkout scripts repo and create new branch
    """

    config = parse_config_file(configuration)
    destination_folder = config["experiment"]["folder_prefix"]

    log.info("Creating new experiment in " \
             "{0} taking data from {1}".format(location,
                                               destination_folder))
    data = tp_data.TP_data(location, analysis_type)

    log.info("Creating experiment folder structure in {0}".format(data.folder_path))
    are_we_appending = is_append(os.path.join(destination_folder,data.folder_path))
    assert (not is_append(os.path.join(destination_folder,data.folder_path)) or
            config[analysis_type]['allow_append_raw_data']), "FATAL: appending data to existing "\
            " experiment {0} is not allowed for this analysis type {1}.".format(os.path.join(destination_folder,
                                                                                             data.folder_path),
                                                                                analysis_type)

    if not are_we_appending:
        # Create destination folder following
        # configuration file
        raw_subfolders = []
        raw_subfolders.extend(config["experiment"]["subfolders"])
        raw_subfolders.extend(config["experiment"]["panels"].values())

        create_destination_folder_structure(os.path.join(destination_folder,
                                                         data.folder_path),
                                            raw_subfolders,
                                            config[analysis_type]['allow_append_raw_data'])

        if not dryrun:
            log.info("Creating new branch for analysis scripts")
            checkout_git_repo(config[analysis_type]['repo']['source'],
                              os.path.join(destination_folder,
                                           data.folder_path,
                                           config[analysis_type]['repo']['location']),
                              "{0}_{1}".format(analysis_type,
                                               data.sample_id),
                              remote_branch=config[analysis_type]['repo']['branch'])

    log.info("Transferring data... ")
    transfer_raw_data(data.location,
                      data.raw_data,
                      os.path.join(destination_folder, data.folder_path),
                      config['experiment']['panels'],
                      config[analysis_type]['allow_append_raw_data'],
                      not dryrun)

    log.info("Done")


if __name__ == "__main__":
    # Setup the command line arguments
    parser = argparse.ArgumentParser(
        description='', prog='tp_preprocessing')

    parser.add_argument('folder_location', type=str,
                        help='Location of the raw input files.')

    parser.add_argument('analysis_type', type=str,
                        help='Analysis type.') 

    parser.add_argument('configuration', type=str,
                        help='Location of configuration YAML file.') 

    parser.add_argument('-d','--dryrun',
                        action='store_true',
                        default=False,
                        help='Enable dryrun. Default: %(default)s.')

    parser.add_argument("-v", "--verbose", help="increase verbosity",
                        action="store_true")

    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    assert os.path.isdir(args.folder_location)

    sys.exit(main(args.folder_location, args.analysis_type, args.configuration, args.dryrun))
