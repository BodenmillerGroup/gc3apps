#!/bin/env python
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

import os
import re
import logging
logging.basicConfig()
log = logging.getLogger()
log.propagate = True

# Defaults
FILE_FORMAT = dict()
FILE_FORMAT['sMC'] = ".fcs"
FILE_FORMAT['IMC'] = ".mcd"
FNAME_SPLIT_CRITERIA = "_"

class TP_data():
    """
    Descriptive class.
    Contains information of a given experiment.
    """

    def __init__(self, location, analysis_type):

        self.location = location
        self.analysis_type = analysis_type

        # Get metadata associated to data files
        self.raw_data = [(data,self._get_metadata(data)) for
                         data in os.listdir(location)
                         if self._get_metadata(data)]

        self.metadata = [data for data in self.raw_data if data[0].lower().endswith(FILE_FORMAT[analysis_type])]

        self._check_metadata_consistency()
        
    @property
    def sample_id(self):
        """
        Assume 1st found .mcd file as representer
        of the whole dataset
        """
        return self.metadata[0][1][0]

    
    @property
    def tumor_type(self):
        """
        Assume 1st found .mcd file as representer
        of the whole dataset
        """
        return self.metadata[0][1][1]

    @property
    def folder_path(self):
        return os.path.join(self.analysis_type,
                            self.tumor_type,
                            self.sample_id)

    def _check_metadata_consistency(self):
        for data in self.raw_data:
            assert data[1][0] == self.sample_id, "FATAL: Found data {0} with sample id {1}/ " \
                "Should be {2}.".format(data[0],
                                        data[1][0],
                                        self.sample_id)

    def _get_metadata(self, raw_file):
        """
        experiment's metadata.
        """
    
        try: 
            (id,analysis_ref) = self.__split_filename(raw_file)[0:2]
            metadata_list = re.findall(r"[a-zA-Z0-9]+",analysis_ref)
            metadata_list.insert(0,self._get_abpanel(raw_file))
            metadata_list.insert(0,id[0]) # tumor type
            metadata_list.insert(0,id) # user id
            return metadata_list
        except Exception:
            log.warning("Failed parsing input data {0}. ignoring".format(raw_file))
            # raise Error("Failed parsing input data {0}. ignoring".format(raw_file))
           
    def _get_abpanel(self, raw_file):
        """
        specific parser only to extract panel information
        @param: filename
        @return: panel reference as found within filename
        """
        return self.__split_filename(raw_file)[1][0]

    def __split_filename(self, filename):
        return filename.split(FNAME_SPLIT_CRITERIA)
