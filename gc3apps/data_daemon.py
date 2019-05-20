#! /usr/bin/env python
#
"""
Session manager daemon: manage and progress tasks in an existing session.

This code basically only instanciates the stock ``SessionBasedDaemon``
class: no new tasks are ever created (as the ``new_tasks`` hook, and the
``created``/``modified``/``deleted`` handlers are not overridden), but one
can still connect to the XML-RPC interface and manage existing tasks.
"""
# Copyright (C) 2018, 2019 University of Zurich. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import (absolute_import, division, print_function)

import os
import sys
import gc3apps
import gc3libs
import gc3apps.pipelines
from fnmatch import fnmatch
from os.path import basename
from gc3libs import Application, Run, Task
from gc3libs.cmdline import SessionBasedDaemon, \
    existing_file, existing_directory
from gc3libs.quantity import Memory, kB, MB, MiB, \
    GB, Duration, hours, minutes, seconds

class InboxProcessingDaemon(SessionBasedDaemon):
    """
    Run a given command on all files created within the given inboxj
    directories.  Each command runs as a separate GC3Pie task.
    Task and session management is possible using the usual server
    XML-RPC interface.
    """

    # setting `version` is required to instanciate the
    # `SessionBasedDaemon` class
    version = '1.1'


    def setup_args(self):
        super(InboxProcessingDaemon, self).setup_args()

        self.add_param("config_file", metavar="config",
                       type=existing_file,
                       help="Location of preprocessing pipeline "
                       "configuration file.")
       
    def _get_inbox_from_subject(self, subject):
        """
        Return the first inbox path corresponding to the location
        of the subject
        """
        for inbox in self.params.inbox:
            if inbox.path in subject.path:
                return inbox.path
        return None

    def parse_args(self):
        super(InboxProcessingDaemon, self).parse_args()
        self.params.config_file = os.path.abspath(self.params.config_file)

    def _get_inbox_from_subject(self, subject):
        """
        Return the first inbox path corresponding to the location
        of the subject
        """
        for inbox in self.params.inbox:
            if inbox.path in subject.path:
                return inbox.path
        return None

    def _check_folder_completion_file(self, subject):
        """
        Check if subject is a folder; then in case check whether
        the termination file has been written.
        If so, return the subject_location as a valid folder
        to process.
        """
        gc3libs.log.info("Reacting to subject {0}".format(subject))
        inbox = self._get_inbox_from_subject(subject)
        if not inbox:
            gc3libs.log.error("Somehow a subject has been created and notified outside "
                              "the monitored inboxes...".format(subject.path))
            return

        if os.path.basename(subject.path).endswith(gc3apps.Default.DEFAULT_EXPERIMENT_FILE_CHECK_MARKER):
            experiment_folder = os.path.dirname(subject.path)
            experiment_folder_name = os.path.dirname(os.path.relpath(subject.path,
                                                                     inbox))

            (analysis_type, dataset_name) = gc3apps.get_dataset_information(subject)
            instrument = get_instrument(subject)

            extra = self.extra.copy()
            extra['jobname'] = "{0}_{1}".format(analysis_type,
                                                dataset_name)
            extra['dryrun'] = self.params.dryrun

            if analysis_type == 'IMC':
                self.add(
                    gc3apps.pipelines.IMCPipeline(
                        subject,
                        dataset_name,
                        instrument,
                        self.params.config_file,
                        **extra))
            elif analysis_type == 'sMC':
                self.add(
                    gc3apps.pipelines.SMCPipeline(
                        os.path.join(inbox,experiment_folder),
                        dataset_name,
                        instrument,
                        self.params.config_file,
                        **extra))
            else:
                gc3libs.log.error("Dataset {0} non valid analysis type: {1}.".format(experiment_folder,
                                                                                     analysis_type))
                return
            
    def created(self, inbox, subject):
        """
        Check whether folder has been completed with file_check marker.
        Add a new tast for each completed folder.
        """
        self._check_folder_completion_file(subject)

    def modified(self, inbox, subject):
        """
        Check whether folder has been completed with file_check marker.
        Add a new tast for each completed folder.
        """
        self._check_folder_completion_file(subject)


## main: run server

if "__main__" == __name__:
    from data_daemon import InboxProcessingDaemon
    InboxProcessingDaemon().run()
