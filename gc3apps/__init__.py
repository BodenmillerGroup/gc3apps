# -*- coding: utf-8 -*-

"""Top-level package for gc3apps."""

__author__ = """Sergio Maffioletti"""
__email__ = 'sergio.maffioletti@uzh.ch'
__version__ = '0.1.0'

import os

# Defaults
class Default(object):

    """
    A namespace for all constants and default values used in the
    package.
    """
    DEFAULT_BBSERVER_MOUNT_POINT = "/mnt/bbvolume"
    DEFAULT_FILE_CHECK_MARKER = "done.txt"
    CELLPROFILER_DONEFILE = "cp.done"
    CELLPROFILER_GROUPFILE = "cpgroups.json"
    CELLPROFILER_COMMAND = "cellprofiler -c -r -p {batch_file} -f {start} -l {end} --do-not-write-schema --plugins-directory={plugins} -o {output_folder} --done-file="+CELLPROFILER_DONEFILE
    CELLPROFILER_DOCKER_COMMAND = "sudo docker run -v {batch_file}:{batch_file} -v {data_mount_point}:{data_mount_point} -v {output_folder}:/output sparkvilla/cellprofiler:3.1.8 -c -r -p {batch_file} -f {start} -l {end} --do-not-write-schema --plugins-directory={plugins} -o /output --done-file=/output/"+CELLPROFILER_DONEFILE
    CELLPROFILER_GETGROUPS_COMMAND = "sudo docker run -v {batch_file}:{batch_file} sparkvilla/cellprofiler:3.1.8 cellprofiler -c --print-groups={batch_file}"


# Utilities

def get_analysis_type(location):
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

