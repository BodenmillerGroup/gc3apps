# -*- coding: utf-8 -*-

"""Top-level package for gc3apps."""

__author__ = """Sergio Maffioletti"""
__email__ = 'sergio.maffioletti@uzh.ch'
__version__ = '0.1.0'

# Defaults
class Default(object):

    """
    A namespace for all constants and default values used in the
    package.
    """
    DEFAULT_FILE_CHECK_MARKER = "done.tx"
    CELLPROFILER_DONEFILE = "cp.done"
    CELLPROFILER_GROUPFILE = "cpgroups.json"
    CELLPROFILER_COMMAND = "cellprofiler -c -r -p {batch_file} -f {start} -l {end} --do-not-write-schema --plugins-directory={plugins} -o {output_folder} --done-file="+CELLPROFILER_DONEFILE
    CELLPROFILER_DOCKER_COMMAND = "docker run -v {src_mount_point}:{dest_mount_point} smaffiol/cellprofiler-3.1.8 -c -r -p {batch_file} -f {start} -l {end} --do-not-write-schema --plugins-directory={plugins} -o {output_folder} --done-file="+CELLPROFILER_DONEFILE
    CELLPROFILER_GETGROUPS_COMMAND = "cellprofiler -c --print-groups={batch_file}"


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

