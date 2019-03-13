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
    DEFAULT_CELLPROFILER_DOCKER = "bblab/cellprofiler:3.1.8"
    CELLPROFILER_COMMAND = "cellprofiler -c -r -p {batch_file} -f {start} -l {end} --do-not-write-schema --plugins-directory={plugins} -o {output_folder} --done-file="+CELLPROFILER_DONEFILE
    CELLPROFILER_DOCKER_COMMAND = "sudo docker run -v {batch_file}:{batch_file} -v {data_mount_point}:{data_mount_point} -v {output_folder}:/output {docker_image} -c -r -p {batch_file} -f {start} -l {end} --do-not-write-schema --plugins-directory={plugins} -o /output --done-file=/output/"+CELLPROFILER_DONEFILE
    CELLPROFILER_GETGROUPS_COMMAND = "sudo docker run -v {batch_file}:{batch_file} {docker_image} -c --print-groups={batch_file}"

    GET_CP_GROUPS_FILE = "cp_pipeline_get_groups.sh"
    GET_CP_GROUPS_CMD = "./" + GET_CP_GROUPS_FILE + " -o {output} -p {pipeline} -i {image_data} -w {cp_plugins} -d {docker_image}"
    DEFAULT_ILASTIK_DOCKER = "ilastik/ilastik-from-binary:1.3.2b3"
    ILASTIK_DOCKER_COMMAND = 'sudo docker run -v {project_file}:{project_file} -v {data_mount_point}:{data_mount_point} -v {output_folder}:/output ' \
            '{docker_image} ' \
            './run_ilastik.sh ' \
            '--headless --project={project_file} ' \
            '--output_format=tiff '\
            '--output_filename_format=/output/{output_filename} '\
            '--export_source {export_source} '\
            '--export_dtype {export_dtype} ' \
            '--pipeline_result_drange="(0.0, 1.0)" '\
            '{input_files}'


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

