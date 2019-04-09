# -*- coding: utf-8 -*-

"""Top-level package for gc3apps."""

__author__ = """Sergio Maffioletti"""
__email__ = 'sergio.maffioletti@uzh.ch'
__version__ = '0.1.0'

import os
import json
import gc3apps
import gc3libs
from gc3libs import Application
from gc3apps.utils.h5parse import CPparser

#####################
# Configuration


# Defaults
class Default(object):

    """
    A namespace for all constants and default values used in the
    package.
    """
    CSV_SUFFIX = '.csv'
    DEFAULT_BBSERVER_MOUNT_POINT = "/mnt/bbvolume"
    DEFAULT_FILE_CHECK_MARKER = "done.txt"
    QTL_COMMAND = "docker run -v {data}:/data -v {output}:/output bblab/qtl:{qtl_version} {phenotypeName} /data /output -f {forests} -t {trees} -s {scores} -p {permutations} -m {threshold}"
    CELLPROFILER_DONEFILE = "cp.done"
    CELLPROFILER_GROUPFILE = "cpgroups.json"
    DEFAULT_CELLPROFILER_DOCKER = "bblab/cellprofiler:3.1.8"
    CELLPROFILER_COMMAND = "cellprofiler -c -r -p {batch_file} -f {start} -l {end} --do-not-write-schema --plugins-directory={plugins} -o {output_folder} --done-file="+CELLPROFILER_DONEFILE
    CELLPROFILER_DOCKER_COMMAND = "sudo docker run -v {batch_file}:{batch_file} -v {data_mount_point}:{data_mount_point} -v {output_folder}:/output {docker_image} -c -r -p {batch_file} -f {start} -l {end} --do-not-write-schema --plugins-directory={plugins} -o /output --done-file=/output/"+CELLPROFILER_DONEFILE
    CELLPROFILER_GETGROUPS_COMMAND = "sudo docker run -v {batch_file}:{batch_file} {docker_image} -c --print-groups={batch_file}"

    GET_CP_GROUPS_FILE = "cp_pipeline_get_groups.sh"
    GET_CP_GROUPS_CMD = "./" + GET_CP_GROUPS_FILE + " -o {output} -p {pipeline} -i {image_data} -w {cp_plugins} -d {docker_image}"
    CELLPROFILER_DOCKER_COMMAND = "sudo docker run -v {batch_file}:{batch_file} {CP_MOUNT_POINT} {docker_image} -c -r -p {batch_file} -f {start} -l {end} --do-not-write-schema --plugins-directory={plugins} -o /output --done-file=/output/"+CELLPROFILER_DONEFILE

    DEFAULT_ILASTIK_DOCKER = "ilastik/ilastik-from-binary:1.3.2b3"
    ILASTIK_DOCKER_COMMAND = 'sudo docker run -v {project_file}:{project_file} -v {data_mount_point}:{data_mount_point} -v {output_folder}:/output ' \
            '{docker_image} ' \
            './run_ilastik.sh ' \
            '--headless --project=/{project_file} ' \
            '--output_format=tiff '\
            '--output_filename_format=/output/{output_filename} '\
            '--export_source {export_source} '\
            '--export_dtype {export_dtype} ' \
            '--pipeline_result_drange="(0.0, 1.0)" '\
            '{input_files}'

#####################
# Utilities
#

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

whereami = os.path.dirname(os.path.abspath(__file__))

#####################
# Applications
#

class NotifyApplication(Application):
    """
    Test application to verify the pre-processing step
    To be used in conjunction with data_daemon and triggered
    when a new dataset is added to the dataset folder
    """

    def __init__(self, data_location, analysis_type, config_file, **extra_args):

        Application.__init__(
            self,
            arguments = ["/bin/true"],
            inputs = [],
            outputs = [],
            stdout = 'log',
            join=True,
            executables=[],
            **extra_args)


class QTLApplication(Application):
    """
    Run celllineQTL at scale
    """
    def __init__(self, phenotypeName, dataDirPath, forests, trees, scores, permutations, threshold, qtl_version, **extra_args):

        inputs = dict()
        outputs = []

        output.append(inputs[dataDirPath])
        inputs[dataDirPath] = os.path.basename(dataDirPath)

        cmd = gc3apps.Default.QTL_COMMAND.format(output="$PWD/{0}".format(inputs[dataDirPath]),
                                                 data="$PWD/{0}".format(inputs[dataDirPath]),
                                                 qtl_version=qtl_version,
                                                 phenotypeName=phenotypeName,
                                                 trees=trees,
                                                 forests=forests,
                                                 scores=scores,
                                                 permutations=permutations,
                                                 threshold=threshold)

        Application.__init__(
            self,
            arguments = cmd,
            inputs = inputs,
            outputs = outputs,
            stdout = 'log',
            join=True,
            executables=[],
            **extra_args)


class RunCellprofiler(Application):
    """
    Run Cellprofiler in batch mode
    """

    application_name = 'runcellprofiler'

    def __init__(self, batch_file, output_folder, start_index, end_index, cp_plugins, **extra_args):

        inputs = dict()
        outputs = []

        self.docker_image = gc3apps.Default.DEFAULT_CELLPROFILER_DOCKER
        cparser = CPparser(batch_file)
        inputs[batch_file] = os.path.basename(batch_file)
        if extra_args["docker_image"]:
            self.docker_image = extra_args["docker_image"]

        self.output_folder = output_folder

        command = gc3apps.Default.CELLPROFILER_DOCKER_COMMAND.format(batch_file="$PWD/{0}".format(inputs[batch_file]),
                                                                     data_mount_point=gc3apps.Default.DEFAULT_BBSERVER_MOUNT_POINT,
                                                                     docker_image = self.docker_image,
                                                                     start=start_index,
                                                                     end=end_index,
                                                                     output_folder=output_folder,
                                                                     plugins=cp_plugins,
                                                                     MOUNT_POINT=mount_point)

        Application.__init__(
            self,
            arguments = command,
            inputs = inputs,
            outputs = [],
            stdout = 'log',
            join=True,
            executables=[],
            **extra_args)

    def terminated(self):
        """
        Check if results have been generated
        create list of .csv files in case
        """
        self.csv_results = [data for data in os.listdir(self.output_folder) if data.endwith(gc3apps.Default.CSV_SUFFIX)]

class RunCellprofilerGetGroups(Application):
    """
    Run Cellprofiler in batch mode and get images groups information
    @params: Cellprofiler HD5 batch file
    @returns: .json file containing image group information
    """

    application_name = 'runcellprofiler'

    def __init__(self, pipeline, image_data_folder, plugins, **extra_args):

        inputs = dict()
        outputs = ["./output/"]

        inputs[pipeline] = os.path.basename(pipeline)
        inputs[os.path.join(whereami,
                            "etc",
                            gc3apps.Default.GET_CP_GROUPS_FILE)] = gc3apps.Default.GET_CP_GROUPS_FILE

        self.json_file = os.path.join(extra_args['output_dir'],"result.json")
        self.batch_file = os.path.join(extra_args['output_dir'], "Batch_data.h5")
        self.docker_image = gc3apps.Default.DEFAULT_CELLPROFILER_DOCKER
        if extra_args["docker_image"]:
            self.docker_image = extra_args["docker_image"]

        cmd = gc3apps.Default.GET_CP_GROUPS_CMD.format(output="./output",
                                                       pipeline=inputs[pipeline],
                                                       image_data=image_data_folder,
                                                       cp_plugins=plugins,
                                                       docker_image=self.docker_image)

	gc3libs.log.debug("In RunCellprofilerGetGroups running {0}.".format(cmd))

        Application.__init__(
            self,
            arguments = cmd,
            inputs = inputs,
            outputs = outputs,
            stdout = "log.out",
            stderr = "log.err",
            join=False,
            executables=["./{0}".format(gc3apps.Default.GET_CP_GROUPS_FILE)],
            **extra_args)


    def terminated(self):
        """
        Check presence of output log and verify it is
        a legit .json file
        Do not trust cellprofiler exit code (exit with 1)
        """

	gc3libs.log.debug("In RunCellprofilerGetGroups running 'termianted'.")

        try:
            with open(self.json_file,"r") as fd:
                try:
                    data = json.load(fd)
                    if len(data) > 0:
                        self.execution.returncode = (0, 0)
                except ValueError as vx:
                    # No valid json
                    gc3libs.log.error("Failed parsing {0}. No valid json.".format(self.json_file))
                    self.execution.returncode = (0,1)
        except IOError as ix:
            # Json file not found
            gc3libs.log.error("Required json file at {0} was not found".format(self.json_file))


class RunCellprofilerGetGroupsWithBatchFile(Application):
    """
    Run Cellprofiler in batch mode and get images groups information
    @params: Cellprofiler HD5 batch file
    @returns: .json file containing image group information
    """

    application_name = 'runcellprofilerwithbatchfile'

    def __init__(self, batch_file, **extra_args):

        inputs = dict()
        outputs = ["./output/"]

        self.docker_image = gc3apps.Default.DEFAULT_CELLPROFILER_DOCKER
        if extra_args["docker_image"]:
            self.docker_image = extra_args["docker_image"]

        command = gc3apps.Default.CELLPROFILER_GETGROUPS_COMMAND.format(batch_file=batch_file,
                                                                        docker_image=self.docker_image)

	gc3libs.log.debug("In RunCellprofilerGetGroups running {0}.".format(cmd))

        Application.__init__(
            self,
            arguments = cmd,
            inputs = inputs,
            outputs = outputs,
            stdout = "log.out",
            stderr = "log.err",
            join=False,
            executables=["./{0}".format(gc3apps.Default.GET_CP_GROUPS_FILE)],
            **extra_args)

    def terminated(self):
        """
        Check presence of output log and verify it is
        a legit .json file
        Do not trust cellprofiler exit code (exit with 1)
        """

	gc3libs.log.debug("In RunCellprofilerGetGroups running 'termianted'.")

        try:
            with open(os.path.join(self.output_dir,self.stdout),"r") as fd:
                try:
                    data = json.load(fd)
                    if len(data) > 0:
                        self.execution.returncode = (0, 0)
                except ValueError as vx:
                    # No valid json
                    gc3libs.log.error("Failed parsing {0}. No valid json.".format(os.path.join(self.output_dir,self.stdout)))
                    self.execution.returncode = (0,1)
        except IOError as ix:
            # Json file not found
            gc3libs.log.error("Required json file at {0} was not found".format(os.path.join(self.output_dir,self.stdout)))

class RunIlastik(Application):
    """
    Run Ilastik in batch mode
    """

    application_name = 'runilastik'

    def __init__(self, project_file, input_files, output_folder, export_source,
            export_dtype, output_filename, **extra_args):

        inputs = dict()
        outputs = []

        self.docker_image = gc3apps.Default.DEFAULT_ILASTIK_DOCKER
        inputs[project_file] = os.path.basename(project_file)

        if extra_args["docker_image"]:
            self.docker_image = extra_args["docker_image"]

        if output_filename is None:
            outtype = filter(str.isalnum, export_source)
            output_filename =  '{{nickname}}_{outtype}.tiff'.format(
                    outtype=outtype)

        input_file_string = ' '.join(input_files)

        command = gc3apps.Default.ILASTIK_DOCKER_COMMAND.format(
            project_file="$PWD/{0}".format(inputs[project_file]),
            data_mount_point=gc3apps.Default.DEFAULT_BBSERVER_MOUNT_POINT,
            docker_image = self.docker_image,
            input_files=input_file_string,
            output_folder=output_folder,
            export_source=export_source,
            export_dtype=export_dtype,
            output_filename=output_filename
        )

        Application.__init__(
            self,
            arguments = command,
            inputs = inputs,
            outputs = [],
            stdout = 'log',
            join=True,
            executables=[],
             **extra_args)

