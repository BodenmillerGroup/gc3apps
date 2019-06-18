# -*- coding: utf-8 -*-

"""Top-level package for gc3apps."""

__author__ = """Sergio Maffioletti"""
__email__ = 'sergio.maffioletti@uzh.ch'
__version__ = '0.1.0'

import os
import json
import yaml
import zipfile
import gc3apps
import gc3libs
from gc3libs import Application
from gc3apps.utils.h5parse import CPparser

#####################
# Configuration


def parse_config_file(config_file):
    """
    Parse config file and verify mandatrory
    fileds have been specified
    """
    with open(config_file, 'r') as fd:
        try:
            cfg = yaml.load(fd)
            assert cfg.has_key("experiment"), "Missing 'experiment' section in config file"
            assert cfg["experiment"].has_key("raw_data_destination"), "Missing 'raw_data_destination' section in config file"
            assert cfg["experiment"].has_key("subfolders"), "Missing 'subfolders' section in config file"

            assert cfg.has_key("IMC"), "Missing IMC section in config file"
            assert cfg.has_key("sMC"), "Missing sMC section in config file"

            _parse_IMC_config(cfg['IMC'])
            _parse_sMC_config(cfg['sMC'])

            return cfg
        except Exception as ex:
	    gc3libs.log.error("Failed parsing config file {0}. "
                              "Error {1}:{2}".format(config_file,
                                                     type(ex),
                                                     str(ex)))
            pass
    return None

def _parse_IMC_config(cfg):
    """
    Parse IMC specific requirements
    """
    assert cfg.has_key("allow_append_raw_data"), "Missing 'allow_append' section in config IMC"
    assert cfg.has_key("repo"), "Missing 'repo' section in config IMC"
    assert cfg['repo'].has_key("source"), "Missing 'repo/source' section in config IMC"
    assert cfg['repo'].has_key("branch"), "Missing 'repo/branch' section in config IMC"
    assert cfg['repo'].has_key("location"), "Missing 'repo/location' section in config IMC"
    assert cfg.has_key("metadata"), "Missing 'metadata' section in config IMC"
    assert cfg.has_key("derived_subfolders"), "Missing 'derived_subfolders' section in config IMC"
    assert cfg['derived_subfolders'].has_key("analysis"), "Missing 'derived_subfolders/analysis' section in config IMC"
    assert cfg['derived_subfolders'].has_key("cellprofiler"), "Missing 'derived_subfolders/cellprofiler' section in config IMC"
    assert cfg['derived_subfolders'].has_key("ilastik"), "Missing 'derived_subfolders/ilastik' section in config IMC"
    assert cfg['derived_subfolders'].has_key("ome"), "Missing 'derived_subfolders/ome' section in config IMC"
    assert cfg['derived_subfolders'].has_key("uncertainty"), "Missing 'derived_subfolders/uncertainty' section in config IMC"
    assert cfg['derived_subfolders'].has_key("histocat"), "Missing 'derived_subfolders/histocat' section in config IMC"


def _parse_sMC_config(cfg):
    """
    Parse sMC specific requirements
    """
    assert cfg.has_key("allow_append_raw_data"), "Missing 'allow_append' section in config sMC"
    assert cfg.has_key("repo"), "Missing 'repo' section in config sMC"
    assert cfg['repo'].has_key("source"), "Missing 'repo/source' section in config sMC"
    assert cfg['repo'].has_key("branch"), "Missing 'repo/branch' section in config sMC"
    assert cfg['repo'].has_key("location"), "Missing 'repo/location' section in config sMC"
    assert cfg.has_key("panels"), "Missing 'panels' section in config sMC"

# Defaults
class Default(object):

    """
    A namespace for all constants and default values used in the
    package.
    """
    
    # Suffixes
    CSV_SUFFIX = '.csv'

    # Generic configs
    DEFAULT_BBSERVER_MOUNT_POINT = "/mnt/bbvolume"
    DEFAULT_FILE_CHECK_MARKER = "done.txt"
    DEFAULT_EXPERIMENT_FILE_CHECK_MARKER = ".zip"
    DEFAULT_OUTPUT_FOLDER = "output"

    # GQTL
    QTL_COMMAND = "sudo docker run -v {data}:/data -v {output}:/output bblab/qtl:{version} {phenotype} /data /output -b {batches} -p {permutations} -i {imputations} -t {trees} -m {mafthres} -l {last}"

    # CellProfiler
    CELLPROFILER_DONEFILE = "cp.done"
    CELLPROFILER_GROUPFILE = "cpgroups.json"
    DEFAULT_CELLPROFILER_DOCKER = "bblab/cellprofiler:3.1.8"
    CELLPROFILER_COMMAND = "cellprofiler -c -r -p {batch_file} -f {start} -l {end} --do-not-write-schema --plugins-directory={plugins} -o {output_folder} --done-file="+CELLPROFILER_DONEFILE
    CELLPROFILER_DOCKER_COMMAND = "sudo docker run -v {batch_file}:{batch_file} -v {data_mount_point}:{data_mount_point} -v {output_folder}:/output {docker_image} -c -r -p {batch_file} -f {start} -l {end} --do-not-write-schema --plugins-directory={plugins} -o /output --done-file=/output/"+CELLPROFILER_DONEFILE
    CELLPROFILER_GETGROUPS_COMMAND = "sudo docker run -v {batch_file}:{batch_file} {docker_image} -c --print-groups={batch_file}"

    GET_CP_GROUPS_FILE = "cp_pipeline_get_groups.sh"
    GET_CP_GROUPS_CMD = "./" + GET_CP_GROUPS_FILE + " -o {output} -p {pipeline} -i {image_data} -w {cp_plugins} -d {docker_image}"

    # Ilastik
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

def get_instrument(location):
    """
    **Warning**:
    This method makes strong assumptions on foldername
    folder containing filename identifies instrument
    expected location format: '/source_prefix/instrument/dataset_name.zip'
    """
    return os.path.dirname(location).split(os.path.sep)[-1]

def get_dataset_info(location):
    """
    Search in location for indicator of analysis type.
    Current algorithm:
    * if .fcs file then analysis type sMC
    * if .mcd file then analysis type IMC
    @param: location of raw data in .zip format
    @ return: [IMC, sMC, None]
    """
    
    instrument = get_instrument(location)
    
    # list content of .zip data and return analysis type
    with zipfile.ZipFile(location) as data:
        dataset = data.namelist()
        # get analysis type
        if [dd for dd in dataset if dd.lower().endswith("fcs")]:
            return ("sMC",os.path.dirname(dataset[0]))
        if [dd for dd in dataset if dd.lower().endswith("mcd")]:
            return ("IMC",os.path.dirname(dataset[0]))
    return None,None

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

class PrepareFolders(Application):

    PREPROCESSING = "preprocessing.py"

    """
    parse metadata and create destination folder accordingly
    """
    def __init__(self, data_location, destination, subfolders, analysis_type, raw_data_destination, **extra_args):
        """
        """
        cmd = ["python",
               os.path.basename(PrepareFolders.PREPROCESSING),
               data_location,
               destination,
               analysis_type,
               "-f",
               ",".join(subfolders),
               "-r",
               raw_data_destination,
               "-vvvv",
        ]

        if extra_args.has_key('dryrun') and extra_args['dryrun']:
            cmd.append("--dryrun")

        Application.__init__(
            self,
            arguments = cmd,
            inputs = [os.path.join(whereami,'etc',PrepareFolders.PREPROCESSING)],
            outputs = [],
            stdout = 'log',
            join=True,
            executables=[],
            **extra_args)

class IMCPreprocessing(Application):
    """
    Parse .mcd and .txt files to extract metadata
    store metadata in .csv file
    convert images into ometiff
    """

    application = 'imc_preprocessing'
    RUNNER = "imc_preprocessing.py"

    def __init__(self, data_source, raw_data_location, metadata_location, imc_cfg, **extra_args):
        cmd = ["python",
               os.path.basename(IMCPreprocessing.RUNNER),
               data_source,
               "--raw",
               raw_data_location,
               "--scripts",
               metadata_location,
               "--ometiff",
               imc_cfg['derived_subfolders']['ome'],
               "--analysis",
               imc_cfg['derived_subfolders']['analysis'],
               "--cellprofiler",
               imc_cfg['derived_subfolders']['cellprofiler'],
               "-vvvv",
        ]
            
        # gc3libs.log.info("Running: '{0}'".format(cmd))
        Application.__init__(
            self,
            arguments = cmd,
            inputs = [os.path.join(whereami,'etc',IMCPreprocessing.RUNNER)],
            outputs = [],
            stdout = 'log',
            join=True,
            executables=[],
            **extra_args)
        
        
class QTLApplication(Application):
    """
    Run celllineQTL at scale
    """
    application_name = 'qtl'

    def __init__(self, phenotype, path, batches, permutations, imputations, trees, mafthres, last, version, **extra_args):
        """
        Crate GC3Pie application object by specifying the dictionary with
        command line argument, input/output requirements.
        """

        inputs[path] = os.path.basename(path)
        outputs.append(inputs[path])

        cmd = gc3apps.Default.QTL_COMMAND.format(version=version,
                                                 phenotype=phenotype,
                                                 data="$PWD/{0}".format(inputs[path]),
                                                 output="$PWD/{0}".format(inputs[path]),
                                                 batches=batches,
                                                 permutations=permutations,
                                                 imputations=imputations,
                                                 trees=trees,
                                                 mafthres=mafthres,
                                                 last=last)
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

    def __init__(self, batch_file, input_folder, output_folder, start_index, end_index, cp_plugins, **extra_args):

        inputs = dict()
        outputs = []

        self.docker_image = gc3apps.Default.DEFAULT_CELLPROFILER_DOCKER
        cparser = CPparser(batch_file)
        inputs[batch_file] = os.path.basename(batch_file)
        if extra_args["docker_image"]:
            self.docker_image = extra_args["docker_image"]

        self.output_folder = output_folder

        command = gc3apps.Default.CELLPROFILER_DOCKER_COMMAND.format(batch_file="$PWD/{0}".format(inputs[batch_file]),
                                                                     data_mount_point=input_folder,
                                                                     docker_image = self.docker_image,
                                                                     start=start_index,
                                                                     end=end_index,
                                                                     output_folder=output_folder,
                                                                     plugins=cp_plugins)

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
        self.csv_results = [data for data in os.listdir(self.output_folder) if data.endswith(gc3apps.Default.CSV_SUFFIX)]

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

	# gc3libs.log.debug("In RunCellprofilerGetGroups running {0}.".format(cmd))

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

