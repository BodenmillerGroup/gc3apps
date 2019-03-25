<<<<<<< HEAD
#!/bin/bash
=======
#!/bin/bash -x
>>>>>>> sergio: wrapper Cellprofiler script to get generate Batch file and get groups

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


# -*- coding: utf-8 -*-

me=$(basename "$0")

## defaults
output=`realpath ./output`
pipeline=`realpath ./pipeline.cppipe`
images=`realpath ./images`
plugins=`realpath ./plugins`
mode_list="get_groups run"
<<<<<<< HEAD
dockerimage="bblab/cellprofiler:3.1.8"
=======
>>>>>>> sergio: wrapper Cellprofiler script to get generate Batch file and get groups

## helper functions

function die () {
    rc="$1"
    shift
    (echo -n "$me: ERROR: ";
        if [ $# -gt 0 ]; then echo "$@"; else cat; fi) 1>&2
    exit $rc
}

<<<<<<< HEAD
function check_mount () {
    sleep 10
    mountpoint -q $1
    if  [ $? -ne 0 ]; then
	echo "DBG: Mount point '$1' not valid"
	exit 1
    fi
}

=======
>>>>>>> sergio: wrapper Cellprofiler script to get generate Batch file and get groups
function check_mode () {
    if [[ $mode_list =~ (^|[[:space:]])$1($|[[:space:]]) ]]; then
	return 0
    else
	echo "Selected mode '$1' not in list '$mode_list'"
	exit 1
    fi
}

function generate_file_list () {
    echo -n "Generating filelist $2 ... "
<<<<<<< HEAD
    find $1 -type f -print > $2
=======
    find $1 -type f -print >> $2
>>>>>>> sergio: wrapper Cellprofiler script to get generate Batch file and get groups
    if [ $? -ne 0 ]; then
	echo "[failed]"
	echo "DBG: Failed generating filelist '$2' from '$1'"
	exit 1
    fi
    echo ["ok"]
}

function get_cp_groups () {
    pipeline=$1
    output=$2
    images=$3
    plugins=$4

}


## usage info

usage () {
    cat <<__EOF__
Usage:
  $me [options]

Run CellProfiler to generate group file.

Options:
  -v            Enable verbose logging
  -h            Print this help text
  -o		Output folder
  -p		Location of .cppipe file
  -i		Images location
  -w		Additional CellProfiler Plugins folder
<<<<<<< HEAD
  -d		Docker image
=======
>>>>>>> sergio: wrapper Cellprofiler script to get generate Batch file and get groups
__EOF__
}


<<<<<<< HEAD
###############################
# main
#


## check mountpoint
check_mount /mnt/bbvolume


## parse command-line

short_opts='hvo:p:i:w:d:'
long_opts='help,verbose,output,pipeline,images,plugins,docker'
=======
## parse command-line

short_opts='hvo:p:i:w:'
long_opts='help,verbose,output,pipeline,images,plugins'
>>>>>>> sergio: wrapper Cellprofiler script to get generate Batch file and get groups

getopt -T > /dev/null
rc=$?
if [ "$rc" -eq 4 ]; then
    # GNU getopt
    args=$(getopt --name "$me" --shell sh -l "$long_opts" -o "$short_opts" -- "$@")
    if [ $? -ne 0 ]; then
        die 1 "Type '$me --help' to get usage information."
    fi
    # use 'eval' to remove getopt quoting
    eval set -- $args
else
    # old-style getopt, use compatibility syntax
    args=$(getopt "$short_opts" "$@")
    if [ $? -ne 0 ]; then
        die 1 "Type '$me --help' to get usage information."
    fi
    set -- $args
fi

while [ $# -gt 0 ]; do
    case "$1" in
        --verbose|-v)  verbose='--verbose' ;;
        --help|-h)     usage; exit 0 ;;
	--output|-o)   output=`realpath $2`; shift ;;
	--pipeline|-p) pipeline=`realpath $2`; shift ;;
	--images|-i)   images=`realpath $2`; shift ;;
	--plugins|-w)  plugins=`realpath $2`; shift ;;
<<<<<<< HEAD
	--docker|-d)   dockerimage=$2; shift ;;	
=======
>>>>>>> sergio: wrapper Cellprofiler script to get generate Batch file and get groups
        --)            shift; break ;;
    esac
    shift
done


<<<<<<< HEAD
=======
## sanity checks


>>>>>>> sergio: wrapper Cellprofiler script to get generate Batch file and get groups
## main
echo "=== ${me}: Starting at `date '+%Y-%m-%d %H:%M:%S'`"

if ! [ -d $output ]; then
    mkdir -p $output
fi

# create filelist.txt
generate_file_list $images $output/filelist.txt

<<<<<<< HEAD
cmd="sudo docker run -v ${output}:/output -v ${pipeline}:/tmp/pipeline.cppipe -v ${output}/filelist.txt:/tmp/filelist.txt -v ${plugins}:${plugins}:ro -v ${images}:${images}:ro ${dockerimage} -c -r --file-list=/tmp/filelist.txt --plugins-directory ${plugins} -p /tmp/pipeline.cppipe -o /output --done-file=/output/done.txt"
=======
cmd="sudo docker run -v ${output}:/output -v ${pipeline}:/tmp/pipeline.cppipe -v ${output}/filelist.txt:/tmp/filelist.txt -v ${plugins}:${plugins}:ro -v ${images}:${images}:ro bblab/cellprofiler:3.1.8 cellprofiler -c -r --file-list=/tmp/filelist.txt --plugins-directory ${plugins} -p /tmp/pipeline.cppipe -o /output --done-file=/output/done.txt"
>>>>>>> sergio: wrapper Cellprofiler script to get generate Batch file and get groups
echo -n "Generating CellProfiler Batch ... "
$cmd 1>${output}/log 2>${output}/err

if ! [ -s ${output}/Batch_data.h5 ]; then
    echo "[failed: check ${output}/err]"
    exit 1
fi
echo ["ok"]

echo -n "Generating CellProfiler groups ... "
<<<<<<< HEAD
cmd="sudo docker run -v ${output}:/output ${dockerimage} -c --print-groups=/output/Batch_data.h5"
=======
cmd="sudo docker run -v ${output}/Batch_data.h5:/tmp/Batch_data.h5 bblab/cellprofiler:3.1.8 cellprofiler -c --print-groups=/tmp/Batch_data.h5"
>>>>>>> sergio: wrapper Cellprofiler script to get generate Batch file and get groups
$cmd 1>${output}/result.json 2>>${output}/err

if ! [ -s ${output}/result.json ]; then
    echo "[failed: check ${output}/err]"
    exit 1
fi
echo ["ok"]

echo "=== ${me}: Ended at `date '+%Y-%m-%d %H:%M:%S'`"


