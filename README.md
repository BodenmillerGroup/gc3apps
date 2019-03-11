# gc3apps
Repository of all GC3Pie applications and utilities for bblab

# Welcome to Gcp pipeline: a custom made pipeline to run cellprofiler workflows in parallel

	## Install a standalone gcp pipeline app

	### Install required packages (gc3pie and system)

	#### Install GC3Pie
	* Grab the latest master branch
	```$ wget https://raw.githubusercontent.com/uzh/gc3pie/master/install.py```

	* Runt the script
	```$ python ~/install.py --develop -y```

	* Activate the virtualenv and generate a gc3pie.conf file
	```
	$ source ~/gc3pie/bin/activate
	$ gservers # this will generate a config file ~/.gc3/gc3pie.conf```

	* Install additional python packages
	```$ source ~/gc3pie/bin/activate && pip install -r requirements.txt```

	#### Make sure debian requied packages are installed
	```
	$ sudo apt-get update && sudo apt-get install -y
	- libffi-dev
	- libssl-dev
	- python-dev
	```

	### Install gcp pipeline
	```$ cd ~ && git clone https://github.com/BodenmillerGroup/gc3apps.git```

	## Run gcp pipeline

	* configure your gc3pie.conf file
	* identify yourself with your cloud provider
	* activate your virtualenv
	* export the gc3apps directory to the pythonpath: e.g. ```export PYTHONPATH=~/gc3apps/gc3apps:~/gc3apps:$PYTHONPATH```

	### Examples
	Run gcp pipeline in different modes:

	* cppipe file mode
	```$ python gc3apps/gc3apps/gcp_pipeline.py /path/to/file.cppipe /path/to/data/ /path/to/output/ -r ScienceCloud -s session_name -N -w 2hours -C 10```

	* batch file mode
	```$ python gc3apps/gc3apps/gcp_pipeline_batch.py /path/to/Batch_data.h5 /path/to/data/ /path/to/output/ -r ScienceCloud -s session_name -N -w 2hours -C 10```

	### Common used options 

        * Parallel processing
        To parallelize the gcp pipeline execution, for both "cppipe" and "batch" file modes, use the ```-K integer``` option. You can add this option to one of the commands in the Example section.  
