import gc3apps
import os
import h5py

def descend_obj(obj,sep='\t'):
    """
    Iterate through groups in a HDF5 file and prints the groups and datasets names and datasets attributes
    """
    if type(obj) in [h5py._hl.group.Group,h5py._hl.files.File]:
        for key in obj.keys():
            print sep,'-',key,':',obj[key]
            descend_obj(obj[key],sep=sep+'\t')
    elif type(obj)==h5py._hl.dataset.Dataset:
        for key in obj.attrs.keys():
            print sep+'\t','-',key,':',obj.attrs[key]

def h5dump(path,group='/'):
    """
    print HDF5 file metadata
    group: you can give a specific group, defaults to the root group
    """
    with h5py.File(path,'r') as f:
        descend_obj(f[group])

########################################################################
# Parse CellProfiler h5 file
########################################################################
class CPparser(object):
    """
    Class to parse a CellProfiler h5 file
    """

    SUPPORTED_VERSIONS = ['3.1.8','3.1.5']

    def __init__(self, path):
        self.path = path
	assert os.path.isfile(path), "File {0} no found.".format(path)

        with h5py.File(path,'r') as obj:
	    self.date = obj['/Measurements'].keys()[0]

	    self.group = os.path.join('Measurements',
                                      self.date,
                                      'Experiment',
                                      'CellProfiler_Version',
                                      'data')

	    self.version = obj[self.group][0]
	    self._verify_version(self.version)

            group_image = os.path.join('/Measurements',
                                       self.date,
                                       'Image')

            group_paths = [os.path.join(group_image,field,'data') for field in obj[group_image].keys() \
                          if field.startswith('PathName')]

            # Extract the real paths to data
            self.paths = [obj[group_path][0] for group_path in group_paths]
            # Get the path of the data root folder
	    for path in self.paths:
	        assert os.path.isdir(path), "Path {0} not found".format(path)
 
    def __repr__(self):
	return """
Experiment: {0}
CellProfiler Version: {1}
Paths to images: {2}
	""".format(self.date,self.version,self.paths)

        # return "Experiment: {0}\tCellProfiler Version: {1}\tPaths to images: {2}".format(self.date,
        #                                                     			         self.version,
	#								                 self.paths)


    def _verify_version(self, h5version):
        """
        Return Cellprofiler version
        """
        assert h5version in self.SUPPORTED_VERSIONS, "Cellprofiler {0} version not supported.".format(h5version)

if __name__ == "__main__":
    cp = CPparser('./tests/test_h5_files/example_grouping/Batch_data.h5')
