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
        self.obj = h5py.File(path,'r')
	self.date = self.obj['/Measurements'].keys()[0]

	self.group = os.path.join('Measurements',
              	                  self.date,
                	       	  'Experiment',
                                  'CellProfiler_Version',
                                  'data')

	self.version = self.obj[self.group][0]
	self._verify_version(self.version)

	self.images_number = len(self.obj[self._get_image_group()])
        self.images_path = self.obj[self._get_image_group()][0]
	
 	self.obj.close()

    def _verify_version(self, h5version):
        """
        Return Cellprofiler version
        """
        assert h5version in self.SUPPORTED_VERSIONS, "Cellprofiler {0} version not supported.".format(h5version)

    def _get_image_group(self):
        """
        Return group to images
        """
        group = os.path.join('Measurements',
                             self.date,
                             'Image',
                             'PathName_bfimage',
                             'data')

        if self.version == '3.1.5': 
            temp = group.split('/')
            temp[3] = 'PathName_FullStack'
            group = os.path.join(*temp)

        return group

def h5_obj_print(path):
    """
    Print HDF5 file metadata
    """
    cp = CPparser(path)         

    print 'Experiment: {0}'.format(cp.date)
    print 'CellProfiler Version: {0}'.format(cp.version)
    print 'Number of images: {0}'.format(cp.images_number)
    print 'Path to images: {0}'.format(cp.images_path)
    print
