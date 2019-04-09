import pytest
import os
from gc3apps.h5parse import CPparser

@pytest.fixture
def rootdir():
    """Return path to folders containing h5 files"""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        'test_h5_files')

def test_parse_example_batch_v318(rootdir):
    """
    Test that the CPparser parses the correct values from a Batch_data.h5 file
    """
    test_file = os.path.join(rootdir,'example_batch_v318','Batch_data.h5')
    cpp = CPparser(test_file)
    assert cpp.version == '3.1.8'    
    assert cpp.date == '2019-01-14-12-01-59'
    assert cpp.images_path == '/mnt/bbvolume/server_homes/vitoz/Data/Analysis/20190108_bfanalysis_tc_p102-105/tiffs'
    assert cpp.images_number == 5123      

def test_parse_example_csv(rootdir):
    """
    Test that the CPparser parses the correct values from a Batch_data.h5 file
    """
    test_file = os.path.join(rootdir,'example_csv','Batch_data.h5')
    cpp = CPparser(test_file)
    assert cpp.version == '3.1.5'    
    assert cpp.date == '2019-01-25-20-01-12'
    assert cpp.images_path == '/mnt/bbvolume/projects/imc_example_data/cp_batch_example/data/probabilities'
    assert cpp.images_number == 3      

