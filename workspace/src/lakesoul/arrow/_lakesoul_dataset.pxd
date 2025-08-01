from pyarrow.includes.libarrow_dataset cimport CDataset
from pyarrow.includes.libarrow_dataset cimport CFragment
from pyarrow.includes.libarrow cimport CSchema

from pyarrow._dataset cimport Dataset
from pyarrow._dataset cimport Fragment

from libcpp.memory cimport shared_ptr
from libcpp.string cimport string
from libcpp.vector cimport vector


cdef extern from "lakesoul/dataset.h" namespace "lakesoul" nogil:
    cdef cppclass CLakeSoulDataset" lakesoul::LakeSoulDataset":
        CLakeSoulDataset(shared_ptr[CSchema])
        CLakeSoulDataset(shared_ptr[CSchema], CExpression)
        void AddFileUrls(const vector[string]& file_url)
        void AddPrimaryKeys(const vector[string]& pk)
        void AddPartitionKeyValue(const string& key, const string& value)
        void SetBatchSize(int batch_size)
        void SetThreadNum(int thread_num)
        void SetRetainPartitionColumns()
        void SetObjectStoreConfig(const string& key, const string& value)


cdef extern from "lakesoul/fragment.h" namespace "lakesoul" nogil:
    cdef cppclass CLakeSoulFragment" lakesoul::LakeSoulFragment":
        pass

cdef class LakeSoulDataset(Dataset):
    cdef:
        CLakeSoulDataset* lakesoul_dataset

    cdef void init(self, const shared_ptr[CDataset]& sp)

cdef class LakeSoulFragment(Fragment):
    cdef:
        CLakeSoulFragment* lakesoul_fragment

    cdef void init(self, const shared_ptr[CFragment]& sp)
