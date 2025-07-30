from typing import Any

import pyarrow as pa
import pyarrow._dataset

class DataSet(pa._dataset.DataSet): # type: ignore
    def __init__(self):
        pass

    def __reduce__(self) ->str | tuple[Any, ...]: # type: ignore
        pass

    def _get_fragments(self,filter):
        pass
    

def lakesoul_dataset():
    return DataSet()