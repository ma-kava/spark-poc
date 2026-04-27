import uuid
import h5py
from datetime import datetime
import numpy as np

class H5Extractor:
    def __init__(self, measurement_id, filepath):
        self.metadata_db = {
            "measurement_id": measurement_id,
            "source_file": filepath,
            "ingested_at": datetime.now().isoformat()
        }
        self.parquet_payload = {
            "measurement_id": [measurement_id]
        }

    # this ensure to call instance of this class like this: f(name, node)
    def __call__(self, name, node):
        if isinstance(node, h5py.Dataset):
            if node.shape == () or node.size < 100: # its a scalar obj or small array
                value = node[()]
                
                # 1. Strip away the NumPy array/scalar wrapper
                if isinstance(value, np.ndarray):
                    if value.size == 1:
                        value = value.item() # Extracts the single item into a pure Python type
                    else:
                        value = value.tolist() # Converts a multi-item array into a Python list
                elif isinstance(value, np.generic): 
                    # Catches standalone NumPy scalars (like np.bytes_ or np.int32)
                    value = value.item()
                
                # 2. Decode bytes to standard UTF-8 strings
                if isinstance(value, bytes):
                    value = value.decode('utf-8')
                elif isinstance(value, list):
                    # If it's a list, decode any byte strings inside it
                    value = [
                        v.decode('utf-8') if isinstance(v, bytes) else v 
                        for v in value
                    ]
                
                self.metadata_db[name] = value
                
            elif len(node.shape) >= 2 or node.size >= 100:
                flattened = node[:].flatten().tolist()
                self.parquet_payload[name] = [flattened]
                
        elif isinstance(node, h5py.Group):
            pass

def process_single_h5(filepath):
    """
    This function can be distributed to workers. 
    Every worker can using this fnc process h5 file.
    """

    measurement_id = str(uuid.uuid4())
    
    with h5py.File(filepath, 'r') as f:
        extractor = H5Extractor(measurement_id=measurement_id, filepath=filepath)
        f.visititems(extractor)
        
    return (extractor.metadata_db, extractor.parquet_payload)


if __name__ == "__main__":
    metadata_db, parquet_payload = process_single_h5("data/h5data.h5")
    for i, k in enumerate(metadata_db.keys()):
        if i == 5: break
        print(f'{k} = {metadata_db[k]}')
