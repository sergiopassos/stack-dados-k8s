import os
import pandas as pd
import tempfile
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from minio import Minio

client = Minio("4.153.0.204", "data-lake", "12620ee6-2162-11ee-be56-0242ac120002", secure=False)

data = pd.read_json('')
df = pd.DataFrame(data)
table = pa.Table.from_pandas(df)

with tempfile.NamedTemporaryFile(suffix=".parquet") as temp_file:

    pq.write_table(table, temp_file.name)
    temp_file.seek(0)

    prq_file_uuid = uuid.uuid4()
    client.put_object(
        bucket_name="landing",
        object_name=f"com.owshq.data/mssql/users/{prq_file_uuid}.parquet",
        data=temp_file,
        length=os.path.getsize(temp_file.name),
        content_type='application/octet-stream'
    )


file_path = "com.owshq.data/mssql/users/{}}.parquet"
data = client.get_object("landing", file_path)

tempfile_path = tempfile.mktemp()
with open(tempfile_path, 'wb') as out_file:
    out_file.write(data.data)

df = pd.read_parquet(tempfile_path)
print(df)

def is_parquet(pq_name):
    try:
        pd.read_parquet(pq_name)
        return True
    except Exception as e:
        return False

print(is_parquet(tempfile_path))
