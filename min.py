from minio import Minio
from minio.error import ResponseError
import gzip
minioClient = Minio('staas-uat.s7.aero',
            access_key='eos-uat-aiops-1-stas-ro',
            secret_key='JXzyoPg6hZClQ0hji8Hxwfbe',
            secure=True)
'''
objects = minioClient.list_objects('eos-uat-aiops-1')
for obj in objects:
    print(obj.bucket_name, obj.object_name.encode('utf-8'), obj.last_modified,
          obj.etag, obj.size, obj.content_type)
'''
data = minioClient.get_object('eos-uat-aiops-1', '/topics/VRN_AIMS_Crew---management_Terminal/2020/02/07/VRN_AIMS_Crew---management_Terminal+0+0295389525.json.gz')
with open('my-testfile.gz', 'wb') as file_data:
    for d in data.stream():
        file_data.write(d)


            #/topics/VRN_AIMS_Crew---management_Terminal/2020/02/07pip install boto3 --upgrade
import gzip
f = gzip.open('my-testfile.json.gz', 'rb')
file_content = f.read()
f.close()
#print(file_content)
import os
os.system('gunzip my-testfile.json.gz')