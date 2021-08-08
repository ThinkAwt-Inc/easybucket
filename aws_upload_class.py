import os
import tarfile
import boto3
from botocore.exceptions import NoCredentialsError

class AWSclient:

  def __init__(self, access_key, secret_key, bucket_name):
    self.access_key = access_key
    self.secret_key = secret_key
    self.bucket_name = bucket_name
  @staticmethod
  def tar_folder(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))
  
  def upload_to_aws(self, local_file_path, s3_name):
    s3 = boto3.client('s3', aws_access_key_id=self.access_key,
                      aws_secret_access_key=self.secret_key)

    try:
        s3.upload_file(local_file_path, self.bucket_name, s3_name)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False