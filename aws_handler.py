import boto3
import tarfile
import os.path
from os import getenv
from pathlib import Path
from typing import Union
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError


load_dotenv()

AWS_ACCESS_KEY = getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = getenv("AWS_SECRET_KEY")


def make_tarfile(output_filename: str, source_dir: Union[Path, str]):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))


def upload_to_aws(
    local_file: Union[Path, str], bucket: str, s3_file: Union[Path, str] = None
):
    s3 = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY
    )
    try:
        if not s3_file:
            s3_file = Path(local_file).name
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


if __name__ == "__main__":
    uploaded = upload_to_aws("img.tar.gz", "zikkie-ml-repo")
