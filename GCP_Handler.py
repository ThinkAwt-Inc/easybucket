import json
import uuid
from io import BytesIO
from google.oauth2 import service_account
from google.cloud.storage.bucket import Bucket
from google.oauth2.service_account import Credentials


class CustomGCPClient:

    """
    Simple Class to allow interacting with GCP and KMS easy.
    The file upload and downlod functions would be the only public interface.
    every other function are private and levraged by the file upload/download
    interfaces.

    This client is not generic and was built specifically for uploading json files
    to GCP.

    args:
        project_id: GCP project id
        service_account_cred: Path to the sevrice account credential
        bucket_name: Name of bucket in GCP GCS
    """

    def __init__(self, project_id, service_account_cred, bucket_name):
        self.project_id = project_id
        self._credentials = self._load_credentials(service_account_cred)
        self._bucket = self._get_bucket(self._credentials, project_id, bucket_name)

    @staticmethod
    def _load_credentials(file_path: str) -> Credentials:
        credentials = service_account.Credentials.from_service_account_file(file_path)
        return credentials

    @staticmethod
    def _get_bucket(credential, proj_id, bucket_name) -> Bucket:
        storage_client = storage.Client(project=proj_id, credentials=credential)
        bucket = storage_client.get_bucket(bucket_name)
        return bucket

    def upload_to_bucket(self, json_dict: dict) -> str:
        """
        takes loaded json files from user and uploads it to gcp
        since we're using script on an api we generate file name
        as an id which is then saved in our database.

        the keyfile_id serves as a reference when we want to dowload the
        file from gcp.

        args:
            json_dict: the file to be uploaded => json.loads(user_json_file)
        """
        # generate keyfile_id
        keyfile_id = str(uuid.uuid4())

        # convert to bytes
        file_ = json.dumps(json_dict)
        file_byte = file_.encode("utf-8")
        file_name = keyfile_id + ".json"

        # upload to gcp
        document_blob = self._bucket.blob("{}".format(file_name))
        document_blob.upload_from_file(BytesIO(file_byte))

        return keyfile_id

    def download_from_bucket(self, file: str) -> dict:
        """
        takes a file name and checks gcp if the file exists
        it then downloads the file.

        args:
            file: The name of the file in your gcp bucket
        """
        document_blob = self._bucket.blob("{}".format(file))
        assert document_blob.exists()

        credentials = document_blob.download_as_string().decode("utf-8")
        # credentials = document_blob.download_as_bytes()

        credentials = json.loads(credentials)
        return credentials
