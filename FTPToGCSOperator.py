from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from collections.abc import Sequence

class FTPToGCSOperator(BaseOperator):
    """
    Transfer files to Google Cloud Storage from FTP server.

    :param source_path: The ftp remote path. This is the specified file path
        for downloading the single file or multiple files from the FTP server.
        You can use only one wildcard within your path. The wildcard can appear only 
        at the end of the path.
    :param destination_bucket: The bucket to upload to.
    :param destination_path: The destination name of the object in the
        destination Google Cloud Storage bucket.
        If destination_path is not provided file/files will be placed in the
        main bucket path.
        If a wildcard is supplied in the destination_path argument, this is the
        prefix that will be prepended to the final destination objects' paths.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param ftp_conn_id: The ftp connection id. The name or identifier for
        establishing a connection to the FTP server.
    :param move_object: When move object is True, the object is moved instead
        of copied to the new location. This is the equivalent of a mv command
        as opposed to a cp command.

    """
    def __init__(self, 
                 task_id: str, 
                 ftp_conn_id: str, 
                 source_path: str,
                 destination_bucket: str,
                 destination_path: str | None = None,
                 gcp_conn_id: str = "google_cloud_default",
                 move_object: bool=False,
                 impersonation_chain: str | Sequence[str] | None = None,
                 **kwargs):

        super().__init__(
            task_id=task_id,
            **kwargs)

        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.ftp_conn_id = ftp_conn_id
        self.move_object = move_object
        self.destination_path = self._set_destination_path(destination_path)
        self.destination_bucket = self._set_bucket_name(destination_bucket)

        temp = source_path.split('/')
        self.ftp_folder = '/'.join(temp[0:-1])
        self.ftp_file = temp[-1]

    @staticmethod
    def _set_destination_path(path: str | None) -> str:
        if path is not None:
            path = path.rstrip("/")
            return path.lstrip("/") if path.startswith("/") else path
        return ""

    @staticmethod
    def _set_bucket_name(name: str) -> str:
        bucket = name if not name.startswith("gs://") else name[5:]
        return bucket.strip("/")

    def execute(self, context):
        import ftplib
        from google.cloud import storage
        from airflow.models import Connection
        c = Connection.get_connection_from_secrets(self.ftp_conn_id)

        storage_client = storage.Client()
        dest_bucket = storage_client.get_bucket(self.destination_bucket)

        try:
            ftp = ftplib.FTP(c.host)
        except Exception as e:
            raise AirflowException(f"Error connecting to {c.host}: {e} ")
        else:
            self.log.info('*** Connected to HOST ' + c.host)

        try:
            ftp.login(c.login, c.get_password())
        except Exception as e:
            ftp.quit()
            raise AirflowException(f"Cannot login: {e} ")

        try:
            ftp.cwd(self.ftp_folder)
        except Exception as e:
            ftp.quit()
            raise AirflowException(f"Error in folder {self.ftp_folder}: {e} ")
        else:
            self.log.info('*** Changed to folder: ' + self.ftp_folder)

        try:
            for filename in ftp.nlst(self.ftp_file):

                file_dest = filename
                if len(self.destination_path)>0:
                    file_dest = self.destination_path + '/' + file_dest
                    
                self.log.info(f"File will be saved to {self.destination_bucket}/{file_dest}")
                dest_blob = dest_bucket.blob(file_dest)

                self.log.info(f"Downloading {filename}")
                with dest_blob.open("wb") as write_stream:
                    ftp.retrbinary('RETR ' + filename, write_stream.write)

                if self.move_object:
                    self.log.info(f"Deleting {filename} on FTP server")
                    ftp.delete(filename)
        except Exception as e:
            ftp.quit()
            raise AirflowException(f"Error reading file {filename}: {e} ")

        ftp.quit()