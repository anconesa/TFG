from fastavro import reader
from collections import defaultdict
from azure.storage.blob import ContainerClient
import io
import re


class Container:
    def __init__(self, container_name, storage_connection_str):
        self.container = ContainerClient.from_connection_string(
            storage_connection_str, container_name=container_name
        )
        self.container_name = container_name

    def retrieve_blob(self, path, backup_container=None):
        blob_client = ContainerClient.get_blob_client(self.container, blob=path)

        if blob_client.exists():
            fileReader = blob_client.download_blob().readall()
            fo = io.BytesIO(fileReader)
            object = next(reader(fo))

            if backup_container is not None:
                ContainerClient.upload_blob(
                    backup_container.container,
                    name=path,
                    data=fileReader,
                    overwrite=True,
                )
        else:
            object = defaultdict(lambda: None)

        return object

    def close(self):
        self.container.close()

    def list_blobs(self, pattern=".*", from_blob=""):
        blob_list = self.container.list_blobs()

        p = re.compile(pattern)

        blob_names = [
            b.name
            for b in blob_list
            if (p.search(b.name) is not None and b.name >= from_blob)
        ]
        return blob_names

    def delete_blobs(self, pattern=".*", from_blob=""):
        container = self.container

        p = re.compile(pattern)

        blob_list = container.list_blobs()
        blob_list = [
            b
            for b in blob_list
            if (p.search(b.name) is not None and b.name >= from_blob)
        ]

        for blob in blob_list:

            container.delete_blob(blob.name)

            print(f"blob {blob.name} deleted from {container.container_name}")

    def copy_blobs(self, target_container, pattern=".*", from_blob="", delete=False):

        container = self.container

        p = re.compile(pattern)

        blob_list = container.list_blobs()
        blob_list = [
            b
            for b in blob_list
            if (p.search(b.name) is not None and b.name >= from_blob)
        ]

        for blob in blob_list:
            _ = self.retrieve_blob(blob.name, backup_container=target_container)

        if delete:
            self.delete_blobs(pattern, from_blob=from_blob)
