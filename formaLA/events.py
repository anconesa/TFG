import pandas as pd
from fastavro import reader, writer
from azure.storage.blob import ContainerClient
import json
import os
import io
import time
from dotenv import load_dotenv
import datetime as dt
from container import Container
import timeit



MAX_EVENTS = 100000 


class Events:
    def __init__(self, events_container, bin_container=None, after=""):
        # en lugar de contenedores, hay que definir carpetas
        download_start_time = time.time()
        self.__retrieve_events(events_container, bin_container, after)

        # como resultado de lo anterior, se ha completado el atributo __events de self.
        download_end_time = time.time()
        download_time = download_end_time - download_start_time
        processing_start_time = time.time()
        self.dataframe = pd.DataFrame(self.__events, dtype=str)

        if self.dataframe.shape[0] > 0:
            # In some old events avro, problem: we drop them
            self.dataframe.drop(
                index=self.dataframe.index[self.dataframe["percentage"] == ""], 
                inplace=True
            )  # hay que quitarlo
            self.dataframe.drop(
                columns=["_id", "state"], inplace=True, 
                errors="ignore")
            self.dataframe = self.dataframe.astype(
                {
                    "percentage": float,
                    "timestamp": float,
                }
            )
            if "time_spent" in self.dataframe.columns:
                self.dataframe = self.dataframe.astype({"time_spent": float})
            self.dataframe["day"] = pd.to_datetime(
                self.dataframe["timestamp"], unit="s"
            ).dt.floor("D")
            self.__add_author_unit()
            self.dataframe.sort_values(["timestamp"], inplace=True)
            processing_end_time = time.time()
            processing_time = processing_end_time - processing_start_time
            total_time = download_time + processing_time
            print(f"Tiempo de descarga de archivos: {download_time} segundos")
            print(f"Tiempo de procesado (excluyendo descarga de archivos): {processing_time} segundos")
            print(f"Tiempo total (incluyendo descarga de archivos): {total_time} segundos")

    
    def __retrieve_events(self, events_container, bin_container, after=''):
        blob_list = [
            blob for blob in events_container.container.list_blobs() if blob.name > after
        ]
        
        blob_list.sort(key=lambda b: b.name)
        # print(file_list)

        if len(blob_list) > 0:
            self.batch_first_events_file = blob_list[0]
        
        else:
            self.batch_first_events_file, self.batch_last_events_file = (None, None)

        self.__events = []
        events_number = 0

        for index, blob in enumerate(blob_list):
            if blob.size > 508:
                blob_client = ContainerClient.get_blob_client(
                    events_container.container, blob=blob.name
                )
                fileReader = blob_client.download_blob().readall()
                print("Downloaded a non empty blob: " + blob.name)
                events_list = self.__process_blob(fileReader)
                events_number += len(events_list)
                if (events_number > MAX_EVENTS) & (index > 1):
                    break

                self.batch_last_events_file = blob.name
                self.__events += events_list

                if bin_container is not None:
                    ContainerClient.upload_blob(
                        bin_container.container,
                        name=blob.name,
                        data=fileReader,
                        overwrite=True,
                    )
        print(f"Number of downloaded events: {len(self.__events)}")
        events_container.container.close()
        if bin_container is not None:
            bin_container.container.close()



    def __process_blob(self, filename):
        with io.BytesIO(filename) as f:
            events_list = []
            avro_reader = reader(f)
            for reading in avro_reader:
                parsed_json = json.loads(reading["Body"])
                events_list.append(parsed_json)
        return events_list

    def upload_metadata(self):
        self.__upload_metadata(
            #BLOB_CONTAINER_NAME,
            "events_metadata.avro"
        )

    def __upload_metadata(self, events_container, path, schema):
        container = Container(events_container)
        fo = io.BytesIO()
        writer(
            fo,
                       [
                {
                    "batch_first_events_file": self.batch_first_events_file,
                    "batch_last_events_file": self.batch_last_events_file,
                }
            ],
        )
        ContainerClient.upload_blob(
            Container.container, name=path, data=fo.getvalue(), overwrite=True
        )
        Container.close()


    def add_unit_type(self):
        if self.dataframe.shape[0] == 0:
            return None

        evaluation_units_urls = ["ed12ad9791554f32b3327671030c0e5e"]  # complete

        if not "unit_type" in self.dataframe:
            self.dataframe["unit_type"] = "Content"
        else:
            self.dataframe["unit_type"] = self.dataframe["unit_type"].where(
                ~self.dataframe["unit_type"].isna(), "Content"
            )

        self.dataframe["unit_type"] = self.dataframe["unit_type"].where(
            ~self.dataframe["url"].isin(evaluation_units_urls), "Evaluation"
        )

    def __add_author_unit(self):
        if self.dataframe.shape[0] > 0:

            urls = self.dataframe["url"]

            url_author = urls.str.contains("/") & (urls != "/la/")

            if url_author.any():
                authors = pd.Series("anonymous", index=urls.index)

                self.dataframe["author"] = authors.where(
                    ~url_author, urls.str.split("/", expand=True).iloc[:, 1]
                )

                self.dataframe["unit"] = urls.where(
                    ~url_author, urls.str.split("/", expand=True).iloc[:, 2]
                )
            else:
                self.dataframe["author"] = "anonymous"
                self.dataframe["unit"] = self.dataframe["url"]

load_dotenv()
anabel_storage_connection_str = os.environ["ANABEL_STORAGE_CONNECTION_STR"]
capture_container = Container("capture", anabel_storage_connection_str)
#print(capture_container.list_blobs())

# ejemplo de carga de eventos, instanciando un objeto de la clase Events que hemos definido en este fichero.
if __name__ == "__main__":
    eventsla =  Events(
        capture_container, 
        after="upctevents/upctforma/0/2023/06/01/00/00/00.avro"
)
print(eventsla.dataframe)  

