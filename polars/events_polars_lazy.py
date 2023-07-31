import polars as pl
from fastavro import reader, writer
from azure.storage.blob import ContainerClient
import json
import os
import io
import time
from dotenv import load_dotenv
import datetime as dt
from container import Container


# CTE
MAX_EVENTS = 100000


class Events:
    def __init__(self, events_container, bin_container=None, after=""):
        # En lugar de contenedores, hay que definir carpetas
        download_start_time = time.time()
        self.__retrieve_events(events_container, bin_container, after)
        # como resultado de lo anterior, se ha completado el atributo __events de self.
        download_end_time = time.time()
        download_time = download_end_time - download_start_time
        processing_start_time = time.time() 
        self.dataframe = pl.DataFrame(self.__events).lazy()

        if len(self.dataframe.collect()) > 0:
            # In some old events avro, problem: we drop them
            # Filtramos las filas del df, luego usamos drop para eliminarlas
            self.dataframe = (
            self.dataframe
            .filter(pl.col("percentage") != "")
            .select([col for col in self.dataframe.columns if col not in ["_id", "state"]])
            .with_columns(
                pl.col("percentage").cast(float),
                pl.col("timestamp").cast(float),
            )
            .lazy()
        )

            if "time_spent" in self.dataframe.columns:
                self.dataframe = self.dataframe.with_columns(
                    pl.col("time_spent").cast(float)
                )

            self.dataframe = (
            self.dataframe.with_columns(
                (pl.col("timestamp") * 1000) 
                .cast(pl.Datetime)
                .dt.with_time_unit("ms")
                .dt.strftime("%d")
                .alias("day")
            )
            
            .sort(by=pl.col("timestamp"))
            .lazy()
        )
        self.__add_author_unit()
        processing_end_time = time.time()
        processing_time = processing_end_time - processing_start_time
        total_time = download_time + processing_time
        print(f"Tiempo de descarga de archivos: {download_time} segundos")
        print(f"Tiempo de procesado (excluyendo descarga de archivos): {processing_time} segundos")
        print(f"Tiempo total (incluyendo descarga de archivos): {total_time} segundos")


    def __retrieve_events(self, events_container, bin_container=None, after=""):
        blob_generator = (
            blob for blob in events_container.container.list_blobs() if blob.name > after
        )

        # file_list.sort()
        # print(file_list)

        # if len(file_list) > 0:
         #   self.batch_first_events_file = file_list[0]
        
        #else:
        self.batch_first_events_file, self.batch_last_events_file = (None, None)

        self.__events = []
        events_number = 0

        for index, blob in enumerate(blob_generator):
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

                self.batch_firts_events_file = self.batch_first_events_file or blob.name
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

        evaluation_units_urls = ["ed12ad9791554f32b3327671030c0e5e"] 

        if not pl.col("unit_type") in self.dataframe.columns:
            self.dataframe = self.dataframe.lazy().with_columns(
                pl.col("unit_type").fill_nan("Content")
    )
        else:
            self.dataframe = self.dataframe.lazy().with_columns(
                pl.when(pl.col("unit_type").is_null())
                .then("Content")
                .otherwise(pl.col("unit_type"))
                .alias("unit_type")
    )
        self.dataframe = self.dataframe.lazy().with_columns(
            pl.when(
            self.dataframe["url"] == pl.lit(evaluation_units_urls[0]))
            .then(pl.lit("Evaluation"))
            .otherwise(pl.col("unit_type"))
            .alias('unit_type')
    )
    def __add_author_unit(self):
        if len(self.dataframe.collect()) > 0:
            urls = self.dataframe.select(pl.col("url"))
            url_author = self.dataframe.filter(
                (pl.col("url").str.contains("/")) & (pl.col("url") != "/la/")
            ).collect()

            if not url_author.is_empty():
            # authors = pl.Series(["anonymous"] * len(urls), dtype=pl.Object) 
            # Agregamos la columna "author" en función de los valores de "url"
                self.dataframe = (
                self.dataframe.lazy()
                .with_columns(
                    pl.when(
                        (pl.col("url").str.contains("/"))
                        & (pl.col("url") != "/la/")
                    )
                    .then(
                        pl.col("url").str.split("/").apply(lambda s: s[1] if len(s) > 1 else "")
                    )
                    .otherwise("anonymous")
                    .alias("author"),
                )
                .lazy()
            )

            # Agregamos la columna "unit" en función de los valores de "url"
            self.dataframe = (
                self.dataframe.lazy()
                .with_columns(
                    pl.when(
                        (pl.col("url").str.contains("/"))
                        & (pl.col("url") != "/la/")
                    )
                    .then(
                        pl.col("url").str.split("/").apply(lambda s: s[2] if len(s) > 2 else "")
                    )
                    .otherwise(pl.col("url"))
                    .alias("unit"),
                )
                .lazy()
            )
        else:
             self.dataframe = (
                self.dataframe.lazy()
                .with_columns("author", "anonymous")
                .with_columns("unit", self.dataframe["url"])
                .lazy()
        )


load_dotenv()
anabel_storage_connection_str = os.environ["ANABEL_STORAGE_CONNECTION_STR"]
capture_container = Container("capture", anabel_storage_connection_str)
# print(capture_container.list_blobs())

# ejemplo de carga de eventos, instanciando un objeto de la clase Events que hemos definido en este fichero.
if __name__ == "__main__":
    eventsla =  Events(
        capture_container, 
         after="upctevents/upctforma/0/2023/06/01/00/00/00.avro"
)
print(eventsla.dataframe.collect())  

#def measure_events():
#    """Función empleada para usar timeit y evaluar el tiempo de ejecución"""
#    eventsla = Events(
#        "capture", 
#        "capture_processed", 
#        after="/upctevents/upctforma/0/2023/06/14/03/41/43.avro"
#)
#tiempo = timeit.timeit(measure_events, number=500)
#print(f"Tiempo de ejecución de eventsla: {tiempo} segundos")