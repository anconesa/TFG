import polars as pl
import pandas as pd
import numpy as np
import shutil
from pathlib import Path
from fastavro import reader
import json
import datetime as dt

BACKUP_INTERMEDIATE_CONTAINER_NAME = "capture_processed"
MAX_EVENTS = 1000 

class Events:
    def __init__(self, capture, capture_processed=None, after=""):
        # En lugar de contenedores, hay que definir carpetas
        self.__retrieve_events(capture, capture_processed, after)

        # como resultado de lo anterior, se ha completado el atributo __events de self.
        self.dataframe = pl.DataFrame(self.__events)

        if self.dataframe.shape[0] > 0:
            # In some old events avro, problem: we drop them
            # Filtramos las filas del df, luego usamos drop para eliminarlas
            self.dataframe = self.dataframe.filter(
                pl.col("percentage") != ""
            )  # hay que quitarlo
            self.dataframe = self.dataframe.select(
                [col for col in self.dataframe.columns if col not in ["_id", "state"]]
            )
            self.dataframe = self.dataframe.with_columns(
                pl.col("percentage").cast(float), pl.col("timestamp").cast(float)
            )

            if "time_spent" in self.dataframe.columns:
                self.dataframe = self.dataframe.with_columns(
                    pl.col("time_spent").cast(float)
                )

            self.dataframe = self.dataframe.with_columns(
                (pl.col("timestamp") * 1000) 
                .cast(pl.Datetime)
                .dt.with_time_unit("ms")
                .dt.strftime("%d")
                .alias("day")
            )
            self.__add_author_unit()
            self.dataframe = self.dataframe.sort(by=pl.col("timestamp"))

    def __retrieve_events(self, capture, capture_processed, after=""):
        file_list = [
            file for file in capture.glob("**/*.avro") if file.as_posix() > after
        ]
        
        file_list.sort()
        # print(file_list)

        if len(file_list) > 0:
            self.batch_first_events_file = file_list[0]
        
        else:
            self.batch_first_events_file, self.batch_last_events_file = (None, None)

        self.__events = []
        events_number = 0

        for index, file_path in enumerate(file_list):
            file_name = file_path.name
            
            if file_path.stat().st_size > 0:
                with open(file_path, "rb") as f:
                    events_list = self.__process_file(f)
                    events_number += len(events_list)

                if events_number > MAX_EVENTS and index > 1:
                    break

                self.batch_last_events_file = file_name
                self.__events += events_list

                if capture_processed is not None:
                    bin_file_path = capture_processed / file_name
                    file_path.rename(bin_file_path) 
                    # Eliminamos el archivo y la carpeta padre              
                    bin_file_path.unlink()
                    parent_directory = file_path.parent
                    # Verificar si la carpeta está vacía antes de eliminarla
                    if not any(parent_directory.iterdir()):
                        shutil.rmtree(parent_directory)
                
        print(f"Number of downloaded events: {len(self.__events)}")


    def __process_file(self, file_name):
            events_list = []
            avro_reader = reader(file_name)

            for reading in avro_reader:
                parsed_json = json.loads(reading["Body"])
                events_list.append(parsed_json)

            return events_list

    def upload_metadata(self):
        #self.__upload_metadata( # "events_metadata.avro")
        self.__upload_metadata("events_metadata.json")

    def __upload_metadata(self, capture, path, schema):
        metadata = {
            "batch_first_events_file": self.batch_first_events_file,
            "batch_last_events_file": self.batch_last_events_file,
        }

        with open(path, "w") as f:
            json.dump(metadata, f)

    def add_unit_type(self):
        if self.dataframe.shape[0] == 0:
            return None

        evaluation_units_urls = ["ed12ad9791554f32b3327671030c0e5e"] 

        if not pl.col("unit_type") in self.dataframe.columns:
            self.dataframe = self.dataframe.with_columns(
                pl.col("unit_type").fill_nan("Content")
    )
        else:
            self.dataframe = self.dataframe.with_columns(
                pl.when(pl.col("unit_type").is_null())
                .then("Content")
                .otherwise(pl.col("unit_type"))
                .alias("unit_type")
    )
        self.dataframe = self.dataframe.with_columns(
            pl.when(
            self.dataframe["url"] == pl.lit(evaluation_units_urls[0]))
            .then(pl.lit("Evaluation"))
            .otherwise(pl.col("unit_type"))
            .alias('unit_type')
    )
    def __add_author_unit(self):
        if self.dataframe.shape[0] > 0:
            urls = self.dataframe["url"]
            url_author = urls.str.contains("/") & (urls != "/la/")

            if url_author.any():
                # authors = pl.Series(["anonymous"] * len(urls), dtype=pl.Object) 

                self.dataframe = self.dataframe.with_columns(
                    pl.when(url_author)
                    .then(
                        urls.str.split("/").apply(lambda s: s[1] if len(s) > 1 else "")
                    )
                    .otherwise("anonymous")
                    .alias("author"),
                    pl.when(url_author)
                    .then(
                        urls.str.split("/").apply(lambda s: s[2] if len(s) > 2 else "")
                    )
                    .otherwise(urls)
                    .alias("unit")
            )
                
        else:
            self.dataframe = self.dataframe.with_columns("author", "anonymous")
            self.dataframe = self.dataframe.with_columns("unit", self.dataframe["url"])


# ejemplo de carga de eventos, instanciando un objeto de la clase Events que hemos definido en este fichero.
if __name__ == "__main__":
    eventsla =  Events(
         Path("capture"),
         Path("capture_processed"), 
         after="/upctevents/upctforma/0/2023/06/17/14/56/43.avro"
)
print(eventsla.dataframe)  
