import pandas as pd
from pathlib import Path
from fastavro import reader
import json
import timeit
import shutil


BACKUP_INTERMEDIATE_CONTAINER_NAME = "capture_processed"
MAX_EVENTS = 1000 


class Events:
    def __init__(self, capture, capture_processed=None, after=""):
        # en lugar de contenedores, hay que definir carpetas
        self.__retrieve_events(capture, capture_processed, after)

        # como resultado de lo anterior, se ha completado el atributo __events de self.
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
   # @profile
    def __retrieve_events(self, capture, capture_processed, after=''):
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

                #if capture_processed is not None:
                 #   bin_file_path = capture_processed / file_name
                  #  file_path.rename(bin_file_path) 
                    # Eliminamos el archivo y la carpeta padre 
                   # bin_file_path.unlink()
                    #parent_directory = file_path.parent
                    # Verificar si la carpeta está vacía antes de eliminarla
                    #if not any(parent_directory.iterdir()):
                     #   shutil.rmtree(parent_directory)
                
        print(f"Number of downloaded events: {len(self.__events)}")


    def __process_file(self, file_path):
        
            events_list = []

            avro_reader = reader(file_path)

            for reading in avro_reader:
                parsed_json = json.loads(reading["Body"])

                events_list.append(parsed_json)

            return events_list

    def upload_metadata(self):
        #self.__upload_metadata( # "events_metadata.avro",)
        self.__upload_metadata("events_metadata.json")

    def __upload_metadata(self, container_name, path, schema):
        metadata = {
            "batch_first_events_file": self.batch_first_events_file,
            "batch_last_events_file": self.batch_last_events_file,
        }

        with open(path, "w") as f:
            json.dump(metadata, f)

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

# ejemplo de carga de eventos, instanciando un objeto de la clase Events que hemos definido en este fichero.
if __name__ == "__main__":
    eventsla =  Events(
        Path("capture"),
        Path("capture_processed"), 
        after="/upctevents/upctforma/0/2023/06/14/03/41/43.avro"
)
print(eventsla.dataframe)  

#def measure_events():
#    """Función empleada para usar timeit y evaluar el tiempo de ejecución"""
#    eventsla = Events(
#        Path("capture"), 
#        Path("capture_processed"), 
#        after="/upctevents/upctforma/0/2023/06/14/03/41/43.avro"
#)
#tiempo = timeit.timeit(measure_events, number=500)
#print(f"Tiempo de ejecución de eventsla: {tiempo} segundos")