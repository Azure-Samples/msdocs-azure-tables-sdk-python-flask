# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------

import json
import os

from azure.data.tables import TableServiceClient
from dotenv import load_dotenv
from flask import request

load_dotenv(verbose=True)


class TableServiceHelper:

    def __init__(self, table_name=None, conn_str=None):
        self.table_name = table_name if table_name else os.getenv("table_name")
        self.conn_str = conn_str if conn_str else os.getenv("conn_str")
        self.table_service = TableServiceClient.from_connection_string(self.conn_str)
        self.table_client = self.table_service.get_table_client(self.table_name)

    def query_entity(self, params):
        return [] #TODO

    def delete_entity(self):
        pass #TODO

    def insert_entity(self):
        pass #TODO

    def upsert_entity(self):
        pass #TODO

    def update_entity(self):
        pass #TODO

    def submit_transaction(self, operations):
        pass #TODO

    def insert_sample_data(self):
        city = request.form.get('City')
        unit = request.form.get("Units")
        sample_data_path = os.path.join(os.getenv("project_root_path"), "webapp/data/sample_data.json")
        weather_data = json.load(open(sample_data_path))
        batch_insert_entity_operations = []
        for w in weather_data:
            if w["StationName"] == city:
                entity = {
                    "PartitionKey": w["StationName"],
                    "RowKey": "{} {}".format(w["ObservationDate"], w["ObservationTime"]),
                    "Temperature": round((w["Temperature"] - 32) * 5 / 9, 2) if unit == "US" else w["Temperature"],
                    "Humidity": w["Humidity"],
                    "Barometer": round(w["Barometer"] * 33.864, 2) if unit == "US" else w["Barometer"],
                    "WindDirection": w["WindDirection"],
                    "WindSpeed": round(w["WindSpeed"] * 1.609, 2) if unit == "US" else w["WindSpeed"],
                    "Precipitation": round(w["Precipitation"] * 25.4, 2) if unit == "US" else w["Precipitation"]
                }
                batch_insert_entity_operations.append(("upsert", entity))
        self.submit_transaction(batch_insert_entity_operations)

    @staticmethod
    def deserialize():
        params = {key: request.form.get(key) for key in request.form.keys()}
        params["PartitionKey"] = params.pop("StationName")
        params["RowKey"] = "{} {}".format(params.pop("ObservationDate"), params.pop("ObservationTime"))
        return params

    @staticmethod
    def update_deserialize():
        params = {key: request.form.get(key) for key in request.form.keys()}
        params["PartitionKey"] = params.pop("StationName")
        params["RowKey"] = params.pop("ObservationDate")
        return params

    @staticmethod
    def serializer(data):
        field_list = []
        for item in data:
            for key in item.keys():
                if key not in field_list and key not in ("PartitionKey", "RowKey"):
                    field_list.append(key)
        entity_list = []
        for item in data:
            entity = {
                "StationName": item["PartitionKey"],
                "ObservationDate": item["RowKey"],
                "Properties": [{
                    "PropertyName": field_name,
                    "Value": item.get(field_name)
                } for field_name in field_list]
            }
            entity_list.append(entity)
        return {"field_list": field_list, "entity_list": entity_list}
