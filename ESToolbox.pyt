import arcpy
import os
from elasticsearch import Elasticsearch
from elasticsearch import helpers


class Toolbox(object):
    def __init__(self):
        self.label = "ESToolbox"
        self.alias = "ESToolbox"
        self.tools = [ESTool]


class ESTool(object):
    def __init__(self):
        self.label = "ESTool"
        self.description = "ESTool"
        self.canRunInBackground = True

    def getParameterInfo(self):
        output_fc = arcpy.Parameter(
            name="output_fc",
            displayName="Points",
            direction="Output",
            datatype="Feature Layer",
            parameterType="Derived")

        es_hosts = arcpy.Parameter(
            name="es_hosts",
            displayName="ES Hosts",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        es_hosts.value = "es"

        index_mapping = arcpy.Parameter(
            name="index_mapping",
            displayName="Index/Mapping",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        index_mapping.value = "gps/gps"

        query_string = arcpy.Parameter(
            name="query_string",
            displayName="Query String",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        query_string.value = "attr:attr6"

        bbox = arcpy.Parameter(
            name="bbox",
            displayName="Bounding Box",
            direction="Input",
            datatype="GPExtent",
            parameterType="Required")

        return [output_fc, es_hosts, index_mapping, query_string, bbox]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        sp_ref = arcpy.SpatialReference('WGS 1984')

        es_hosts = parameters[1].value.strip()
        index_mapping = parameters[2].value.strip()
        query_string = parameters[3].value
        extent = parameters[4].value
        extent = extent.projectAs(sp_ref.exportToString())

        tl = extent.upperLeft
        br = extent.lowerRight

        query = {
            "query": {
                "filtered": {
                    "query": {
                        "query_string": {
                            "query": query_string
                        }
                    },
                    "filter": {
                        "geo_bounding_box": {
                            "loc": {
                                "top_left": {
                                    "lat": tl.Y,
                                    "lon": tl.X
                                },
                                "bottom_right": {
                                    "lat": br.Y,
                                    "lon": br.X
                                }
                            }
                        }
                    }
                }
            }
        }

        es = Elasticsearch(hosts=es_hosts.split(','))
        index, doc_type = index_mapping.split("/")

        in_memory = True
        if in_memory:
            ws = "in_memory"
            fc = ws + "/" + index
        else:
            fc = os.path.join(arcpy.env.scratchGDB, index)
            ws = os.path.dirname(fc)

        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)

        arcpy.management.CreateFeatureclass(ws, index, "POINT", spatial_reference=sp_ref)
        arcpy.management.AddField(fc, "ATTR", "STRING")
        arcpy.management.AddField(fc, "CREATED", "STRING")

        with arcpy.da.InsertCursor(fc, ["SHAPE@XY", "ATTR", "CREATED"]) as cursor:
            try:
                for doc in helpers.scan(es, index=index, query=query):
                    src = doc["_source"]
                    cursor.insertRow([src["loc"], src["attr"], src["created"]])
            except Exception as e:
                arcpy.AddWarning(str(e))
        parameters[0].value = fc
