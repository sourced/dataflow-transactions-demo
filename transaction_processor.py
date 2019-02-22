from __future__ import absolute_import
from apache_beam.io.gcp.internal.clients import bigquery
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from pprint import pprint
import json
import os
import sys
import argparse
import logging
import apache_beam as beam

scope = "https://www.googleapis.com/auth/cloud-platform"

def get_service(api_name, api_version, scopes):
    service = build(api_name, api_version)
    return service

def get_table_schema():
    table_schema = [
        "account_number:NUMERIC",
        "first_name:STRING",
        "last_name:STRING",
        "city:STRING",
        "country:STRING",
        "post_code:STRING",
        "transaction_lat:NUMERIC",
        "transaction_long:NUMERIC",
        "transaction_vendor_name:STRING",
        "transaction_amount:NUMERIC",
        "transaction_time:TIMESTAMP",
    ]
    return ",".join(table_schema)


def deidentify_row(line, project):
    dlp_client = get_service(api_name="dlp", api_version="v2", scopes=[scope])
    record = json.loads(line)
    request_body = {
        "deidentifyConfig": {
            "recordTransformations": {
                "fieldTransformations": [
                    {
                        "primitiveTransformation": {
                            "cryptoReplaceFfxFpeConfig": {
                                "cryptoKey": {"unwrapped": {"key": "YWJjZGVmZ2hpamtsbW5vcA=="}},
                                "commonAlphabet": "NUMERIC",
                            }
                        },
                        "fields": [{"name": "account_number"}],
                    },
                    {
                        "primitiveTransformation": {
                            "cryptoReplaceFfxFpeConfig": {
                                "cryptoKey": {"unwrapped": {"key": "YWJjZGVmZ2hpamtsbW5vcA=="}},
                                "commonAlphabet": "ALPHA_NUMERIC",
                            }
                        },
                        "fields": [{"name": "first_name"}, {"name": "last_name"}],
                    },
                ]
            }
        },
        "inspectConfig": {"infoTypes": [{"name": "PERSON_NAME"}, {"name": "CREDIT_CARD_NUMBER"}]},
        "item": {
            "table": {
                "headers": map(lambda k, v: {"name": k}, record.keys(), record.values()),
                "rows": [
                    {
                        "values": map(
                            lambda k, v: {"string_value": str(v)}, record.keys(), record.values()
                        )
                    }
                ],
            }
        },
    }

    resp = (
        dlp_client.projects()
        .content()
        .deidentify(parent="projects/{}".format(project), body=request_body)
        .execute()

    )
    dlp_keys = resp["item"]["table"]["headers"]
    dlp_values = resp["item"]["table"]["rows"][0]["values"]
    dlp_tuple_values = tuple([d["stringValue"] for d in dlp_values])
    dlp_tuple_keys = tuple([d["name"] for d in dlp_keys])
    x=0
    dlp_obj = {}
    for i in dlp_tuple_keys:
        dlp_obj[i] = dlp_tuple_values[x]
        x=x+1
    return dlp_obj

def print_row(row):
    print(row)


def run(argv=None):
    """Build and run the pipeline."""

    # yapf: disable
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', dest='input_topic', required=True, help='Input PubSub topic')
    parser.add_argument('--output_table', dest='output_table', required=True, help='Output BQ table')
    known_args, pipeline_args = parser.parse_known_args(argv)
    # yapf: enable

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(SetupOptions).setup_file = "./setup.py"
    pipeline_options.view_as(StandardOptions).streaming = True

    index = pipeline_args.index('--project')
    project = pipeline_args[index + 1]

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(known_args.input_topic)
        lines = lines | "DeIdentifyRow" >> beam.Map(deidentify_row, project)
        lines | "PrintRow" >> beam.Map(print_row)
        lines | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            known_args.output_table,
            schema=get_table_schema(),
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
