from __future__ import absolute_import
from pprint import pprint
import json
import argparse
from pprint import pprint
import unicodedata
from google.cloud import pubsub_v1
import sys
import os
import time


def _remove_accents(data):
    return unicodedata.normalize('NFKD', data).encode('ascii', 'ignore')

def _asciify_dict(data):
    """ Ascii-fies dict keys and values """
    ret = {}
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = _remove_accents(key)
            key = key.encode('utf-8')
        if isinstance(value, unicode):
            value = _remove_accents(value)
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = _asciify_list(value)
        elif isinstance(value, dict):
            value = _asciify_dict(value)
        ret[key] = value
    return ret

def get_service(api_name, api_version):
    service = build(api_name, api_version)
    return service

def callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(
            pubsub_topic, message_future.exception()))
    else:
        print(message_future.result())

def process_message(x):
    data = json.dumps(_asciify_dict(x))
    submit = publisher.publish(topic_path, data=data)
    submit.add_done_callback(callback)
    print("Submitting payload: {}".format(data))

# yapf: disable
parser = argparse.ArgumentParser()
parser.add_argument('--input_topic', dest='pubsub_topic', required=True, help='Input PubSub topic (topic name only)')
parser.add_argument('--project', dest='project', required=True, help='GCP Project ID')
known_args = parser.parse_known_args(sys.argv)
# yapf: enable

project = known_args[0].project
pubsub_topic = known_args[0].pubsub_topic

scope = "https://www.googleapis.com/auth/cloud-platform"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project, pubsub_topic)


import requests

headers = {
    'X-API-Key': os.environ.get('MOCKAROO_API_KEY')
}


for i in range(0,10):
    response = requests.get('https://my.api.mockaroo.com/transaction.json', headers=headers)
    data = json.loads(response.content)
    for x in data:
        process_message(x)
    time.sleep(65)
