from __future__ import print_function

import base64
import json
import os

from botocore.vendored import requests
from google.cloud import logging
from google.cloud.logging.resource import Resource


JAEGER_COLLECTOR_URL = os.getenv('JAEGER_COLLECTOR_URL')
STACKDRIVER_COLLECTOR_URL = os.getenv('STACKDRIVER_COLLECTOR_URL')
REGION = 'aws:' + os.getenv('AWS_REGION')

logging_client = logging.Client()
stackdriver = logging_client.logger('aws')


def lambda_handler(event, context):
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here.
        payload = base64.b64decode(record['kinesis']['data'])
        _process_record(payload, context)

    return 'Successfully processed {} records.'.format(len(event['Records']))


def _process_record(payload, context):
    try:
        payload = json.loads(payload)
    except:
        return

    log = payload.get('log')
    if not log:
        return

    try:
        log = json.loads(log)
    except:
        return

    if 'trace' in log:
        _process_trace(log['trace'], context)
    else:
        _process_log(log, context)


def _process_trace(trace, context):
    """Process traces by writing them to Jaeger and Stackdriver."""

    headers = {'Content-Type': 'application/x-thrift'}
    data = base64.b64decode(trace)

    resp = requests.post(JAEGER_COLLECTOR_URL, headers=headers, data=data)
    if not resp.ok:
        print('Failed to POST span to Jaeger: ' + resp.content)

    resp = requests.post(STACKDRIVER_COLLECTOR_URL, headers=headers, data=data)
    if not resp.ok:
        print('Failed to POST span to Stackdriver: ' + resp.content)


def _process_log(log, context):
    """Process logs by writing them out to CloudWatch and Stackdriver."""

    res = Resource(
        type='generic_task',
        labels={
            'location': REGION,
            'namespace': 'default',
            'job': 'router',
            'task_id': context.aws_request_id,
        },
    )
    stackdriver.log_struct(log, resource=res)
    print(log)

