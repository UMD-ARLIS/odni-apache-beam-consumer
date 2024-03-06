import json
import pprint
#import logging
from datetime import datetime

import apache_beam as beam
from beam_nuggets.io.kafkaio import KafkaConsume
from apache_beam import Pipeline, Map, GroupByKey, ParDo, WindowInto
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import Always
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions

class MSKTokenProvider():
    def token(self):
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        token, _ = MSKAuthTokenProvider.generate_auth_token(region)
        return token

def get_secret():
    import boto3
    from botocore.exceptions import ClientError

    try:
        get_secret_value_response = boto3.session.Session().client(service_name='secretsmanager', region_name=region).get_secret_value(SecretId="odni-msk-rest-proxy")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid params:", e)
        elif e.response['Error']['Code'] == 'DecryptionFailure':
            print("The requested secret can't be decrypted using the provided KMS key:", e)
        elif e.response['Error']['Code'] == 'InternalServiceError':
            print("An error occurred on service side:", e)
    else:
        # Secrets Manager decrypts the secret value using the associated KMS CMK
        # Depending on whether the secret was a string or binary, only one of these fields will be populated
        if 'SecretString' in get_secret_value_response:
            return get_secret_value_response['SecretString']
        else:
            return get_secret_value_response['SecretBinary']

def print_element(elem):
    pp.pprint(f"{elem}\n")
    return elem

def filter_out_nomes(log):
  if log[0] is not None:
    json_log = json.loads(log[1])
    page_url = json_log["pageUrl"] if "pageUrl" in json_log else None
    log_type = json_log["type"] if "type" in json_log else None

    if (not (page_url is None or log_type is None) and
        isinstance(page_url, str) and str(page_url).lower().find("openstreetmap") > -1 and
        isinstance(log_type, str) and str(log_type).lower() == "visit"):
        yield log
  else:
    print('we found a none! get it out')

def test(elem):
    return elem

region = "us-east-1"
date_tag = datetime.now().strftime("%Y-%m-%d-%H%M%S")

pp  = pprint.PrettyPrinter(indent=4, width=100)

pl_options = PipelineOptions([], **{
    "region": region,
    "parallelism": 2,
    #"runner": "FlinkRunner",
    "job_name": f"tap_filter_{date_tag}",
})

pl_options.view_as(StandardOptions).streaming = True
pl_options.view_as(SetupOptions).save_main_session = False #Must be set to "False" to prevent pickling issues

with Pipeline(options=pl_options) as pipeline:
    user_tags = (
        pipeline
        | "read from topic" >> KafkaConsume(
            consumer_config={
                "topic": "raw-logs",
                "enable_auto_commit": "True",
                'auto_offset_reset': 'earliest',
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "OAUTHBEARER",
                "sasl_oauth_token_provider": MSKTokenProvider(),
                "bootstrap_servers": json.loads(get_secret())['MSK_BROKERS'],
                #NOTE: Setting "group_id" to a unique value each run will ensure that the offset
                #will return to the earliest value and all records will be retrieved. If "group_id"
                #stays constant then only the latest records will be retrieved
                "group_id": f"raw-logs-reader_{date_tag}",
                #"enable.auto.commit": "False",
                #"consumer.timeout.ms": '15000'
            }
        )
        #| "agg every 60 sec" >> WindowInto(
        #    windowfn=FixedWindows(60),
        #    trigger=AfterWatermark(
        #        late=AfterCount(1),
        #        early=AfterProcessingTime(delay=1 * 60)
        #    ),
        #    accumulation_mode=AccumulationMode.ACCUMULATING
        #)
        | "filter out NOME logs" >> ParDo(filter_out_nomes)
        #| "group by user id" >> GroupByKey()
        | "test" >> ParDo(test)
    )

#(
#    user_tags
#    | "print" >> Map(lambda element: print_element(element))
#)