import json
import pprint
#import logging
from datetime import datetime
from datetime import timedelta

from beam_nuggets.io.kafkaio import KafkaConsume
from apache_beam import Pipeline, Map, GroupByKey, CombinePerKey, DoFn, ParDo, WindowInto, PTransform
from apache_beam.transforms.window import FixedWindows, TimestampedValue, Sessions, GlobalWindows, SlidingWindows
from apache_beam.transforms.trigger import AfterWatermark, AfterEach, Repeatedly, AfterAny, AfterProcessingTime, AfterCount, AccumulationMode
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions

from apache_beam.transforms.combiners import ToListCombineFn

class MSKTokenProvider():
    def token(self):
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        token, _ = MSKAuthTokenProvider.generate_auth_token(region)
        return token

class LogProcessor(DoFn):
    def process(self, log, timestamp=DoFn.TimestampParam, window=DoFn.WindowParam):
        if log[0] is not None:
            json_log = json.loads(log[1])
            page_url = json_log["pageUrl"] if "pageUrl" in json_log and isinstance(json_log["pageUrl"], str) else None
            log_type = json_log["type"] if "type" in json_log and isinstance(json_log["type"], str) else None

            if (not (page_url is None or log_type is None) and
                str(page_url).lower().find("openstreetmap") > -1 and str(log_type).lower() == "visit" and
                "details" in json_log and "name" in json_log["details"] and isinstance(json_log["details"]["name"], str)):
                yield {"key": log[0], "value": (int(json_log["clientTime"]), json_log["details"]["name"])}
                #return (log[0], (int(json_log["clientTime"]), json_log["details"]["name"]))

class Aggregate(PTransform):
  """Calculates scores for each team within the configured window duration.

  Extract team/score pairs from the event stream, using hour-long windows by
  default.
  """
  def __init__(self, team_window_duration, allowed_lateness):
    # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
    # super().__init__()
    PTransform.__init__(self)
    self.team_window_duration = team_window_duration * 60
    self.allowed_lateness_seconds = allowed_lateness * 60

  def expand(self, pcoll):
    # NOTE: the behavior does not exactly match the Java example
    # TODO: allowed_lateness not implemented yet in FixedWindows
    # TODO: AfterProcessingTime not implemented yet, replace AfterCount
    return (
        pcoll
        # We will get early (speculative) results as well as cumulative
        # processing of late data.
        | 'LeaderboardTeamFixedWindows' >> WindowInto(
            FixedWindows(self.team_window_duration),
            trigger=AfterWatermark(AfterCount(10), AfterCount(20)),
            accumulation_mode=AccumulationMode.ACCUMULATING)
    )

class AddTimestamp(DoFn):
    def process(self, elem):
        yield TimestampedValue(elem, round(elem["value"][0] / 1000))
        #return TimestampedValue(elem, time.time())

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
    pp.pprint(elem)
    return elem

count = 0
region = "us-east-1"
date_tag = datetime.now().strftime("%Y-%m-%d-%H%M%S")

# Sliding windows of 60 seconds every 60 seconds
window_size = timedelta(seconds=60).total_seconds()  # in seconds
window_period = timedelta(seconds=60).total_seconds()  # in seconds

pp  = pprint.PrettyPrinter(indent=4, width=100)

pl_options = PipelineOptions([], **{
    "region": region,
    "parallelism": 1,
    #"runner": "FlinkRunner",
    "runner": "DirectRunner",
    "job_name": f"tap_filter_{date_tag}",
})

pl_options.view_as(StandardOptions).streaming = True
pl_options.view_as(SetupOptions).save_main_session = False #Must be set to "False" to prevent pickling issues

with Pipeline(options=pl_options) as pipeline: (
    pipeline
    | "read from topic" >> KafkaConsume(
        consumer_config={
            "topic": "raw-logs",
            "enable_auto_commit": "False",
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
        },
    )
    | "filter out NOME logs" >> ParDo(LogProcessor())
    | 'Add timestamp' >> ParDo(AddTimestamp())
    
    #| "aggregate" >> Aggregate(10, 0)
    | 'windowing' >> WindowInto(
        windowfn=FixedWindows(10),
        trigger=Repeatedly(AfterWatermark(AfterProcessingTime(10))),
        accumulation_mode=AccumulationMode.ACCUMULATING
    )
   
    #| "agg every 60 sec" >> WindowInto(
    #    FixedWindows(10)
    #    #windowfn=GlobalWindows(),
    #    #trigger=Repeatedly(AfterProcessingTime(10)),
    #    #accumulation_mode=AccumulationMode.ACCUMULATING
    #)
    #| '10 sec session' >> WindowInto(Sessions(10))
    | 'group by' >> GroupByKey()
    | "test2" >> ParDo(print_element)
)

#(
#    user_tags
#    | "print" >> Map(lambda element: print_element(element))
#)