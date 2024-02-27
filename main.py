import json
import boto3
from botocore.exceptions import ClientError

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

expansion_service = "localhost:8088"


def get_secret():
    secret_name = "odni-msk-rest-proxy"
    client = boto3.session.Session().client(service_name='secretsmanager', region_name="us-east-1")

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + secret_name + " was not found")
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

beam_options = PipelineOptions([], **{
    "job_name": "kafka_echo_demo",
    "streaming": True,
})

beam_options.view_as(SetupOptions).save_main_session = True

aws_secret = json.loads(get_secret())

try:
    with beam.Pipeline(options=beam_options) as pipeline: (
        pipeline
        # | 'write' >> WriteToKafka(
        #     producer_config={

        #     }
        # )
        | 'read' >> ReadFromKafka(
            consumer_config={
                "bootstrap.servers": aws_secret["MSK_BROKERS"],
                "group.id": "tap_kafka_read",
                'auto.offset.reset': 'earliest',
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "OAUTHBEARER",
                "sasl.jaas.config": "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;",
                "sasl.login.callback.handler.class": "software.amazon.msk.auth.iam.IAMOAuthBearerLoginCallbackHandler",
            },
            topics=['test-topic'],
            with_metadata=True,
            max_num_records=3,
            expansion_service=expansion_service,
        )
        | 'print' >> beam.Map(print)
    )
except Exception as e:
    print(e)