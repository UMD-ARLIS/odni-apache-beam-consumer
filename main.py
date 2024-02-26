import json
#import logging
import functools

import boto3
from botocore.exceptions import ClientError

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

#job_server = "localhost"
expansion_service = "localhost:16000"

#class MSKTokenProvider():
#    def token(self):
#        auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("us-east-1")
#        # Note that this library expects oauth_cb to return expiry time in seconds since epoch, while the token generator returns expiry in ms
#        return auth_token #, expiry_ms/1000
def oauth_cb():
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("us-east-1")
    # Note that this library expects oauth_cb to return expiry time in seconds since epoch, while the token generator returns expiry in ms
    return auth_token #+ expiry_ms/1000

#def log_element(elem):
#    #logging.info(elem)
#    print("Element {elem} was just processed.")
#    return elem

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
    #"runner": "FlinkRunner",
    "job_name": "kafka_echo_demo",
    #"job_endpoint": f"{job_server}:8099",
    #"artifact_endpoint": f"{job_server}:8098",
    #"environment_type": "EXTERNAL",
    #"environment_config": "localhost:50000",
    "streaming": True,
    #"parallelism": 2,
    #"experiments": ["use_deprecated_read"],
    #"checkpointing_interval": "60000",
})

beam_options.view_as(SetupOptions).save_main_session = True

aws_secret = json.loads(get_secret())

#token_provider = MSKTokenProvider()

#token = oauth_cb()

try:
    with beam.Pipeline(options=beam_options) as pipeline: (
        pipeline
        | 'write' >> WriteToKafka(
            producer_config={

            }
        )
        | 'read' >> ReadFromKafka(
            consumer_config={
                "bootstrap.servers": "b-1.tapcluster.0z1x5f.c9.kafka.us-east-1.amazonaws.com:9096,b-3.tapcluster.0z1x5f.c9.kafka.us-east-1.amazonaws.com:9096,b-2.tapcluster.0z1x5f.c9.kafka.us-east-1.amazonaws.com:9096", #aws_secret["MSK_BROKERS"],
                "group.id": "tap_kafka_read",
                'auto.offset.reset': 'earliest',
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "SCRAM-SHA-512",
                "sasl.jaas.config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"odni-msk-rest-proxy\" password=\"Dfasd124g#@\";",
                #"oauth_cb": token,
                #"sasl.oauthbearer.method": "oidc",
                #"sasl.oauthbearer.token.endpoint.url": "https://oidc.eks.us-east-1.amazonaws.com/id/DEECDA738F89979DEDD0DD49528D0D62",
                #"sasl.oauth.token.provider": token[0],
                #"sasl.jaas.config": "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId=\"msk-rest-proxy\" clientSecret=\"odni-msk-rest-proxy\";",
                #"sasl.login.callback.handler.class": "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler",
                #"sasl.kerberos.service.name": "kafka",
                #"sasl_ssl.plain.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required",
                #"sasl.plain.username": "odni-msk-rest-proxy",
                #"sasl.plain.password": "Dfasd124g#@",
            },
            topics='test-topic',
            with_metadata=True,
            max_num_records=3,
            expansion_service=expansion_service,
        )
        | 'print' >> beam.Map(print)
    )
except Exception as e:
    print(e)