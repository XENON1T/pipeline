import boto3
import botocore
from boto3.dynamodb.conditions import Key, Attr
from pymongo import MongoClient
import tempfile
import os
import shutil
from pax import core

s3_bucket="xenon1t-eu"

def LoopQueue():

    # Connect to queue
    sqs = boto3.resource('sqs')    
    queue = sqs.get_queue_by_name(QueueName='files_to_be_processed')

    for message in queue.receive_messages():
        key = message.body
        print("Found key: " + key)

        # Now check that the key is in our S3 bucket
        s3_resource = boto3.resource('s3')
        s3_client = boto3.client('s3')
        try:
            s3_resource.Object(s3_bucket, key).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("Didn't find key in our s3 bucket.")
                print(key)
                return
        
        # Get the run number and event range
        splitstring = key.split("-") 
        number = int(splitstring[1])
        events = int(splitstring[4].split(".")[0]) 
        first_event = int(splitstring[2])
        last_event = first_event + events
        
        # Get the name from pymongo
        runclient = MongoClient(
            "mongodb://user:pw@copslx50.fysik.su.se:27017/run"
        )
        runs_collection = runclient['run']['runs_new']

        try:
            name = runs_collection.find_one({"number": number})['name']
        except Exception as e:
            print("Couldn't find run in DB, exiting: "+str(e))
            return
        
        # Clear all the events from Dynamo if they're there
        print("Clearing old entries with name " + name + " from " + 
              str(first_event) + " to " + str(last_event) )
        ClearDynamoRange(name, first_event, last_event)

        # Copy the run to a local tempfile
        directory_name = tempfile.mkdtemp()
        dlpath = os.path.join(directory_name, key)
        s3_client.download_file(s3_bucket, key, dlpath)

        # Process the run using pax
        pax_config = {
            "DEFAULT":
            {
                "run_number": number
            },
            "pax":
            {
            'logging_level': 'ERROR',
                'input_name': dlpath,
                'output': ['AmazonAWS.WriteDynamoDB'],
                'encoder_plugin': None,
                'n_cpus': 8
            },
            "AmazonAWS.WriteDynamoDB":
            {
                "fields_to_ignore": ['sum_waveforms',
                                     'sum_waveform',
                                    'reconstructed_positions',      
                                     'area_per_channel',
                                     'n_saturated_per_channel',             
                                     'hits_per_channel',
                                     'all_hits',
                                     'sum_waveform_top',
                                     'trigger_signals',
                                     'pulses',
                                     'hits',
                                     'raw_data',
                                 ],
            },
            "MongoDB":
            {
                "user": "pax",
                "password": "luxstinks",
                "host": "copslx50.fysik.su.se",
                "port": 27017,
                "database": "run"
            },
        }
        print("Starting pax")
        thispax = core.Processor(config_names="XENON1T",
                                 config_dict=pax_config)
        try:
            thispax.run()
            shutil.rmtree(directory_name)
            print("Finished run " + name)
        except Exception as exception:
            # Data processing failed.
            print("Pax processing for run " + name + 
                  " encountered exception " + str(exception) )
            ClearDynamoRange(name, first_event, last_event)
            shutil.rmtree(directory_name)
            return
    print("Finished")

def ClearDynamoRange(file_name, event_start, event_finish):

    db = boto3.resource('dynamodb',region_name='eu-central-1',
                    aws_secret_access_key="key",
                    aws_access_key_id="id")
    table = db.Table('reduced')

    with table.batch_writer() as batch:
        for event in range(event_start, event_finish):            
            batch.delete_item(
                Key={
                    "dataset_name": file_name,
                    "event_number": event
                }
            )

    '''
        try:
            response = table.delete_item(
                Key={
                    'dataset_name': file_name,
                    'event_number': event
                },
                #ConditionExpression="attribute_exists",
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                print(e.response['Error']['Message'])
            else:
                print(str(e))#raise

    '''
    return

import time
while(1):        
    LoopQueue()
    print("Sleeping...")
    time.sleep(5)
