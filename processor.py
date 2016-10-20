import boto3
import botocore
from boto3.dynamodb.conditions import Key, Attr
from pymongo import MongoClient
import tempfile
import os
import shutil
from json import dumps
import requests
from pax import core

s3_bucket="xenon1t-eu-raw"

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
            "mongodb://"+os.getenv("MONGO_USER") + ":" + os.getenv("MONGO_PASSWORD")
            +"@copslx50.fysik.su.se:27017/" + os.getenv("MONGO_AUTH_DB")
        )
        runs_collection = runclient['run']['runs_new']

        try:
            doc = runs_collection.find_one({"number": number})
            name = doc['name']
            nev = 0
            uuid = doc['_id']
            if 'trigger' in doc and 'events_built' in doc['trigger']:
                nev  = doc['trigger']['events_built']
        except Exception as e:
            print("Couldn't find run in DB, exiting: "+str(e))
            return
        
        # Clear all the events from Dynamo if they're there
        print("Clearing old entries with name " + name + " from " + 
              str(first_event) + " to " + str(last_event) )
        ClearDynamoRange(name, first_event, last_event)

        # Copy the run to a local tempfile
        directory_name = tempfile.mkdtemp()
        filename = key.split("/")[1]
        dlpath = os.path.join(directory_name, filename)
        s3_client.download_file(s3_bucket, key, dlpath)

        # Process the run using pax
        pax_config = {
            "DEFAULT":
            {
                "run_number": number,
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
                "user": os.getenv("MONGO_USER"),
                "password": os.getenv("MONGO_PASSWORD"),
                "host": "copslx50.fysik.su.se",
                "port": 27017,
                "database": os.getenv("MONGO_AUTH_DB")
            },
        }
        print("Starting pax")
        thispax = core.Processor(config_names="XENON1T",
                                 config_dict=pax_config)
        status = "error"
        try:
            thispax.run()
            status = "success"
            print("Finished run " + name)            
        except Exception as exception:
            # Data processing failed.
            print("Pax processing for run " + name + 
                  " encountered exception " + str(exception) )
            ClearDynamoRange(name, first_event, last_event)
            
        shutil.rmtree(directory_name)
        UpdateRunsDB(uuid, nev, status)
        
        # If everything is good kill the message so we don't
        # process again. Otherwise if we somehow die before getting 
        # here this run gets processed again later after the timeout
        if status == 'success':
            message.delete()
        
    print("Finished")



def UpdateRunsDB(uuid, nev, status):
    """ 
    Makes an API call to tell the runs DB what we did
    """

    # Query dynamo to check if we have all the events
    db = boto3.resource('dynamodb',region_name='eu-central-1',
                        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"))
    table = db.Table('reduced')
    ret = table.query(KeyConditionExpression=Key("dataset_name").eq("161019_0353"), 
                      Select="COUNT")
    if status != "error":
        status = "transferred"
        if ret['Count'] != nev:
            status="transferring"
    

    # Now call the API to update with the proper status
    headers = {
        "content-type": "application/json",
        "Authorization": "ApiKey "+os.getenv("XENON_API_USER")+":"+
        os.getenv("XENON_API_KEY")
    }
    update = {
        "host": "aws",
        "location": "dynamodb:reduced",
        "checksum": "NA",
        "status": status,
        "type": "processed"
    }
    url = os.getenv("XENON_API_URL") + str(uuid) + "/"
    pars=dumps(update)
    ret = requests.put(url, data=pars,
                       headers=headers)
    
    
    

def ClearDynamoRange(file_name, event_start, event_finish):

    db = boto3.resource('dynamodb',region_name='eu-central-1',
                        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"))
    table = db.Table('reduced')

    with table.batch_writer() as batch:
        for event in range(event_start, event_finish):            
            batch.delete_item(
                Key={
                    "dataset_name": file_name,
                    "event_number": event
                }
            )

    return

import time
for i in range(100):
    LoopQueue()
    print("Iteration " + str(i) + "finished...")
    time.sleep(5)
exit()

    
