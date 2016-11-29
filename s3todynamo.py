#!/usr/bin/env python

import boto3


s3 = boto3.resource('s3')
bucket = s3.Bucket('fullstackrockstars2')

dynamodb = boto3.resource('dynamodb', region_name='eu-central-1')
table = dynamodb.Table('FSMessages')

# Iterates through all the objects, doing the pagination for you. Each obj
# is an ObjectSummary, so it doesn't contain the body. You'll need to call
# get to get the whole body.
for obj in bucket.objects.all():
    key = obj.key
    msg = obj.get()['Body'].read()
    table.put_item(
        Item={
            'msgid': msg['Id'],
            'total_parts': msg['TotalParts'],
            'part_number': msg['PartNumber'],
            'data': msg['data']
            }
        )
    #bucket.delete_object(key)




