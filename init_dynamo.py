from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb', region_name='eu-central-1', endpoint_url="https://dynamodb.eu-central-1.amazonaws.com")

msg_table = dynamodb.create_table(
  TableName='FSMessages',
  KeySchema=[
    {
      'AttributeName': 'msgid',
      'KeyType': 'HASH'  #Partition key
    },
    {
      'AttributeName': 'part_number',
      'KeyType': 'RANGE'
    }
  ],
  AttributeDefinitions=[
    {
      'AttributeName': 'msgid',
      'AttributeType': 'S'
    },
    # {
    #   'AttributeName': 'total_parts',
    #   'AttributeType': 'N'
    # },
    {
      'AttributeName': 'part_number',
      'AttributeType': 'N'
    # },
    # {
    #   'AttributeName': 'data',
    #   'AttributeType': 'S'
    }
  ],
  ProvisionedThroughput={
    'ReadCapacityUnits': 1,
    'WriteCapacityUnits': 1
  }
)

proc_table = dynamodb.create_table(
  TableName='FSMessages_complete',
  KeySchema=[
    {
      'AttributeName': 'msgid',
      'KeyType': 'HASH'  #Partition key
    }
  ],
  AttributeDefinitions=[
    {
      'AttributeName': 'msgid',
      'AttributeType': 'S'
    }
  ],
  ProvisionedThroughput={
    'ReadCapacityUnits': 1,
    'WriteCapacityUnits': 1
  }
)

print("MSG table status:", msg_table.table_status)
print("PROC table status:", proc_table.table_status)