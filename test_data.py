from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr

msg2process = 111

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

dynamodb = boto3.resource('dynamodb', region_name='eu-central-1', endpoint_url="http://localhost:8000")

try:
  dynamodb.Table('FSMessages').delete()
  dynamodb.Table('FSMessages_complete').delete()
except:
  pass

msg_table = dynamodb.create_table(
  TableName='FSMessages',
  KeySchema=[
    {
      'AttributeName': 'msgid-partnum',
      'KeyType': 'HASH'  #Partition key
    }
  ],
  AttributeDefinitions=[
    {
      'AttributeName': 'msgid-partnum',
      'AttributeType': 'S'
    # },
    # {
    #   'AttributeName': 'msgid',
    #   'AttributeType': 'S'
    # },
    # {
    #   'AttributeName': 'total_parts',
    #   'AttributeType': 'N'
    # },
    # {
    #   'AttributeName': 'part_number',
    #   'AttributeType': 'N'
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

messages_table = dynamodb.Table('FSMessages')
processed_table = dynamodb.Table('FSMessages_complete')

msgs = [
  {"msgid-partnum": "123-1", "msgid":"123", "total_parts": "2", "part_number": "1", "data":"Hello, "},
  {"msgid-partnum": "123-2", "msgid":"123", "total_parts": "2", "part_number": "2", "data":"World"}
  ]

  #persist message to dynamo
for msg in msgs:
  messages_table.put_item(
      Item={
          'msgid-partnum': msg['msgid'],
          'msgid': msg['msgid'],
          'total_parts': msg['total_parts'],
          'part_number': msg['part_number'],
          'data': msg['data']
          }
      )