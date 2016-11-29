from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr

msg2process = "123"

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

messages_table = dynamodb.Table('FSMessages')
processed_table = dynamodb.Table('FSMessages_complete')

all_messages = messages_table.query(
    KeyConditionExpression=Key('msgid').eq(msg2process)
)
print(all_messages)
processed_msg = processed_table.query(
    KeyConditionExpression=Key('msgid').eq(msg2process)
)
print(processed_msg)

print("found ", processed_msg['Count'], " processed messages")
is_processed_already = processed_msg['Count'] == 1

if is_processed_already:
  print ("already processed")
  exit(0)

if all_messages['Count'] == 0:
  print ("no message parts to process")
  exit(1)

message_items = all_messages['Items']
total_parts = int(message_items[0]['total_parts'])

if total_parts != int(all_messages['Count']):
  print ("Only have ", total_parts, " of ", all_messages['Count'], " to process")
  exit(2)

final_message = ""

for i in range(total_parts):
  print(message_items[i]['part_number'], " out of ", message_items[i]['total_parts'], " is ", message_items[i]['data'])
  final_message = final_message + message_items[i]['data']

dynamodb.Table('FSMessages_complete').put_item(
    Item={
      'msgid': msg2process
    }
  )

print (final_message)




