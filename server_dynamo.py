#!/usr/bin/env python
"""
Client which receives the requests

Args:
    API Token
    API Base (https://...)

"""
from flask import Flask, request
import logging
import argparse
import urllib2
import boto3

# logging.basicConfig(level=logging.DEBUG)

# parsing arguments
PARSER = argparse.ArgumentParser(description='Client message processor')
PARSER.add_argument('API_token', help="the individual API token given to your team")
PARSER.add_argument('API_base', help="the base URL for the game API")

ARGS = PARSER.parse_args()

# defining global vars
MESSAGES = {} # A dictionary that contains message parts
API_BASE = ARGS.API_base
# 'https://csm45mnow5.execute-api.us-west-2.amazonaws.com/dev'

DYNAMODB = boto3.resource('dynamodb')
STATE_TABLE = DYNAMODB.Table('FSMessages')

APP = Flask(__name__)

# creating flask route for type argument
@APP.route('/', methods=['GET', 'POST'])
def main_handler():
    """
    main routing for requests
    """
    if request.method == 'POST':
        return process_message(request.get_json())
    else:
        return get_message_stats()

def get_message_stats():
    """
    provides a status that players can check
    """
    msg_count = len(MESSAGES.keys())
    return "There are %d messages in the MESSAGES dictionary" % msg_count

def process_message(msg):
    """
    processes the messages by combining and appending the kind code
    """
    msg_id = msg['Id'] # The unique ID for this message
    part_number = msg['PartNumber'] # Which part of the message it is
    data = msg['Data'] # The data of the message

    #store message
    store_message(msg_id, part_number, data)

    #check and process message if applicable
    check_messages(msg_id)

    # Try to get the parts of the message from the MESSAGES dictionary.
    # If it's not there, create one that has None in both parts
    parts = MESSAGES.get(msg_id, [None, None])

    # store this part of the message in the correct part of the list
    parts[part_number] = data

    # store the parts in MESSAGES
    MESSAGES[msg_id] = parts

    # if both parts are filled, the message is complete
    if None not in parts:
        # app.logger.debug("got a complete message for %s" % msg_id)
        print "have both parts"
        # We can build the final message.
        result = parts[0] + parts[1]
        # sending the response to the score calculator
        # format:
        #   url -> api_base/jFgwN4GvTB1D2QiQsQ8GHwQUbbIJBS6r7ko9RVthXCJqAiobMsLRmsuwZRQTlOEW
        #   headers -> x-gameday-token = API_token
        #   data -> EaXA2G8cVTj1LGuRgv8ZhaGMLpJN2IKBwC5eYzAPNlJwkN4Qu1DIaI3H1zyUdf1H5NITR
        APP.logger.debug("ID: %s" % msg_id)
        APP.logger.debug("RESULT: %s" % result)
        url = API_BASE + '/' + msg_id
        print url
        print result
        req = urllib2.Request(url, data=result, headers={'x-gameday-token':ARGS.API_token})
        resp = urllib2.urlopen(req)
        resp.close()

    return 'OK'

def store_message(input_id, part_num, data):
    """
    stores the message locally on a file on disk for persistence
    """
    try:
        STATE_TABLE.update_item(
            Key={
                'Id': input_id
            },
            UpdateExpression="set #key=:val",
            ExpressionAttributeValues={
                ":val":data
            },
            ExpressionAttributeNames={
                "#key":str(part_num),
                "#p":"processed"
            },
            ConditionExpression="attribute_not_exists(#p)"
        )
    except Exception:
        # conditional update failed since we have already processed this message
        # at this point we can bail since we don't want to process again
        # and lose cash moneys
        return False


def check_messages(input_id):
    """
    checking to see in dynamo if we have the part already
    """
    # do a get item from dynamo to see if item exists
    response = STATE_TABLE.get_item(
        Key={
            'Id': input_id
        },
        ConsistentRead=True
    )
    item = response['Item']
    # check if both parts exist
    if "0" in item and "1" in item:
        print("we have both!")
        # we have all the parts
        build_final(item, input_id)
        # now we need to update dynamo saying we processed this message
	    # TODO mark_processed(input_id)
    else:
        # we have some parts but not all
        return

def build_final(item, input_id):
    print "have both parts"
    result = item[0]['data'] + item[1]['data']
    # sending the response to the score calculator
    # format:
    #   url -> api_base/jFgwN4GvTB1D2QiQsQ8GHwQUbbIJBS6r7ko9RVthXCJqAiobMsLRmsuwZRQTlOEW
    #   headers -> x-gameday-token = API_token
    #   data -> EaXA2G8cVTj1LGuRgv8ZhaGMLpJN2IKBwC5eYzAPNlJwkN4Qu1DIaI3H1zyUdf1H5NITR
    APP.logger.debug("ID: %s" % msg_id)
    APP.logger.debug("RESULT: %s" % result)
    url = API_BASE + '/' + msg_id
    print url
    print result
    req = urllib2.Request(url, data=result, headers={'x-gameday-token': ARGS.API_token})
    resp = urllib2.urlopen(req)
    resp.close()
    return 'OK'

def mark_processed(input_id):
    try:
        STATE_TABLE.update_item(
            Key={
                'Id': input_id
            },
            UpdateExpression="set #key=:val",
            ExpressionAttributeValues={
                ":val":data
            },
            ExpressionAttributeNames={
                "#key":str(part_num),
                "#p":"processed"
            },
            ConditionExpression="attribute_not_exists(#p)"
        )
    except Exception:
        # conditional update failed since we have already processed this message
        # at this point we can bail since we don't want to process again
        # and lose cash moneys
        return False

if __name__ == "__main__":

    # By default, we disable threading for "debugging" purposes.
    # This will cause the app to block requests, which means that you miss out on some points,
    # and fail ALB healthchecks, but whatever I know I'm getting fired on Friday.
    APP.run(host="0.0.0.0", port="80")
    
    # Use this to enable threading:
    # APP.run(host="0.0.0.0", port="80", threaded=True)
