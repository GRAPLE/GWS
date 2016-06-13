#!/usr/bin/env python

from pymongo import MongoClient
from gwsconfig import gwsconf
import sys, string, random, datetime, pytz

db_client = MongoClient()
db = db_client[gwsconf['graple_db_name']]
collection = db[gwsconf['api_coll_name']]

def api_keygen(size = 64, chars = string.ascii_uppercase + string.digits):
    random.seed()
    bid = ''.join(random.choice(chars) for _ in range(size))
    while collection.find_one({'key':bid}) != None:
        bid = ''.join(random.choice(chars) for _ in range(size))
    return bid

if len(sys.argv) == 1:
    print 'Usage:'
    print 'drop'
    print 'insert name email tz'
    print 'delete email'
    print 'query email'
    print 'print'
    sys.exit()

operation = sys.argv[1]

# format = {
# 'key': char stream of length 64
# 'name': name of the user
# 'email': default email address of the user
# 'tz': timezone for emails 
# }

if operation == 'drop':
    collection.drop()
    print "Dropped the API key collection"
elif operation == 'insert':
    insdoc = {'key': api_keygen(),'name':sys.argv[2], 'email':sys.argv[3], 'tz':sys.argv[4]}
    if insdoc['tz'] in pytz.all_timezones:
        print "Inserted at ID:", collection.insert_one(insdoc).inserted_id
    else:
        print "Timezone not found. Closest matches:"
        for tzitem in pytz.all_timezones:
            if insdoc['tz'] in tzitem:
                print tzitem
elif operation == 'delete':
    print "Found:", collection.find_one({'email':sys.argv[2]})
    print "Delete count:", collection.delete_one({'email':sys.argv[2]}).raw_result['n']
elif operation == 'query':
    print collection.find_one({'email':sys.argv[2]})
elif operation == 'print':
    for doc in collection.find({}):
        print doc
