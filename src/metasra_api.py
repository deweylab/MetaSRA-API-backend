from bson import json_util
from flask import Flask, request, Response
app = Flask(__name__)

# Establish database connection
from pymongo import MongoClient
db = MongoClient()['metaSRA']



@app.route('/samples')
def samples():
    and_terms = [t.strip().upper() for t in request.args.get('all', '').split(',') if not t=='']
    not_terms = [t.strip().upper() for t in request.args.get('none', '').split(',') if not t=='']

    # Return an error if we don't have and_terms, because we don't want to blow
    # up the server by returning the whole database.
    if len(and_terms) == 0:
        return jsonresponse({'error' : 'Please enter some query terms'})

    # Get skip and limit arguments for paging, and make sure that they are
    # valid integers.
    try:
        skip = int(request.args.get('skip', 0))
    except ValueError:
        skip = 0

    try:
        limit = int(request.args.get('limit', 10))
    except ValueError:
        limit = 10


    samples = db['samplegroups'].aggregate([

        # Use the index to find samples matching the terms-query.
        # This has to be the first aggregation stage to utilize the index.
        # TODO: add full-text matching here for the raw attribute text?
        {'$match': {
            'terms': {'$all': and_terms, '$nin': not_terms}
        }},

        {'$project': {'_id': False}},

        {'$group': {
            '_id' : '$study.id',
            'study': {'$first': '$study'},
            'samplegroups': {'$push': '$$ROOT'}
        }},

        # Drop '_id'
        {'$project': {'_id': False}},

        {'$facet':{
            'count': [{'$count': 'studycount'}],

            'studies': [
                {'$skip': skip},
                {'$limit': limit}
                # TODO: join in ontology terms here?
            ]
        }}

    ])

    return jsonresponse(samples)



def jsonresponse(obj):
    return Response(json_util.dumps(obj), mimetype='application/json')
