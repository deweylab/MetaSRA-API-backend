
"""
This file is a Flask app for the MetaSRA API/back-end.  It has two main resources:
samples and terms, and a variety of end-points for accessing these resources.

To run it For development, use the following shell commands after having
installed all dependencies:
$ export FLASK_DEBUG=1
$ export FLASK_APP=metasra_api.py
$ flask run

To delopy, you should use UWSGI.
"""

# Path to the front-end repository in debug-mode/development.  (This isn't used in deployment.)
import os.path
debug_frontend_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'metasra-frontend')



from bson import json_util
from flask import Flask, request, Response
import re
from collections import OrderedDict # this is only to specify the sort order for mongodb query
import csv
from io import StringIO

app = Flask(__name__)

# Establish database connection
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import OperationFailure
db = MongoClient()['metaSRA']
DEBUG = app.config.get('DEBUG')


# Prefix all API URL routes with this stem.
urlstem = '/api/v01'



def samples():
    """
    Get parameters from the request, and lookup matching samples in the database.

    Return a python dict that looks like the JSON object to return.  (Functions
    below handle the request/response, and converting to CSV.)

    This function itself is not mapped to a URL, but it's called by functions
    which are mapped to URL's.
    """


    and_terms = [t.strip().upper() for t in request.args.get('and', '').split(',') if not t=='']
    not_terms = [t.strip().upper() for t in request.args.get('not', '').split(',') if not t=='']

    sampletype = request.args.get('sampletype')

    # Filter by study or sample ID
    studyID = request.args.get('study')

    # Return an error if we don't have and_terms or a studyID,
    # because we don't want to blow up the server by returning the whole database.
    if len(and_terms) == 0 and not studyID:
        return {'error' : 'Please enter some query terms in the "and" field, or provide a study ID.'}

    # Get skip and limit arguments for paging, and make sure that they are
    # valid integers.
    try:
        skip = int(request.args.get('skip', 0))
    except ValueError:
        skip = 0

    # -1 represents no limit
    try:
        limit = int(request.args.get('limit', -1))
    except ValueError:
        limit = -1


    # Match parameter to run against MongoDB
    matchquery = {'aterms': {'$nin': not_terms}}
    if and_terms:
        matchquery['aterms']['$all'] = and_terms
    if studyID:
        matchquery['study.id'] = studyID.upper()

    if sampletype:
        matchquery['type.type'] = re.sub(r'%20|\+', ' ', sampletype) # we want spaces instead of some other URL encodings

    try:
      result = db['samplegroups'].aggregate([

        # Use the index to find samples matching the terms-query.
        # This has to be the first aggregation stage to utilize the index.
        # TODO: add full-text matching here for the raw attribute text?
        {'$match': matchquery},

        {'$project': {'_id': False, 'aterms': False}},

        {'$group': {
            '_id' : '$study.id',
            'study': {'$first': '$study'},
            'sampleGroups': {'$push': '$$ROOT'},
            'sampleCount': {'$sum': {'$size': '$samples'}},
            'dterms': {'$push': '$dterms'}
        }},

        # Drop '_id',
        {'$project': {
            '_id': False,
            'study': True,
            'sampleGroups': True,
            'sampleCount': True,
            'dterms': {'$reduce': {
                'input': '$dterms',
                'initialValue': [],
                'in': {'$setUnion': ['$$value', '$$this']}
            }},
        }},

        {'$facet':{
            'studyCount': [{'$count': 'studyCount'}],
            'sampleCount': [{'$group': {
                                '_id': None,
                                'sampleCount': {'$sum': '$sampleCount'}
                                }}],
            'studies':
                [
                    {'$sort': {'sampleCount': -1}},
                    {'$skip': skip}
                ]
                + ([{'$limit': limit}] if limit > 0 else []),

            # Calculate most-common display terms
            'terms': [
                # Narrow down to these two fields so we don't eat unneccesary
                # memory when we unwind
                {'$project': {
                    'dterms': True,
                    'sampleCount': True
                }},

                # Group terms, count sample occurrences
                {'$unwind': '$dterms'},
                {'$group': {
                    '_id': '$dterms',
                    'sampleCount': {'$sum': '$sampleCount'}
                }},

                # Rearrange document shape and sort
                {'$project': {
                    '_id': False,
                    'dterm': '$_id',
                    'sampleCount': True,
                }},
                {'$sort': OrderedDict([
                    ('sampleCount', -1),
                    ('dterm.name', 1)
                ])}

            ]
        }}

      ]).next()
    except OperationFailure:
        return {'error': 'Your search matches too many samples and the server exceeded its memory limit.  Please try a more-specific search.'}

    # Rearrange document shape
    result['studyCount'] = result['studyCount'][0]['studyCount'] if result['studyCount'] else 0
    result['sampleCount'] = result['sampleCount'][0]['sampleCount'] if result['sampleCount'] else 0

    # Include these so the API user is not confused by implicit limit if they didn't provide one
    if limit > 0:
        result['limit'] = limit
    result['skip'] = skip

    return result





@app.route(urlstem + '/samples')
@app.route(urlstem + '/samples.json')
def samplesJSON():
    """Handle JSON request/response"""
    return jsonresponse(samples())


@app.route(urlstem + '/samples.csv')
def samplesCSV():
    """Convert data to CSV and return response"""

    result = samples()
    if 'error' in result:
        return jsonresponse(result)

    csvfile = StringIO()
    writer = csv.writer(csvfile)

    # Header
    writer.writerow(['study_id', 'study_title', 'sample_id', 'sample_name', 'sample_type',
        'sample_type_confidence', 'mapped_ontology_ids', 'mapped_ontology_terms', 'raw_SRA_metadata',])

    # Write a row for each sample
    for study in result['studies']:
        for sampleGroup in study['sampleGroups']:
            for sample in sampleGroup['samples']:
                writer.writerow([
                    study['study']['id'],
                    study['study']['title'],
                    sample['id'],
                    sample.get('name', ''),
                    sampleGroup['type']['type'],
                    sampleGroup['type']['conf'],
                    ', '.join([', '.join(term['ids']) for term in sampleGroup['dterms']]),
                    ', '.join([term['name'] for term in sampleGroup['dterms']]),
                    '; '.join([': '.join(attr) for attr in sampleGroup['attr']]),
                ])

    return Response(csvfile.getvalue(), mimetype='text/csv',
        headers={"Content-disposition": "attachment; filename=metaSRA-samples.csv"})



@app.route(urlstem + '/runs.csv')
def experimentCSV():
    """
    CSV file of search results with one run per line.
    """

    result = samples()
    if 'error' in result:
        return jsonresponse(result)

    csvfile = StringIO()
    writer = csv.writer(csvfile)

    # Header
    writer.writerow(['sra_study_id', 'study_title', 'sra_sample_id', 'sample_name', 'sra_experiment_id', 'sra_run_id'])

    # Write one row for each run
    for study in result['studies']:
        for sampleGroup in study['sampleGroups']:
            for sample in sampleGroup['samples']:
                for experiment in sample['experiments']:
                    for run in experiment['runs']:
                        writer.writerow([
                            study['study']['id'],
                            study['study']['title'],
                            sample['id'],
                            sample.get('name', ''),
                            experiment['id'],
                            run
                        ])

    return Response(csvfile.getvalue(), mimetype='text/csv',
        headers={"Content-disposition": "attachment; filename=metaSRA-runs.csv"})




@app.route(urlstem + '/runs.ids.txt')
def runIDs():
    """
    API resource returning a list of line-delimited run ID's.
    """

    result = samples()
    if 'error' in result:
        return jsonresponse(result)

    ids = []

    for study in result['studies']:
        for sampleGroup in study['sampleGroups']:
            for sample in sampleGroup['samples']:
                for experiment in sample['experiments']:
                    for run in experiment['runs']:
                        ids.append(run)

    return Response('\n'.join(ids), mimetype='text',
        headers={"Content-disposition": "attachment; filename=metaSRA-runs.ids.txt"})




# Match by all non-word characters.  This should exclude things like _
TOKEN_DELIMITER = re.compile('\W+')

def get_tokens(text):
    """
    Split the given text into tokens for the autocomplete search.

    THIS FUNCTION MUST BE EXACTLY THE SAME AS THE 'tokens' FUNCTION USED BY
    build-db.py.

    TODO: put this somewhere where both scripts can import it.
    """

    tokens = set()

    # 1) Add all tokens split by whitespace
    tokens.update(text.lower().split())

    # 2) Add all tokens split by non-word characters
    tokens.update(re.split(TOKEN_DELIMITER, text.lower()))

    return tokens






def lookupterms(q_remove_trailing_s=False):
    """
    Looks up ontology terms, returning python object shaped like the JSON to return.
    Returns records from the 'terms' collection, which represent distinct term names
    accross all the included ontologies.

    Can take 2 parameters, 'q' for text-searching (meant for the autocomplete
    function,) and 'id' which is a comma-separated list of ontology ID's.

    If the q_remove_trailing_s flag is true, remove the trailing s/S from all tokens
    in the search string (to depluralize, since users often type plurals but the
    ontology terms are mostly singular.)

    This function itself is not mapped to a URL, but it is called by functions
    which are mapped to URL's.
    """


    q = request.args.get('q')
    id = request.args.get('id')

    # Make sure limit is an integer
    limit = request.args.get('limit')
    if limit:
        try:
            limit = int(limit)
        except:
            return {'error': 'Limit argument must be an integer.', terms:[]}


    if not limit or limit > 500:
        limit = 500

    # Punt if the user didn't enter any parameters.
    if not (q or id):
        return {'error' : 'Please enter some query terms', 'terms':[]}

    # Building components to query against the terms collection in Mongo
    query = {}
    sortpipeline = [{'$sort': {'score': ASCENDING}}]



    if q:

        # Restrict terms to those having tokens prefixed by all of the
        # user-entered tokens.  Mongodb can use indexes for regex prefix queries.
        tokens = get_tokens(q)

        # Remove s or S from the end of all tokens if passed the q_remove_trailing_s flag.
        if q_remove_trailing_s:
            tokens = [token.rstrip('sS') for token in tokens]

        query['$and'] = [{'tokens': {'$regex': '^'+token}} for token in tokens]

        # This whole thing is to show first the terms that have the user's query
        # in the name of the term instead of just the synonyms.
        sortpipeline = [

            # This pipeline stage adds a 'namematch' field counting the
            # occurrences of the user's entered tokens in the term name
            {'$addFields': {
                'namematch' : {

                    # Iterate over user-provided tokens, counting how many of them are
                    # contained in the term's name tokens.
                    '$sum' : [
                        {'$cond': {
                            'if': { '$size':{
                                # Filter the list of term-name tokens to those matching the user-provided token
                                '$filter': {
                                    'input': '$nametokens',
                                    'as': 'nametoken',
                                    'cond': {'$ne': [-1,

                                        # Mongodb 3.4 doesn't support regular expressions in the project
                                        # stage of the aggregation pipeline, so we have to use the synonym_string
                                        # indexOf method.  The last 2 arguments restrict it to checking the beginning
                                        # of the string only.
                                        {'$indexOfBytes': ['$$nametoken', querytoken, 0, len(querytoken)]}
                                    ]}
                                }
                            }},
                            'then': 1,
                            'else': 0
                        }}
                        for querytoken in tokens
                    ]
                }
            }},

            # Sort first by the number of times the user's tokens occur in the
            # term name, then by score (term name length)
            {'$sort': OrderedDict([
                ('namematch', DESCENDING),
                ('score', ASCENDING)
            ])},
        ]



    # Filter by user-provided ID's
    if id:
        query['ids'] = {'$in': id.split(',')}





    limitpipeline = []
    if limit:
        limitpipeline = [{'$limit': int(limit)}]




    result = db['terms'].aggregate(
        [{'$match': query}]
        + sortpipeline
        + limitpipeline

        + [
            # Hide extra fields
            {'$project': {
                '_id': False,
                'nametokens': False,
                'score': False,
                'tokens': False,
            }}
        ]
    )

    return {'terms': list(result)}




@app.route(urlstem + '/terms')
def terms_json():
    """
    API resouce for terms.  Looks up terms and returns a JSON response.

    This funciton is essentially a wrapper around lookupterms().  If lookupterms()
    returns no results, it tries calling lookupterms() again with a flag to remove
    trailing s/S characters from query tokens.  This is so a search on "neurons"
    will match "neuron" also.
    """

    terms = lookupterms()

    # If there are no terms, try searching again with trailing s/S characters
    # removed from the tokens of the search string.
    if 'error' not in terms and not len(terms.get('terms')):
        terms = lookupterms(q_remove_trailing_s=True)

    return jsonresponse(terms)





def jsonresponse(obj):
    """Useing this instead of Flask's JSONify because of MongoDB BSON encoding"""
    return Response(json_util.dumps(obj), mimetype='application/json')




if DEBUG:

    # If we're in debug mode, start the typescript transpiler for the front-end.
    # This will start the npm process twice, because flask loads this python module
    # twice for the automatic-reloader.  This is stupid but looks harmless.

    # Commented out because I'm using Atom's typescript plugin to do the
    # compilation instead.  You might want to enable this again depending on
    # your development setup.

    #import subprocess
    #subprocess.Popen('npm run build:watch', cwd=debug_frontend_path, shell=True)


    # Only on the local development server, serve static files from the /src
    # and /node_modules directories of the front-end.
    from flask import send_from_directory

    @app.route('/node_modules/<path:filename>')
    def node_modules(filename):
        return send_from_directory(os.path.join(debug_frontend_path, 'node_modules'), filename)

    @app.route('/<path:filename>')
    def rootdir(filename):
        print('foo')
        try:
            # First try serving from the 'support pages' directory
            return send_from_directory(os.path.join(debug_frontend_path, 'src/supportpages'), filename)
        except:
            try:
                # Then try serving from the 'src' directory
                return send_from_directory(os.path.join(debug_frontend_path, 'src'), filename)
            except:
                # catch 404 and send index page instead
                return send_from_directory(os.path.join(debug_frontend_path, 'src'), "index.html")

    # Catch-all URL path to serve the index page (for single-page application)
    @app.route("/", defaults={"path": ""})
    @app.route("/<path:path>")
    def index(path):
        return send_from_directory(os.path.join(debug_frontend_path, 'src'), "index.html")
