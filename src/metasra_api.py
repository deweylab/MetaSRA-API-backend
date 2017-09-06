
# Path to the front-end repository in debug-mode/development.  (This isn't used in deployment.)
import os.path
debug_frontend_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', 'metasra-frontend')



from bson import json_util
from flask import Flask, request, Response
import re
app = Flask(__name__)

# Establish database connection
from pymongo import MongoClient, ASCENDING
db = MongoClient()['metaSRA']

# Prefix all API URL routes with this stem.  This should be handled by the web
# server in deployment, but we need to do this for the local development server.
DEBUG = app.config.get('DEBUG')
urlstem = '/api/v01' if DEBUG else ''


@app.route(urlstem + '/samples')
def samples():
    and_terms = [t.strip().upper() for t in request.args.get('and', '').split(',') if not t=='']
    not_terms = [t.strip().upper() for t in request.args.get('not', '').split(',') if not t=='']

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


    result = db['samplegroups'].aggregate([

        # Use the index to find samples matching the terms-query.
        # This has to be the first aggregation stage to utilize the index.
        # TODO: add full-text matching here for the raw attribute text?
        {'$match': {
            'aterms': {'$all': and_terms, '$nin': not_terms}
        }},

        {'$project': {'_id': False, 'aterms': False}},

        {'$group': {
            '_id' : '$study.id',
            'study': {'$first': '$study'},
            'sampleGroups': {'$push': '$$ROOT'}
        }},

        # Drop '_id'
        {'$project': {'_id': False}},

        {'$facet':{
            'studyCount': [{'$count': 'studyCount'}],

            'studies': [
                {'$skip': skip},
                {'$limit': limit}
                # TODO: join in ontology terms here?
            ]
        }}

    ]).next()

    result['studyCount'] = result['studyCount'][0]['studyCount'] if result['studyCount'] else 0

    return jsonresponse(result)






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






@app.route(urlstem + '/terms')
def terms():
    q = request.args.get('q')
    id = request.args.get('id')

    if not (q or id):
        return jsonresponse({'error' : 'Please enter some query terms', 'terms':[]})

    query = {}

    if q:
        tokens = get_tokens(q)
        query['$and'] = [{'tokens': {'$regex': '^'+token}} for token in tokens]

    if id:
        query['id'] = id

    result = db['terms'].find(query).sort('score', ASCENDING)

    return jsonresponse({'terms': result})





def jsonresponse(obj):
    """Useing this instead of Flask's JSONify because of MongoDB BSON encoding"""
    return Response(json_util.dumps(obj), mimetype='application/json')




if DEBUG:

    # If we're in debug mode, start the typescript transpiler for the front-end.
    # This will start the npm process twice, because flask loads this python module
    # twice for the automatic-reloader.  This is stupid but looks harmless.
    import subprocess
    subprocess.Popen('npm run build:watch', cwd=debug_frontend_path, shell=True)


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
            return send_from_directory(os.path.join(debug_frontend_path, 'src'), filename)
        except:
            # catch 404 and send index page instead
            return send_from_directory(os.path.join(debug_frontend_path, 'src'), "index.html")

    # Catch-all URL path to serve the index page (for single-page application)
    @app.route("/", defaults={"path": ""})
    @app.route("/<path:path>")
    def index(path):
        return send_from_directory(os.path.join(debug_frontend_path, 'src'), "index.html")
