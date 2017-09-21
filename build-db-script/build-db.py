"""
Builds mongodb database from SQLite files
"""

# TODO: put these config variables somewhere better
SRA_SUBSET_SQLITE_LOCATION = '/home/matt/projects/MetaSRA/mb-database-code/metasra_website/SRAmetadb.subdb.17-06-22.sqlite'
METASRA_PIPELINE_OUTPUT_SQLITE_LOCATION = '/home/matt/projects/MetaSRA/mb-database-code/metasra.sqlite'

# Attributes to remove so they don't interfere when samples are grouped by like
# attributes.  These should be sample-level ID's that don't contain meaningful
# information.  (Sometimes tricky because different studies use these labels
# differently.)
ATTRIBUTE_GROUPING_BLACKLIST = set((
    'gap_sample_id',
    'gap_subject_id',
    'submitted sample id',
    'submitted subject id',
    'sample id',
    'sample_id',
    'individual',
    'c1 chip id',
    'biospecimen repository sample id',
    'replicate',
    'section',
    'mrna-seq reads',
    'subject_id',
    'flowcell',
    'brain_number',
    'donor id',
    'biological replicate',
    # 'isolate', ?
    'experimental batch',
))


# When looking up ancestor terms and descendent terms to display in the autocomplete,
# if there are more ancestors/descendents than this number at radius 2,
# then only include terms from radius 1.
RELATED_TERM_SHRINKAGE_THRESHOLD = 50


# We're grouping ontology terms by name.  If a term has ID's in multiple ontologies,
# sort/prioritize them in this order.  For when we only want one term ID, eg for
# term tag hilighting, choose the one with the highest precedence.
ONTOLOGY_PRECEDENCE_ORDER = ['CVCL', 'DOID', 'CL', 'UBERON', 'EFO']
def ontology_precedence(term_id):
    return ONTOLOGY_PRECEDENCE_ORDER.index(term_id.split(':')[0])




from pymongo import MongoClient, ASCENDING
import sqlite3
import re


from onto_lib import load_ontology, ontology_graph, general_ontology_tools
ONT_NAME_TO_ONT_ID = {"EFO_CL_DOID_UBERON_CVCL":"17"}
ONT_ID_TO_OG = {x:load_ontology.load(x)[0] for x in ONT_NAME_TO_ONT_ID.values()}



def new_output_db():
    """
    Create and return a new, empty mongo database 'metaSRA', and rename the old
    one to 'mongo_old'
    """

    # Connection uses localhost and default port, change here if need be
    client = MongoClient()

    # TODO: use database version numbers instead of _old?
    if 'metaSRA' in client.database_names():
        print("Renaming old database to metaSRA_old")
        if 'metaSRA_old' in client.database_names():
            client.drop_database('metaSRA_old')
        client.admin.command('copydb', fromdb='metaSRA', todb='metaSRA_old')
        client.drop_database('metaSRA')

    return client['metaSRA']





def get_samples():
    with sqlite3.connect(SRA_SUBSET_SQLITE_LOCATION) as conn:
        conn.row_factory = sqlite3.Row # so we can use column names instead of indices
        return conn.execute("""
            SELECT sample_accession, study_accession, study_title
            FROM (sample JOIN experiment USING (sample_accession)) JOIN study USING (study_accession);
        """)




def lookup_attributes_and_samplename(sampleID, SRAconnection):
    """
    Look up sammplenames and raw attributes from the SRA subset database.

    The sample name is stored as a the attribute 'source_name', and we're pulling
    it out so we can treat it separately, and so it doesn't affect the sample
    groupings when we later group them by attributes.
    """

    cursor = SRAconnection.execute("""
        SELECT tag, value
        FROM sample_attribute
        WHERE sample_accession = ?
    """, (sampleID,))

    # Putting attributes in a list of (key,value) tuples instead of just a
    # key:value object, because Mongodb has restrictions on certain characters
    # being used in keys.
    attributes, samplename = [], None
    for (k,v) in cursor:
        if k == 'source_name':
            samplename = v
        elif k not in ATTRIBUTE_GROUPING_BLACKLIST:
            #attributes.append({'k':k, 'v':'v'})
            attributes.append((k,v))

    #return sorted(attributes, key=lambda d: (d['k'], d['v'])), samplename
    return sorted(attributes), samplename



def lookup_ontology_terms(sampleID, metaSRAconnection):
    """
    So far, just returns a list of ontology term ID's for a given sample.
    Eventually, I should amend this to look up the term text.
    """

    # TODO: include ontolgy term text
    terms = metaSRAconnection.execute("""
        SELECT term_id
        FROM mapped_ontology_terms WHERE
        sample_accession = ? ;
    """, (sampleID,))
    return sorted([s['term_id'] for s in terms])





def lookup_sample_type(sampleID, metaSRAconnection):
    """

    """

    sampletypes = metaSRAconnection.execute("""
        SELECT sample_type, confidence
        FROM sample_type
        where sample_accession = ?
    """, (sampleID,))

    r = [{'type': t['sample_type'], 'conf': t['confidence']} for t in sampletypes]
    return r[0] if len(r) else None




def build_samples(outdb):
    """
    Imports the samples table into MongoDB, and looks up sample attributes
    for each.
    """

    print('Building sample table')

    with sqlite3.connect(SRA_SUBSET_SQLITE_LOCATION) as SRAconnection:
     with sqlite3.connect(METASRA_PIPELINE_OUTPUT_SQLITE_LOCATION) as metaSRAconnection:
        SRAconnection.row_factory = sqlite3.Row # so we can use column names instead of indices
        metaSRAconnection.row_factory = sqlite3.Row


        # Create indices for faster lookups against individual sample ID's
        SRAconnection.executescript("""
            CREATE INDEX IF NOT EXISTS
                sample_attr_ind ON sample_attribute(sample_accession);
        """)
        metaSRAconnection.executescript("""
            CREATE INDEX IF NOT EXISTS
                mapped_ontology_terms_ind ON mapped_ontology_terms(sample_accession);
            CREATE INDEX IF NOT EXISTS
                sample_type_ind on sample_type(sample_accession);
        """)



        for sample in get_samples():
            attributes, samplename = lookup_attributes_and_samplename(
                sample['sample_accession'], SRAconnection)

            # Insert a document for this sample.
            # A stupid thing about big document-store databases is that keys
            # need to be kept short to save space.
            document = {
                'id': sample['sample_accession'],
                'study': {
                    'id': sample['study_accession'],
                    'title': sample['study_title']
                },
                'attr': attributes,
                'terms': lookup_ontology_terms(sample['sample_accession'], metaSRAconnection),
                'type': lookup_sample_type(sample['sample_accession'], metaSRAconnection)
            }
            if samplename:
                document['name'] = samplename
            outdb['samples'].insert_one(document)




def group_samples(outdb):
    """
    Group samples by study accession and raw attributes, and put them in a new
    collection called 'samplegroups'.
    """

    print('Grouping samples by same attributes')
    outdb['samples'].aggregate([

        # group by study accession and raw attributes
        {'$group': {
            '_id': {
                'attr': '$attr',
                'studyid': '$study.id',
                'terms': '$terms'
            },
            'samples': {'$addToSet': {
                'id': '$id',
                'type': '$type',
                'name': '$name'
            }},
            'study': {'$first': '$study'}
        }},

        # re-shape the document to have 'attr' and 'study' fields instead of
        # tucking them in '_id'.
        {'$project': {
            'attr': '$_id.attr',
            'terms': '$_id.terms',
            '_id': False, # suppress '_id' field
            'samples': True, # include 'samples',
            'study': True
        }},

        # Send to a new collection called 'samplegroups'
        {'$out': 'samplegroups'}
    ], allowDiskUse=True)





def elaborate_samplegroup_terms(outdb):
    """
    For each sample group, 1) find the set of terms to display by removing terms that have
    children in the set, and 2) find a different set of terms to use for computing the
    search queries by including ancestors of the terms in the set.
    """

    print('Looking up most-specific terms and ancestral terms')
    for samplegroup in outdb['samplegroups'].find().sort('_id', ASCENDING):

        # Terms to display
        dterm_ids = ontology_graph.most_specific_terms(samplegroup['terms'],
            ONT_ID_TO_OG["17"],
            sup_relations=["is_a", "part_of"])

        dterms = [[tid, general_ontology_tools.get_term_name(tid)] for tid in dterm_ids]


        # Ancestral terms
        aterms = set(samplegroup['terms'])
        for term in samplegroup['terms']:
            aterms.update(ONT_ID_TO_OG["17"].recursive_relationship(term, ["is_a", "part_of"]))

        outdb['samplegroups'].update_one(
            {'_id': samplegroup['_id']},
            {'$set':{
                'dterms': list(sorted(dterms)),
                'aterms': list(aterms)
                },
            '$unset': {'terms': 1}
            },
        )




def get_distinct_termIDs(outdb):
    """
    Create a new collection 'terms' with one document for every distinct term
    in the 'aterms' field of the 'samplegroups' collection.
    """

    print('Creating collection of distinct terms.')

    outdb['samplegroups'].aggregate([

        # This step is redundant but may possibly speed things up a bit by getting
        # rid of extra fields before creating a bunch of new documents.
        {'$project': {
            'id': '$aterms',
            '_id': False,
        }},

        # One document for each term in each samplegroup.
        {'$unwind': {
            'path': '$id'
        }},

        # Get distinct terms.
        {'$group': {
            '_id': '$id'
        }},

        # Rename _id to id.
        {'$project': {
            'id': '$_id',
            '_id': False
        }},

        # Send to collection called 'terms'.
        {'$out': 'termIDs'}
    ], allowDiskUse=True)






def get_term_names(outdb):
    """
    Look up names for all terms in the 'termIDs' collection, and then create the
    'terms' collection with one document for each term name.
    """

    print("Getting distinct term names")

    # Look up term names for all ontology terms
    for term in outdb['termIDs'].find().sort('_id', ASCENDING):
        term_name = general_ontology_tools.get_term_name(term['id'])
        outdb['termIDs'].update_one(
            {'_id': term['_id']},
            {'$set':{
                'name': term_name,
                },
            },
        )

    # Create terms collection, with a list of term ID's for each distinct term name.
    outdb['termIDs'].aggregate([
        {'$group': {
            '_id': '$name',
            'ids': {'$push': '$id'}
        }},
        {'$project': {
            '_id': False,
            'name': '$_id',
            'ids': True
        }},
        {'$out': 'terms'}
    ])





# Match by all non-word characters.  This should exclude things like _
TOKEN_DELIMITER = re.compile('\W+')

def get_tokens(text):
    """
    Split the given text into tokens for the autocomplete search.

    THIS FUNCTION MUST BE EXACTLY THE SAME AS THE 'tokens' FUNCTION USED BY
    THE API BACK_END.

    TODO: put this somewhere where both scripts can import it.
    """

    tokens = set()

    # 1) Add all tokens split by whitespace
    tokens.update(text.lower().split())

    # 2) Add all tokens split by non-word characters
    tokens.update(re.split(TOKEN_DELIMITER, text.lower()))

    return tokens






def distinct_terms_from_term_ids(term_ids):
    """
    Given an iterable of term ID's, look up names for each term and group the
    terms by name.

    Results look like this: {term_name: [term_id1, term_id2, ...]}
    """

    term_names = dict()
    for term_id in term_ids:
        term_name = general_ontology_tools.get_term_name(term_id)
        if term_name in term_names:
            term_names[term_name].append(term_id)
        else:
            term_names[term_name] = [term_id]

    return term_names




def term_id_in_metasra(term_id, outdb):
    """
    Given a term ID, check to see if it has any matching samples in MetaSRA.
    """

    if outdb['samplegroups'].find_one({'aterms': term_id}):
        return True
    else:
        return False





ANCESTORS, DESCENDENTS = 1, 2
def lookup_related_terms(term_ids, direction, outdb):
    """
    Find ancestor or decendent terms for a list of term ID's, formatted to go in
    the DB.
    """

    # Get the function to go either up or down the ontology
    lookup_function = (general_ontology_tools.get_ancestors_within_radius if
        direction == ANCESTORS else general_ontology_tools.get_descendents_within_radius)

    """
    # Look up terms at radius 1
    related_term_ids = set()
    for term_id in term_ids:
        related_term_ids.update(lookup_function(term_id, 1))
    related_term_names = distinct_terms_from_term_ids(related_term_ids)

    # If we're below the threshold (not enough terms), look up terms from radius 2
    if len(related_term_names) < RELATED_TERM_EXPANSION_THRESHOLD:
        for term_id in term_ids:
            related_term_ids.update(lookup_function(term_id, 2))
        related_term_names = distinct_terms_from_term_ids(related_term_ids)
    """

    # Look up terms at radius 2
    related_term_ids = set()
    for term_id in term_ids:
        related_term_ids.update(lookup_function(term_id, 2))

    # Exclude terms that don't match any samples in SRA
    filtered_term_ids = filter(lambda term: term_id_in_metasra(term, outdb), related_term_ids)
    related_term_names = distinct_terms_from_term_ids(filtered_term_ids)



    # If we have too many terms at radius 2, repeat only including terms from radius 1
    if len(related_term_names) > RELATED_TERM_SHRINKAGE_THRESHOLD:
        related_term_ids = set()
        for term_id in term_ids:
            related_term_ids.update(lookup_function(term_id, 1))
        # Exclude terms that don't match any samples in SRA
        filtered_term_ids = filter(lambda term: term_id_in_metasra(term, outdb), related_term_ids)
        related_term_names = distinct_terms_from_term_ids(filtered_term_ids)




    # Sort, and get into proper shape to go into the database
    return sorted([{
        'ids': sorted(term_ids, key=ontology_precedence),
        'name': term_name
    } for (term_name, term_ids) in related_term_names.items()],
    key=lambda term: term['name'])







def lookup_term_attributes(outdb):
    """
    For each term in the 'terms' collection, populate fields gleaned from ontolib.
    """

    print('Looking up term info from ontolib')
    for term in outdb['terms'].find().sort('_id', ASCENDING):
        term_ids = term['ids']
        term_name = term['name']


        # Look up set of synonyms for all ID's for this term
        name_and_synonyms = set()
        for term_id in term_ids:
            name_and_synonyms.update(general_ontology_tools.get_term_name_and_synonyms(term_id))

        # Get tokens for finding autocomplete terms, from name and synonyms
        tokens = set()
        for text in name_and_synonyms:
            tokens.update(get_tokens(text))

        # Keep a field with term-name tokens, so we can rank the term higher if it
        # matches the term name instead of only the synonyms.
        name_tokens = get_tokens(term_name)

        # Lookup ancestor and descendent terms to show in the autocomplete
        ancestor_terms = lookup_related_terms(term_ids, ANCESTORS, outdb)
        descendent_terms = lookup_related_terms(term_ids, DESCENDENTS, outdb)

        # Synonym string for display
        synonyms = name_and_synonyms.copy()
        synonyms.remove(term_name)
        synonym_string = ', '.join(sorted(synonyms))

        # Heuristic for sorting autocomplete results
        score = len(term_name)

        # Put ID's in order of precedence
        term_ids.sort(key=ontology_precedence)

        outdb['terms'].update_one(
            {'_id': term['_id']},
            {'$set':{
                'ids': term_ids,
                'syn': synonym_string,
                'tokens': list(tokens),
                'nametokens': list(name_tokens),
                'ancestors': ancestor_terms,
                'descendents': descendent_terms,
                'score': score
                },
            },
        )






if __name__ == '__main__':
    #outdb = new_output_db()
    #build_samples(outdb)

    #group_samples(outdb)
    #elaborate_samplegroup_terms(outdb)

    # add terms index for sample queries
    #print('Creating ancestral terms index on samplegroups collection')
    #outdb['samplegroups'].create_index('aterms')

    outdb = MongoClient()['metaSRA']
    get_distinct_termIDs(outdb)

    get_term_names(outdb)
    lookup_term_attributes(outdb)

    # Add token index for term autocomplete queries, and id index for lookup
    print('Creating id and token indices on terms collection')
    outdb['terms'].create_index('tokens')
    outdb['terms'].create_index('ids')

    print('Dropping intermediate, unused collections')
    #outdb['samples'].drop()
    outdb['termIDs'].drop()
