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
))

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




def get_distinct_terms(outdb):
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
        {'$out': 'terms'}
    ], allowDiskUse=True)



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



def lookup_term_attributes(outdb):
    """
    For each term in the 'terms' collection, populate fields gleaned from ontolib.
    """

    print('Looking up term info from ontolib')
    for term in outdb['terms'].find().sort('_id', ASCENDING):
        term_id = term['id']

        term_name = general_ontology_tools.get_term_name(term_id)
        name_and_synonyms = general_ontology_tools.get_term_name_and_synonyms(term_id)


        # get tokens for autocomplete
        tokens = set()
        for text in name_and_synonyms:
            tokens.update(get_tokens(text))

        up = sorted([[tid, general_ontology_tools.get_term_name(tid)] for tid
            in general_ontology_tools.get_ancestors_within_radius(term_id, 2)],

            # Sort by term text ascending
            key=lambda t: t[1])

        down = sorted([[tid, general_ontology_tools.get_term_name(tid)] for tid
            in general_ontology_tools.get_descendents_within_radius(term_id, 2)],

            # Sort by term text ascending
            key=lambda t: t[1])


        # Synonym string for display
        synonyms = name_and_synonyms.copy()
        synonyms.remove(term_name)
        synonym_string = ', '.join(sorted(synonyms))


        # Heuristic for sorting autocomplete results
        score = len(term_name)


        outdb['terms'].update_one(
            {'_id': term['_id']},
            {'$set':{
                'name': term_name,
                'syn': synonym_string,
                'tokens': list(tokens),
                'up': up,
                'down': down,
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
    #outdb['samples'].create_index('aterms')

    outdb = MongoClient()['metaSRA']
    get_distinct_terms(outdb)
    lookup_term_attributes(outdb)

    # Add token index for term autocomplete queries, and id index for lookup
    print('Creating id and token indices on terms collection')
    outdb['terms'].create_index('tokens')
    outdb['terms'].create_index('id')
