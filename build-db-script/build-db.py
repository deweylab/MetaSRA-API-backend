"""
Builds mongodb database from SQLite files
"""

# TODO: put these config variables somewhere better
SRA_SUBSET_SQLITE_LOCATION = '/home/matt/projects/MetaSRA/mb-database-code/metasra_website/SRAmetadb.subdb.17-06-22.sqlite'
METASRA_PIPELINE_OUTPUT_SQLITE_LOCATION = '/home/matt/projects/MetaSRA/mb-database-code/metasra.sqlite'


from pymongo import MongoClient
import sqlite3



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
        else:
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
                'terms': '$terms',
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

    # add index for term queries
    print('Creating terms index on samplegroups collection')
    outdb['samples'].create_index('terms')






if __name__ == '__main__':
    #outdb = new_output_db()
    #build_samples(outdb)

    outdb = MongoClient()['metaSRA']
    group_samples(outdb)
