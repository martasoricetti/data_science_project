from argparse import ArgumentParser

# Supposing that all the classes developed for the project
# are contained in the file 'impl.py', then:
from pprint import pprint

# 1) Importing all the classes for handling the relational database
from impl import RelationalDataProcessor, RelationalQueryProcessor
from impl_graph import TriplestoreProcessor, TriplestoreDataProcessor
# 2) Importing all the classes for handling RDF database
from impl_graph import TriplestoreDataProcessor #, TriplestoreQueryProcessor

# 3) Importing the class for dealing with generic queries
#from impl_graph import GenericQueryProcessor

def run_software(rel_db_path, rel_csv, rel_json, grp_endpoint, grp_csv, grp_json):

    # Once all the classes are imported, first create the relational
    # database using the related source data
    rel_dp = RelationalDataProcessor()
    rel_dp.setDbPath(rel_db_path)
    rel_dp.uploadData(rel_csv)
    rel_dp.uploadData(rel_json)

    # Then, create the RDF triplestore (remember first to run the
    # Blazegraph instance) using the related source data
    '''grp_dp = TriplestoreDataProcessor()
    grp_dp.setEndpointUrl(grp_endpoint)
    grp_dp.uploadData(grp_csv)
    grp_dp.uploadData(grp_json)'''

    # In the next passage, create the query processors for both
    # the databases, using the related classes
    rel_qp = RelationalQueryProcessor()
    rel_qp.setDbPath(rel_db_path)

    #grp_qp = TriplestoreQueryProcessor()
    #grp_qp.setEndpointUrl(grp_endpoint)

    # Finally, create a generic query processor for asking
    # about data
    '''generic = GenericQueryProcessor()
    generic.addQueryProcessor(rel_qp)
    generic.addQueryProcessor(grp_qp)'''

    #result_q1 = rel_qp.getPublicationsPublishedInYear(2020)
    #result_q2 = generic.getPublicationsByAuthorId("0000-0001-9857-1511")
    #pprint(result_q2)

if __name__ == '__main__':
    arg_parser = ArgumentParser('run.py', description='''This script is responsible for running the entire software: it 
    instantiate the RelationalDataProcessor, RelationalQueryProcessor, TriplestoreDataProcessor, TriplestoreQueryProcessor, GenericQueryProcessor objects;
    upload the data in the two databases and makes the environment ready to query them''')
    arg_parser.add_argument('-db', '--rel_db', dest='rel_db_path', required=True,
                            help='path to the file.db')
    arg_parser.add_argument('-rd1', '--relational_data_csv', dest='rel_csv', required=True,
                            help='path to the csv file containing data to upload in the relational database')
    arg_parser.add_argument('-rd2', '--relational_data_json', dest='rel_json', required=True,
                            help='path to the json file containing data to upload in the relational database')
    arg_parser.add_argument('-grp', '--graph_db', dest='grp_endpoint', required=True,
                            help='endpoint to the RDF triplestore')
    arg_parser.add_argument('-grpd1', '--graph_data_csv', dest='grp_csv', required=True,
                            help='path to the csv file containing data to upload in the triplestore')
    arg_parser.add_argument('-grp2', '--graph_data_json', dest='grp_json', required=True,
                            help='path to the json file containing data to upload in the triplestore')
    args = arg_parser.parse_args()
    rel_db_path = args.rel_db_path
    rel_csv = args.rel_csv
    rel_json = args.rel_json
    grp_endpoint = args.grp_endpoint
    grp_csv = args.grp_csv
    grp_json = args.grp_json

    run_software(rel_db_path, rel_csv, rel_json, grp_endpoint, grp_csv, grp_json)
