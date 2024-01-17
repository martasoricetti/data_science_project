# Supposing that all the classes developed for the project
# are contained in the file 'impl.py', then:
from pprint import pprint

# 1) Importing all the classes for handling the relational database
from impl import RelationalDataProcessor, RelationalQueryProcessor
from impl_generic import GenericQueryProcessor

# 2) Importing all the classes for handling RDF database
#from impl import TriplestoreDataProcessor, TriplestoreQueryProcessor
from graph_functions_and_tests import *
from impl_graph import *
# 3) Importing the class for dealing with generic queries
#from impl import GenericQueryProcessor

# Once all the classes are imported, first create the relational
# database using the related source data
rel_path = "relational.db"
rel_dp = RelationalDataProcessor()
rel_dp.setDbPath(rel_path)
#rel_dp.uploadData("data/relational_publications.csv")
#rel_dp.uploadData("data/relational_other_data.json")

# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint = " http://10.201.26.43:9999/blazegraph/sparql"
#grp_dp = TriplestoreDataProcessor()
#grp_dp.setEndpointUrl(grp_endpoint)
#grp_dp.uploadData("data/graph_publications.csv")
#grp_dp.uploadData("data/graph_other_data.json")

# In the next passage, create the query processors for both
# the databases, using the related classes
rel_qp = RelationalQueryProcessor()
rel_qp.setDbPath(rel_path)

grp_qp = TriplestoreQueryProcessor()
grp_qp.setEndpointUrl(grp_endpoint)
# Finally, create a generic query processor for asking
# about data

generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)
#generic.addQueryProcessor(grp_qp)

#result_q1 = grp_qp.getJournalArticlesInJournal('issn:0219-1377')
result_q1 = generic.getDistinctPublisherOfPublications(['doi:10.1007/s11192-019-03217-6','doi:10.3390/admsci10030069'])
print(result_q1)

#a= grp_qp.getOrganization(crossref_id="crossref:2780").getName()
#a= rel_qp.getPublicationsPublishedInYear('2017')


