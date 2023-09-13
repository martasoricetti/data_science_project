# Supposing that all the classes developed for the project
# are contained in the file 'impl.py', then:
from pprint import pprint

# 1) Importing all the classes for handling the relational database
from impl import RelationalDataProcessor, RelationalQueryProcessor
from impl_graph import TriplestoreProcessor, TriplestoreDataProcessor
# 2) Importing all the classes for handling RDF database
#from impl import TriplestoreDataProcessor, TriplestoreQueryProcessor

# 3) Importing the class for dealing with generic queries
#from impl import GenericQueryProcessor

# Once all the classes are imported, first create the relational
# database using the related source data
rel_path = "relational.db"
rel_dp = RelationalDataProcessor()
rel_dp.setDbPath(rel_path)
rel_dp.uploadData("data/relational_publications.csv")
rel_dp.uploadData("data/relational_other_data.json")

# Then, create the RDF triplestore (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint = "http://192.168.1.2:9999/blazegraph/"
grp_dp = TriplestoreDataProcessor()
grp_dp.setEndpointUrl(grp_endpoint)
grp_dp.uploadData("data/graph_publications.csv")
grp_dp.uploadData("data/graph_other_data.json")

# In the next passage, create the query processors for both
# the databases, using the related classes
rel_qp = RelationalQueryProcessor()
rel_qp.setDbPath(rel_path)

'''grp_qp = TriplestoreQueryProcessor()
grp_qp.setEndpointUrl(grp_endpoint)

# Finally, create a generic query processor for asking
# about data
generic = GenericQueryProcessor()
generic.addQueryProcessor(rel_qp)
generic.addQueryProcessor(grp_qp)'''

#result_q1 = rel_qp.getPublicationsPublishedInYear(2020)
#result_q2 = generic.getPublicationsByAuthorId("0000-0001-9857-1511")
#pprint(result_q1)
