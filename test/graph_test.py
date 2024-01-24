import os.path
import unittest
from impl_graph import * 
from graph_functions_and_tests import *
from os.path import join
import sqlite3

import unittest

from rdflib import Graph
from rdflib import URIRef
from rdflib import RDF
from rdflib import Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get

class TestGraph(unittest.TestCase):

    def setUp(self) -> None:
        self.test_dir = join("test", "data_graph")
        self.graph_data = join(self.test_dir, "graph_project_data")
        self.graph_csv = join(self.graph_data, "graph_publications_test.csv")
        self.graph_json = join(self.graph_data, "graph_other_data_test.json")
        self.endpointUrl='http://10.201.6.200:9999/blazegraph/sparql' #change with the updated Url obtained by launching blazegraph
        
    

    def uploadData(self):
        grp_dp = TriplestoreDataProcessor()
        
        grp_dp.setEndpointUrl(self.endpointUrl)
        grp_dp.uploadData(self.graph_csv)
        grp_dp.uploadData(self.graph_json)

    def instantiateTriQP(self):
        
            grp_qp = TriplestoreQueryProcessor()
            grp_qp.setEndpointUrl(self.endpointUrl)
            return grp_qp

            
    def test_uploadData(self):
        self.uploadData()  

        query='''PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
            PREFIX schema: <https://schema.org/>
              PREFIX cito:<http://purl.org/spar/cito/> 
              prefix dcterms: <http://purl.org/dc/terms/> 
              prefix fabio: <http://purl.org/spar/fabio/>  
              SELECT ?internalID ?id ?title ?publicationVenue ?publication_year ?author ?cited 
              WHERE{ ?internalID schema:identifier ?id;  
                schema:author ?authoruri;  
                dcterms:title ?title; 
                schema:datePublished  ?publication_year;  
                schema:isPartOf ?publicationVenueuri. 
                ?publicationVenueuri schema:identifier ?publicationVenue.
                  ?authoruri schema:identifier ?author. 
                  OPTIONAL{?internalID cito:cites ?cited_pub. ?cited_pub schema:identifier ?cited.}  }'''
        df_final = get (self.endpointUrl, query, True)
        df_final = df_final.fillna('')
        #expected= ['doi:10.1016/j.websem.2021.100655','doi:10.1007/s10115-017-1100-y','doi:10.1016/j.websem.2014.03.003',
         #          'doi:10.1093/nar/gkz997', 'doi:10.3390/publications7030050','doi:10.1017/s0269888920000065',
         #          'doi:10.3390/info11030129','doi:10.1007/s00778-018-0528-3', ...]
        csv=pd.read_csv(self.graph_csv)
        expected=csv['id'].tolist()
        #self.assertTrue(set( df_final['id'].tolist()) == set(expected))
        self.assertTrue(set( df_final['id'].tolist()) == set(expected))
       

        

    

    def test_getPublicationsPublishedInYear(self):
        #self.uploadData()

        grp_qp = self.instantiateTriQP()
        df = grp_qp.getPublicationsPublishedInYear(2020)
        dois = list(df['id'])
        csv=pd.read_csv(self.graph_csv)
        filtered_df = csv[csv['publication_year'] == 2020]
        expected_dois = filtered_df['id'].tolist()
        self.assertTrue(set(dois) == set(expected_dois))

        
       

    def test_getPublicationsByAuthorId(self):
        #self.uploadData()

        grp_qp = self.instantiateTriQP()
        df = grp_qp.getPublicationsByAuthorId('0000-0002-9260-0753')
        dois = list(df['id'])
        expected_dois = ["doi:10.1016/j.websem.2021.100655","doi:10.3390/info11030129"]
        self.assertTrue(set(dois) == set(expected_dois))

      #MOST CITED: DA AGGIUSTARE ANCHE IN IMPL
    
  
    def test_getMostCitedPublication(self):
        #self.uploadData()
        grp_qp = self.instantiateTriQP()
        df = grp_qp.getMostCitedPublication()
        dois = str(df['id'])
        print(dois)
        #expected_dois = ""
        #self.assertEqual(dois,expected_dois)

       

    def test_getMostCitedVenue(self):
        #self.uploadData()
        grp_qp = self.instantiateTriQP()
        df = grp_qp.getMostCitedVenue()
        venues_id = str(df['VenueId'])
        print(venues_id)
        #expected_venues_id = ''
        #self.assertEqual(venues_id, expected_venues_id)

    

    

    def test_getVenuesByPublisherId(self):
        #self.uploadData()
        grp_qp = self.instantiateTriQP()
        df = grp_qp.getVenuesByPublisherId('crossref:78')
        venues_id = list(df['VenueId'])
        expected_venues_id = ["issn:1570-8268"]

        self.assertTrue(set(venues_id) == set(expected_venues_id))

       
    
    def test_getPublicationInVenue(self):
        #self.uploadData()
        grp_qp = self.instantiateTriQP()
        df = grp_qp.getPublicationInVenue("issn:1570-8268")
        dois = list(df['id'])
        expected_dois = ["doi:10.1016/j.websem.2021.100655",'doi:10.1016/j.websem.2014.03.003','doi:10.1016/j.websem.2014.06.002']
        self.assertEqual(set(dois), set(expected_dois))

       

    def test_getJournalArticlesInIssue(self):
        
        #self.uploadData()
        grp_qp = self.instantiateTriQP()
        df = grp_qp.getJournalArticlesInIssue(issue="1", volume="54", journalId="issn:0269-2821")
        dois = list(df['id'])
        expected_dois = ["doi:10.1007/s10462-020-09866-x"]
        self.assertEqual(set(dois), set(expected_dois))

        
    

    def test_getJournalArticlesInVolume(self):
        #self.uploadData()
        grp_qp = self.instantiateTriQP()
        df = grp_qp.getJournalArticlesInVolume(volume="53", journalId="issn:0269-2821")
        dois = list(df['id'])
        expected_dois = ['doi:10.1007/s10462-020-09826-5']
        self.assertTrue(set(dois)==set(expected_dois))

        


    def test_getJournalArticlesInJournal(self):
        #self.uploadData()
        grp_qp = self.instantiateTriQP()
        df = grp_qp.getJournalArticlesInJournal(journalId="issn:1570-8268")
        dois = list(df['id'])
        expected_dois = ["doi:10.1016/j.websem.2021.100655", "doi:10.1016/j.websem.2014.03.003", "doi:10.1016/j.websem.2014.06.002"]
        self.assertTrue(set(dois) == set(expected_dois))

       

    def test_getProceedingsByEvent(self):
        #self.uploadData()
        grp_qp = self.instantiateTriQP()
        df = grp_qp.getProceedingsByEvent('testevent')
        venue_ids = list(df['VenueId'])
        expected_venues_ids = ["isbn:9783030307929 isbn:9783030307936"]
        #print (venue_ids,expected_venues_ids)
        self.assertTrue(venue_ids == expected_venues_ids)

       
    def test_getPublicationAuthors(self):
        #self.uploadData()
        grp_qp = self.instantiateTriQP()
        df = grp_qp.getPublicationAuthors("doi:10.1016/j.websem.2014.03.003")
        authors_ids = df['PersonId']
        expected_authors_ids = ['0000-0003-0183-6910','0000-0002-5711-4872','0000-0003-0461-0028']
        self.assertTrue(set(authors_ids) == set(expected_authors_ids))

       
    
    def test_getPublicationsByAuthorName(self):
        #self.uploadData()
        grp_qp = self.instantiateTriQP()
        df = grp_qp.getPublicationsByAuthorName('ari')
        dois = df['id']
        expected_dois = ["doi:10.1007/s00778-018-0528-3", "doi:10.1016/j.websem.2021.100655",'doi:10.1007/978-3-030-33220-4_25', 'doi:10.3390/info11030129']
        self.assertTrue(set(dois) == set(expected_dois))

        
      
    def test_getDistinctPublisherOfPublications(self):
        #self.uploadData()
        grp_qp = self.instantiateTriQP()
        df = grp_qp.getDistinctPublisherOfPublications(["doi:10.1016/j.websem.2021.100655", "doi:10.1007/s10115-017-1100-y", "doi:10.1093/nar/gkz997"])
        crossref_ids = df['OrganizationId']
        expected_crossref_ids = ['crossref:78', 'crossref:297', 'crossref:286']
        self.assertTrue(crossref_ids.tolist() == expected_crossref_ids)

    
#run from terminal:
#cd + path to repo        
#python -m unittest discover -s test -p "graph_test.py"

