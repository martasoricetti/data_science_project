import os.path
import pandas as pd
import json
import numpy as np
#rdflib 
from rdflib import Graph
from rdflib import URIRef
from rdflib import RDF
from rdflib import Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

from graph_functions_and_tests import upload_csv_graph, upload_on_store, upload_json_authors, upload_json_publishers, upload_json_references, upload_json_venuedf, upload_json_graph

#Publication
#Person
#IdentifiableEntity
#Venue
#Organization
#JournalArticle
#BookChapter
#ProceedingsPaper
#Journal
#Book
#Proceedings 

#---------------URIs---------------

base_url = "https://github.com/martasoricetti/data_science_project"

#--------Publications---:
JournalArticleUri = URIRef("http://purl.org/spar/fabio/JournalArticle")
BookChapterUri = URIRef("http://purl.org/spar/fabio/BookChapter")
ProceedingsPaperUri = URIRef("http://purl.org/spar/fabio/ProceedingsPaper") 

#--------Venues---------:
JournalUri = URIRef("http://purl.org/spar/fabio/Journal")
BookUri = URIRef("http://purl.org/spar/fabio/Book")
ProceedingsUri = URIRef("http://purl.org/spar/fabio/AcademicProceedings")

#---------Publishers----------:
OrganizationUri = URIRef("https://schema.org/Organization")

#---------Authors-------------:
PersonUri = URIRef("https://schema.org/Person")

#-------------PROPERTIES---------------:

hasIdentifier = URIRef("http://purl.org/dc/terms/identifier")                   
hasTitle = URIRef("http://purl.org/dc/terms/title")  

#------Publication + sub-classes---:
hasPublicationYear = URIRef("https://schema.org/datePublished")                 
hasCited = URIRef("http://purl.org/spar/cito/cites")                            
hasPublicationVenue = URIRef("https://schema.org/isPartOf")                     
hasAuthor = URIRef("https://schema.org/author")                                 

#-------------JournalArticle--------------:
hasIssue = URIRef("https://schema.org/issueNumber")                             
hasVolume = URIRef("https://schema.org/volumeNumber")                           

#-------------BookChapter------------------:
hasChapterNumber = URIRef("http://purl.org/spar/fabio/hasSequenceIdentifier")   

#-------------Venue and its sub-classes----:
hasPublisher = URIRef("https://schema.org/publisher")                           

#-------------Proceedings------------------:
hasEvent = URIRef("https://schema.org/description")                             

#-------------Organization-----------------:
hasName = URIRef("https://schema.org/name")                                     

#-------------Person-----------------------:
hasGivenName = URIRef("https://schema.org/givenName")                           
hasFamilyName = URIRef("https://schema.org/familyName")       


#funzioni per le venues:
#aprire csv, estrarre le venues , creare un internal id per le venues e unirlo al dataframe .
#poi usare l'internal id per creare 
#gli uri e metterli in un dizionario
#poi col json creare un altro dataframe che ha gli id delle venues collegati ad ogni doi.
#funzione per unire i dataframe, ha in input le due funzioni precedenti
#unire il dataframe del json a quello del csv con gli internal id delle venues tramite le doi
#su questo dataframe si può iterare per creare i triples con attributi e relazioni delle venues
# NO ASP, FORSE QUESTO DISCORSO VA FATTO CON LE PUBLICATIONS? 
#visto che il loro internal id è per ora l'idx
#sennò devo usare la doi
#vabbè uso la doi

          

#----------UPLOAD FUNCTIONS-----------

#separare le venues in una funzione a parte per controllare il numero di triple? 
# print("-- Number of triples added to the graph after processing venues and publications")
#print(len(my_graph))





#def upload_json_graph():
#def test_upload_json_graph():

#Triplestore



class TriplestoreProcessor:
    def __init__(self, endpointUrl:str = ''):
        self.endpointUrl = endpointUrl

    def getEndpointUrl(self):
        if self.endpointUrl:
            return self.endpointUrl

    def setEndpointUrl(self, url: str):
        if type(url)==str:
            self.endpointUrl = url
            return True
        else:
            return False
        


class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self, endpointUrl: str = ''):
        super(TriplestoreDataProcessor, self).__init__(endpointUrl)
    def uploadData(self, path: str):
        if os.path.exists(path):
            store = SPARQLUpdateStore()
            my_graph = Graph()
            # funzione per upload sullo store chiamata dopo aver processato i files?            
            '''
            store = SPARQLUpdateStore()
            store.open((self.endpointUrl, self.endpointUrl))
            '''
                    
            if path.endswith(".csv"):               
                #funzione per upload csv
                upload_csv_graph(path, my_graph)

            elif path.endswith(".json"):
                #funzione per upload json
                venue_df=upload_json_venuedf(path)
                upload_json_graph(venue_df, my_graph)
                upload_json_authors(path, my_graph)
                upload_json_publishers(path, my_graph)
                upload_json_references(path,my_graph)
                #upload_on_store(store,my_graph, self.endpointUrl)
                
                store = SPARQLUpdateStore()

                # The URL of the SPARQL endpoint is the same URL of the Blazegraph
                # instance + '/sparql'
                endpoint = self.endpointUrl

                # It opens the connection with the SPARQL endpoint instance
                store.open((endpoint, endpoint))

                for triple in my_graph.triples((None, None, None)):
                   store.add(triple)
                    
                # Once finished, remeber to close the connection
                store.close() 
            # triple che prendono da csv e json? forse basta creare i triples dal json e dal csv con gli stessi internal id
        print('len:',len(my_graph))
        
