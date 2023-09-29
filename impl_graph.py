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

base_url = "https://github.com/martasoricetti/data_science_project/"

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

hasIdentifier = URIRef("https://schema.org/identifier")                   
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

        from graph_functions_and_tests import my_graph
        if os.path.exists(path):
           
            
            store = SPARQLUpdateStore()

                # The URL of the SPARQL endpoint is the same URL of the Blazegraph
                # instance + '/sparql'
            endpoint = self.endpointUrl
                # It opens the connection with the SPARQL endpoint instance
            
            store.open((endpoint, endpoint))
            
            
            # funzione per upload sullo store chiamata dopo aver processato i files?            
                    
            if path.endswith(".csv"):               
                #funzione per upload csv

                csv_graph=upload_csv_graph(path, my_graph)
                
                for triple in csv_graph.triples((None, None, None)):
                   store.add(triple)
                
                #print(len(my_graph))
                my_graph+=csv_graph
                print('len after csv_graph: ')

            elif path.endswith(".json"): 
                print(len(my_graph))
                venue_df=upload_json_venuedf(path)
                for i,row in venue_df.iterrows():
                    for el in row["DOIs"]:
                        for s,o in my_graph.subject_objects(hasPublicationVenue):
                            if str(s).endswith(el):
                                #print(2)
                                store.add((s,hasPublicationVenue,(URIRef(base_url+row["Internal ID"])) ))
                                store.remove((s,hasPublicationVenue,o), None)
                                my_graph.remove((s,hasPublicationVenue,o))
                                for o2 in my_graph.objects(o, hasPublisher):
                                    #print(3)
                                    store.add((URIRef(base_url+row["Internal ID"]), hasPublisher,o2)) #publisher
                                    my_graph.remove((o, hasPublisher, o2))
                                    store.remove((o, hasPublisher, o2), None)

                                for tipo in my_graph.objects(o, RDF.type ):
                                    #print('remove:',o, RDF.type, tipo, 'add:',(URIRef(base_url+row["Internal ID"]), RDF.type, tipo))
                                    store.add((URIRef(base_url+row["Internal ID"]), RDF.type, tipo)) #type
                                    my_graph.remove((o, RDF.type, tipo))
                                    store.remove((o, RDF.type, tipo),None)

                                for title in my_graph.objects(o, hasTitle ):
                                    #print( o, 'venue:' base_url+row["Internal ID"], 'type', title )#title
                                    store.add((URIRef(base_url+row["Internal ID"]), RDF.type,title)) 
                                    my_graph.remove((o, hasTitle, title))
                                    store.remove((o, hasTitle, title), None)
                    
                   
                json_graph=upload_json_graph(venue_df, my_graph, store, endpoint)
                upload_json_authors(path, my_graph)
                upload_json_publishers(path, my_graph)
                upload_json_references(path,my_graph)
                '''
                for idx, row in venue_df.iterrows():
                    for el in row["DOIs"]:
                        new_object = base_url + row["Internal ID"]

                        for soggetto, oggetto_literal in upload_json_graph(venue_df, my_graph, store, endpoint)[1].subject_objects(hasPublicationVenue):

                            if oggetto_literal.strip() == "venue-" + str(el).strip():
                                print(1) '''
                

                for soggetto, oggetto in json_graph.subject_objects(hasPublicationVenue):
                    #for publisher in 
                    store.remove((soggetto, hasPublicationVenue, oggetto),None)
                    #my_graph.remove(soggetto, hasPublicationVenue, oggetto)
                    '''
                    store.remove((oggetto, hasPublisher, None), None)
                    store.remove((oggetto, RDF.type, None), None)
                    store.remove((oggetto, hasTitle, None), None)
                for i,r in venue_df.iterrows():
                    for doi in r['DOIs']:
                        '''


                for triple in json_graph.triples((None, None, None)):
                   store.add(triple)
                for triple in my_graph.triples((None, None, None)):
                   store.add(triple)

                
                # Once finished, remeber to close the connection
                store.close() 
            print('after method', len(my_graph))
            
            #dict for updating uris 
            doi_venue_dict=dict()
            for s,o in my_graph.subject_objects(hasPublicationVenue):
                    #print (s,o)
                    #if o not in doi_venue_dict.keys(): - 
                    if s not in doi_venue_dict.keys():
                        doi_venue_dict[s]=[]
                        doi_venue_dict[s].append(o)
                    else:
                        doi_venue_dict[s].append(o)
                    
                    
            #print(venue_dict)

            #for s,o in my_graph.subject_objects(hasPublisher):
                #print(1)
            
            """  #print(my_graph)
                for s, o in my_graph.subject_objects(hasPublicationVenue):
                            print(s,o)     
                   
                for idx, row in venue_df.iterrows():
                    uri_venue = base_url + row["Internal ID"]
                    #print(uri_venue) 
                    my_graph.add((URIRef(uri_venue), hasIdentifier, Literal(row["Venue IDs"]) ) )
                
                    for el in row["DOIs"]:                        
                        new_object = base_url + row["Internal ID"] 
                        #print(base_url+"venue-" + str(el))
                      
                        
                        
                        for soggetto, oggetto_literal in my_graph.subject_objects(hasPublicationVenue):
                                print(oggetto_literal)
                                if oggetto_literal ==  base_url+"venue-" + str(el):
                                    #print(soggetto, hasPublicationVenue, oggetto_literal)
                                    print(5)
                                    my_graph.remove((soggetto, hasPublicationVenue, oggetto_literal))
                                    store.remove(soggetto, hasPublicationVenue, oggetto_literal)
                                    my_graph.add((soggetto, hasPublicationVenue, URIRef(new_object)))
                                    #print(soggetto, hasPublicationVenue, URIRef(new_object))
                        for soggetto, oggetto in my_graph.subject_objects(RDF.type):       
                        #stessa cosa per publisher e type 
                                if soggetto == base_url+"venue-" + str(el):
                                    print(2)
                                    my_graph.remove((soggetto, RDF.type, oggetto))
                                    store.remove(soggetto, RDF.type, oggetto)
                                    my_graph.add((URIRef(new_object), RDF.type, oggetto ))      """
                
            #elif  path.endswith(".csv"):
            #print(my_graph,len(my_graph))
             
                   #print(triple)
                    
                   # triple che prendono da csv e json? forse basta creare i triples dal json e dal csv con gli stessi internal id
        #print('len:',len(my_graph))

'''
            elif path.endswith(".json"):
                #funzione per upload json
                #venue_df=upload_json_venuedf(path)
                #upload_json_graph(venue_df, my_graph)
                upload_json_authors(path, my_graph)
                upload_json_publishers(path, my_graph)
                upload_json_references(path,my_graph)
                #upload_on_store(store,my_graph, self.endpointUrl)'''  
        
