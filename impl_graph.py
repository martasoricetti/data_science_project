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
from sparql_dataframe import get
from pandas import concat
from graph_functions_and_tests import upload_csv_graph, upload_on_store, upload_json_authors, upload_json_publishers, upload_json_references, upload_json_venuedf, upload_json_graph

import SPARQLWrapper

from classes import *
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



base_url = "https://sparkle_db/"


JournalArticleUri = URIRef("http://purl.org/spar/fabio/JournalArticle")
BookChapterUri = URIRef("http://purl.org/spar/fabio/BookChapter")
ProceedingsPaperUri = URIRef("http://purl.org/spar/fabio/ProceedingsPaper") 

JournalUri = URIRef("http://purl.org/spar/fabio/Journal")
BookUri = URIRef("http://purl.org/spar/fabio/Book")
ProceedingsUri = URIRef("http://purl.org/spar/fabio/AcademicProceedings")


OrganizationUri = URIRef("https://schema.org/Organization")


PersonUri = URIRef("https://schema.org/Person")

#PROPERTIES:

hasIdentifier = URIRef("https://schema.org/identifier")                   
hasTitle = URIRef("http://purl.org/dc/terms/title")  


hasPublicationYear = URIRef("https://schema.org/datePublished")                 
hasCited = URIRef("http://purl.org/spar/cito/cites")                            
hasPublicationVenue = URIRef("https://schema.org/isPartOf")                     
hasAuthor = URIRef("https://schema.org/author")                                 


hasIssue = URIRef("https://schema.org/issueNumber")                             
hasVolume = URIRef("https://schema.org/volumeNumber")                           

hasChapterNumber = URIRef("http://purl.org/spar/fabio/hasSequenceIdentifier")   


hasPublisher = URIRef("https://schema.org/publisher")                           


hasEvent = URIRef("https://schema.org/description")                             


hasName = URIRef("https://schema.org/name")                                     


hasGivenName = URIRef("https://schema.org/givenName")                           
hasFamilyName = URIRef("https://schema.org/familyName")       




#UPLOAD FUNCTIONS

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
                #print('len after csv_graph: ')

            elif path.endswith(".json"): 
                #print(len(my_graph))
                venue_df=upload_json_venuedf(path)
                #------------venue substitutions---------------------------------------------------
                for i,row in venue_df.iterrows():
                    for el in row["DOIs"]:
                        for s,o in my_graph.subject_objects(hasPublicationVenue):
                            if str(s).endswith(el):
                                
                                store.add((s,hasPublicationVenue,(URIRef(base_url+row["Internal ID"])) ))
                                store.remove((s,hasPublicationVenue,o), None)
                                my_graph.remove((s,hasPublicationVenue,o))
                                #event
                                for event in my_graph.objects(o,hasEvent):
                                    store.add(((URIRef(base_url+row["Internal ID"])),hasEvent, event))
                                    store.remove((o, hasEvent, event), None)
                                    my_graph.remove((o, hasEvent, event))
                                #publisher
                                for o2 in my_graph.objects(o, hasPublisher):
                                    
                                    store.add((URIRef(base_url+row["Internal ID"]), hasPublisher,o2)) #publisher
                                    my_graph.remove((o, hasPublisher, o2))
                                    store.remove((o, hasPublisher, o2), None)
                                #type
                                for tipo in my_graph.objects(o, RDF.type ):
                                    
                                    #print('remove:',o, RDF.type, tipo, 'add:',(URIRef(base_url+row["Internal ID"]), RDF.type, tipo))
                                    store.add((URIRef(base_url+row["Internal ID"]), RDF.type, tipo)) #type
                                    my_graph.remove((o, RDF.type, tipo))
                                    store.remove((o, RDF.type, tipo),None)
                                #title
                                for title in my_graph.objects(o, hasTitle ):
                                    #print( o, 'venue:' base_url+row["Internal ID"], 'type', title )#title
                                    store.add((URIRef(base_url+row["Internal ID"]), hasTitle,title)) 
                                    my_graph.remove((o, hasTitle, title))
                                    store.remove((o, hasTitle, title), None)
                    
                   
                json_graph=upload_json_graph(venue_df, my_graph, store, endpoint)
                upload_json_authors(path, my_graph)
                upload_json_publishers(path, my_graph)
                upload_json_references(path,my_graph)
                
                

                for soggetto, oggetto in json_graph.subject_objects(hasPublicationVenue):
                    #for publisher in 
                    store.remove((soggetto, hasPublicationVenue, oggetto),None)
                    #my_graph.remove(soggetto, hasPublicationVenue, oggetto)
                    


                for triple in json_graph.triples((None, None, None)):
                   store.add(triple)
                for triple in my_graph.triples((None, None, None)):
                   store.add(triple)

                
                # Once finished, remeber to close the connection
                store.close() 
            #print('after method', len(my_graph))
            
            #dict for updating uris 
            
            doi_venue_dict = dict()
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
            
class TriplestoreQueryProcessor(TriplestoreProcessor):
    def __init__(self):
        super().__init__()
    
    def getPublicationsPublishedInYear(self, year: int):
    # La variabile "year" è definita come argomento della funzione, quindi è accessibile qui
        query= ["PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
                 "PREFIX schema:<https://schema.org/>",
                 "PREFIX cito:<http://purl.org/spar/cito/>", 
                 "prefix dcterms:<http://purl.org/dc/terms/>",  
                 "prefix fabio:<http://purl.org/spar/fabio/>", 
                 "SELECT ?publication ?id ?title ?publicationVenue" , 
                 "WHERE { ?publication schema:datePublished " ,str(year), ".", 
                 "?publication schema:identifier ?id.",
                 "?publication dcterms:title ?title.",                
                 "OPTIONAL{?publication schema:isPartOf ?publicationVenue.}",
                 
                 "}"]
        query_authors_and_cited=["""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                    PREFIX schema: <https://schema.org/>
                                    PREFIX cito:<http://purl.org/spar/cito/>
                                    prefix dcterms: <http://purl.org/dc/terms/> 
                                    prefix fabio: <http://purl.org/spar/fabio/> 
                                    SELECT ?publication (GROUP_CONCAT(DISTINCT ?author_single;separator=" ") AS ?author) (GROUP_CONCAT(DISTINCT ?cited_pub;separator=", ") AS ?cited)
                                    WHERE { ?publication schema:datePublished """ ,str(year), ".", 
                                        """?publication schema:author ?author_single .
                                            OPTIONAL{?publication cito:cites ?cited_pub.}
                                            OPTIONAL{?publication schema:isPartOf ?publicationVenue.}
                                        }
                                    GROUP BY ?publication 
                                    """]
        stringa = (" ".join(query))
        stringa2=(" ".join(query_authors_and_cited))
        df_sparql1 = get(self.endpointUrl, stringa, True)
        #df_sparql1=df_sparql1.drop_duplicates()
        df_sparql2=get(self.endpointUrl, stringa2, True)
        df_sparql=pd.merge(df_sparql1,df_sparql2, on="publication")
        df_sparql = df_sparql.fillna('')
        return df_sparql
    
    def getPublicationsByAuthorId(self, id: str):
        query=[ """PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                    PREFIX schema: <https://schema.org/>
                                    PREFIX cito:<http://purl.org/spar/cito/>
                                    prefix dcterms: <http://purl.org/dc/terms/> 
                                    prefix fabio: <http://purl.org/spar/fabio/> 
            SELECT  ?publication ?id ?title ?publicationVenue ?publication_year 
            WHERE{?publication schema:author ?author.
                  ?author schema:identifier """ ,str("'"+id+"'"), """.
                  ?publication schema:datePublished ?publication_year.         
                  ?publication schema:identifier ?id.
                  ?publication dcterms:title ?title.
                   OPTIONAL {?publication schema:isPartOf ?publicationVenue.}
               }                                                           """
               

        ]
        query_authors_and_cited=["""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                    PREFIX schema: <https://schema.org/>
                                    PREFIX cito:<http://purl.org/spar/cito/>
                                    prefix dcterms: <http://purl.org/dc/terms/> 
                                    prefix fabio: <http://purl.org/spar/fabio/> 
                                    SELECT ?publication (GROUP_CONCAT(DISTINCT ?author_single;separator=" ") AS ?author) (GROUP_CONCAT(DISTINCT ?cited_pub;separator=", ") AS ?cited)
                                    WHERE {?publication schema:author ?author.
                                            ?author schema:identifier """ ,str("'"+id+"'"), """.
                                            ?publication schema:author ?author_single .
                                            OPTIONAL{?publication cito:cites ?cited_pub.}
                                            OPTIONAL{?publication schema:isPartOf ?publicationVenue.}
                                        }
                                    GROUP BY ?publication 
                                 """

        ]
        stringa = (" ".join(query))
        stringa2=(" ".join(query_authors_and_cited))
        df_sparql1 = get(self.endpointUrl, stringa, True)
        #df_sparql1=df_sparql1.drop_duplicates()
        df_sparql2=get(self.endpointUrl, stringa2, True)
        df_sparql=pd.merge(df_sparql1,df_sparql2, on="publication")
        df_sparql = df_sparql.fillna('')
        return df_sparql
    
    def getMostCitedPublication(self):
        query="""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX schema: <https://schema.org/>
                PREFIX cito:<http://purl.org/spar/cito/>
                prefix dcterms: <http://purl.org/dc/terms/> 
                prefix fabio: <http://purl.org/spar/fabio/> 
                SELECT  ?publication  (COUNT(?p) as ?citing_publications )
            WHERE{?p cito:cites ?publication.
                  }
            GROUP BY ?publication
            ORDER BY DESC(?citing_publications)
            LIMIT 1
            
            """
        df_sparql=get(self.endpointUrl, query, True)
        #access most cited pub:
        most_cited=df_sparql['publication'][0]
        #retrieve pub data:
        query2=["""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX schema: <https://schema.org/>
                PREFIX cito:<http://purl.org/spar/cito/>
                prefix dcterms: <http://purl.org/dc/terms/> 
                prefix fabio: <http://purl.org/spar/fabio/> 
                SELECT ?id ?publicationVenue ?publication_year ?title
                WHERE {""", '<',str(most_cited),'> ', """schema:identifier ?id;
                                             dcterms:title ?title;
                                              schema:datePublished ?publication_year.""",
                    "OPTIONAL {",'<',str(most_cited),'> ', 'schema:isPartOf ?publicationVenue.}  }'
            ]
        stringa = ("".join(query2))
        df_sparql2=get(self.endpointUrl, stringa, True)

        query_authors_and_cited=["""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                    PREFIX schema: <https://schema.org/>
                                    PREFIX cito:<http://purl.org/spar/cito/>
                                    prefix dcterms: <http://purl.org/dc/terms/> 
                                    prefix fabio: <http://purl.org/spar/fabio/> 
                                    SELECT ?id (GROUP_CONCAT(DISTINCT ?author_single;separator=" ") AS ?author) (GROUP_CONCAT(DISTINCT ?cited_pub;separator=", ") AS ?cited)
                                    WHERE {""", '<',str(most_cited),'> ',""" schema:identifier ?id;
                                            
                                                         schema:author ?author_single .

                                            OPTIONAL{""", '<',str(most_cited),'> ',""" cito:cites ?cited_pub.}
                                            OPTIONAL{""", '<',str(most_cited),'> ',""" schema:isPartOf ?publicationVenue.}
                                        }
                                    GROUP BY ?id 
                                 """]
        stringa_au_cit = ("".join(query_authors_and_cited))
        df_sparql3=get(self.endpointUrl, stringa_au_cit, True)
        df_sparql=pd.merge(df_sparql2,df_sparql3, on='id')
        df_sparql = df_sparql.fillna('')

        return df_sparql
    
    def getMostCitedVenue(self):
        query="""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX schema: <https://schema.org/>
                PREFIX cito:<http://purl.org/spar/cito/>
                prefix dcterms: <http://purl.org/dc/terms/> 
                prefix fabio: <http://purl.org/spar/fabio/> 
                SELECT  ?publicationVenue  (COUNT(?p) as ?citing_publications )
            WHERE{?p cito:cites ?publication.
                  ?publication schema:isPartOf ?publicationVenue.
                  }
            GROUP BY ?publicationVenue
            ORDER BY DESC(?citing_publications)
           LIMIT 1
            
            """
        df_sparql=get(self.endpointUrl,query, True)
        most_cited=df_sparql['publicationVenue'][0]
        cited=df_sparql.iloc[0]['citing_publications']

        query2=["""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX schema: <https://schema.org/>
                PREFIX cito:<http://purl.org/spar/cito/>
                prefix dcterms: <http://purl.org/dc/terms/> 
                prefix fabio: <http://purl.org/spar/fabio/> 
                SELECT ?VenueId ?title ?publisher ?event
                WHERE {""", '<',str(most_cited),'> ', """schema:identifier ?VenueId;
                                             dcterms:title ?title;
                                              schema:publisher ?publisher.
                        OPTIONAL{""", '<',str(most_cited),'> ', """schema:description ?event.} }"""
                     
            ]
        stringa = ("".join(query2))
        df_sparql2=get(self.endpointUrl, stringa, True)
        #df_sparql_merge=pd.merge(df_sparql,df_sparql2, on='VenueId')
        df_sparql2 = df_sparql2.fillna('')
        df_sparql2['cited']=cited
        return df_sparql2
    
    def getVenuesByPublisherId(self, id: str):
        query=["""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX schema: <https://schema.org/>
                PREFIX cito:<http://purl.org/spar/cito/>
                prefix dcterms: <http://purl.org/dc/terms/> 
                prefix fabio: <http://purl.org/spar/fabio/> 
                SELECT ?VenueId ?title ?publisher
               WHERE {?venue schema:publisher ?publisher.
               ?publisher schema:identifier """, "'",str(id),"'", """.
               ?venue dcterms:title ?title.
               ?venue schema:identifier ?VenueId.
               OPTIONAL{?venue schema:description ?event.}              
               }
               """]
        stringa = ("".join(query))
        df_sparql=get(self.endpointUrl, stringa, True)
        df_sparql = df_sparql.fillna('')
        return df_sparql
    

    #filter per la stringa da cercare dentro l'id delle venues
    def getPublicationInVenue(self, venueId: str):
        query=["""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX schema: <https://schema.org/>
                PREFIX cito:<http://purl.org/spar/cito/>
                prefix dcterms: <http://purl.org/dc/terms/> 
                prefix fabio: <http://purl.org/spar/fabio/> 
                SELECT ?publication ?id ?title ?publicationVenue ?publication_year
               WHERE {?publication schema:isPartOf ?publicationVenue;
                          schema:datePublished ?publication_year.
                      ?publicationVenue schema:identifier ?VenueId.
                      ?publication dcterms:title ?title;
                                   schema:identifier ?id.
               FILTER CONTAINS(?VenueId,""","'",str(venueId),"'",""") }
                                   
                """]
        stringa = ("".join(query))
        df_sparql=get(self.endpointUrl, stringa, True)

        query_authors_and_cited=["""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                    PREFIX schema: <https://schema.org/>
                                    PREFIX cito:<http://purl.org/spar/cito/>
                                    prefix dcterms: <http://purl.org/dc/terms/> 
                                    prefix fabio: <http://purl.org/spar/fabio/> 
                                    SELECT ?publication (GROUP_CONCAT(DISTINCT ?author_single;separator=" ") AS ?author) (GROUP_CONCAT(DISTINCT ?cited_pub;separator=", ") AS ?cited)
                                    WHERE { ?publication schema:isPartOf ?publicationVenue.
                                        ?publicationVenue schema:identifier ?VenueId.
                                            
                                            ?publication schema:author ?author_single .
                                            OPTIONAL{?publication cito:cites ?cited_pub.}
                                           
                                      FILTER CONTAINS(?VenueId,""","'",str(venueId),"'",""")  }
                                  
                                    GROUP BY ?publication 
                                 """]
        stringa_au_cit = ("".join(query_authors_and_cited))
        df_sparql2=get(self.endpointUrl, stringa_au_cit, True)
        df_sparql=pd.merge(df_sparql,df_sparql2, on='publication')
        df_sparql = df_sparql.fillna('')

        #filter per la stringa da cercare dentro l'id delle venues

        return df_sparql
    
    def getJournalArticlesInIssue(self, issue: str, volume: str, journalId: str):
        query=["""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX schema: <https://schema.org/>
                PREFIX cito:<http://purl.org/spar/cito/>
                prefix dcterms: <http://purl.org/dc/terms/> 
                prefix fabio: <http://purl.org/spar/fabio/> 
                SELECT ?publication ?id ?title ?publicationVenue  ?publication_year
               WHERE {?publication schema:isPartOf ?publicationVenue;
                                  schema:datePublished ?publication_year.
                      ?publicationVenue schema:identifier ?VenueId.
                      ?publication dcterms:title ?title;
                                    schema:issueNumber ""","'",str(issue),"'",""";
                                    schema:volumeNumber""","'",str(volume),"'",""";
                                   schema:identifier ?id.
               FILTER CONTAINS(?VenueId,""","'",str(journalId),"'",""") }
                                   
                """]
        stringa = ("".join(query))
        df_sparql=get(self.endpointUrl, stringa, True)

        query_authors_and_cited=["""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                    PREFIX schema: <https://schema.org/>
                                    PREFIX cito:<http://purl.org/spar/cito/>
                                    prefix dcterms: <http://purl.org/dc/terms/> 
                                    prefix fabio: <http://purl.org/spar/fabio/> 
                                    SELECT ?publication (GROUP_CONCAT(DISTINCT ?author_single;separator=" ") AS ?author) (GROUP_CONCAT(DISTINCT ?cited_pub;separator=", ") AS ?cited)
                                    WHERE { ?publication schema:isPartOf ?publicationVenue.
                                        ?publicationVenue schema:identifier ?VenueId.
                                            
                                            ?publication schema:author ?author_single ;
                                 schema:issueNumber ""","'",str(issue),"'",""";
                                    schema:volumeNumber""","'",str(volume),"'",""";
                                            OPTIONAL{?publication cito:cites ?cited_pub.}
                                           
                                      FILTER CONTAINS(?VenueId,""","'",str(journalId),"'",""")  }
                                  
                                    GROUP BY ?publication 
                                 """]
        stringa_au_cit = ("".join(query_authors_and_cited))
        df_sparql2=get(self.endpointUrl, stringa_au_cit, True)
        df_sparql=pd.merge(df_sparql,df_sparql2, on='publication')
        df_sparql = df_sparql.fillna('')
        return df_sparql
    
    def getJournalArticlesInVolume(self, volume: str, journalId: str):
        query=["""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX schema: <https://schema.org/>
                PREFIX cito:<http://purl.org/spar/cito/>
                prefix dcterms: <http://purl.org/dc/terms/> 
                prefix fabio: <http://purl.org/spar/fabio/> 
                SELECT ?publication ?id ?title ?publicationVenue ?issue ?publication_year
               WHERE {?publication schema:isPartOf ?publicationVenue;
                        schema:datePublished ?publication_year.
                      ?publicationVenue schema:identifier ?VenueId.
                      ?publication dcterms:title ?title;
                                    
                                    schema:volumeNumber ""","'",str(volume),"'",""";
                                   schema:identifier ?id.
                    OPTIONAL{?publication schema:issueNumber ?issue.}
               FILTER CONTAINS(?VenueId,""","'",str(journalId),"'",""") }
                                   
                """]
        stringa = ("".join(query))
        df_sparql=get(self.endpointUrl, stringa, True)

        query_authors_and_cited=["""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                    PREFIX schema: <https://schema.org/>
                                    PREFIX cito:<http://purl.org/spar/cito/>
                                    prefix dcterms: <http://purl.org/dc/terms/> 
                                    prefix fabio: <http://purl.org/spar/fabio/> 
                                    SELECT ?publication (GROUP_CONCAT(DISTINCT ?author_single;separator=" ") AS ?author) (GROUP_CONCAT(DISTINCT ?cited_pub;separator=", ") AS ?cited)
                                    WHERE { ?publication schema:isPartOf ?publicationVenue.
                                        ?publicationVenue schema:identifier ?VenueId.
                                            
                                            ?publication schema:author ?author_single ;
                                 
                                    schema:volumeNumber ""","'",str(volume),"'",""";
                                            OPTIONAL{?publication cito:cites ?cited_pub.}
                                            OPTIONAL{?publication schema:issueNumber ?issue.}
                                           
                                      FILTER CONTAINS(?VenueId,""","'",str(journalId),"'",""")  }
                                  
                                    GROUP BY ?publication 
                                 """]
        stringa_au_cit = ("".join(query_authors_and_cited))
        df_sparql2=get(self.endpointUrl, stringa_au_cit, True)
        df_sparql=pd.merge(df_sparql,df_sparql2, on='publication')
        df_sparql = df_sparql.fillna('')
        return df_sparql
    
    def getJournalArticlesInJournal(self, journalId: str):
        query=["""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX schema: <https://schema.org/>
                PREFIX cito:<http://purl.org/spar/cito/>
                prefix dcterms: <http://purl.org/dc/terms/> 
                prefix fabio: <http://purl.org/spar/fabio/> 
                SELECT ?publication ?id ?title ?publicationVenue ?issue ?volume ?publication_year
               WHERE {?publication schema:isPartOf ?publicationVenue;
               schema:datePublished ?publication_year.
                       ?publicationVenue rdf:type fabio:Journal.
                      ?publicationVenue schema:identifier ?VenueId.
                      ?publication dcterms:title ?title;
                                   schema:identifier ?id.
                     OPTIONAL {?publication schema:issueNumber ?issue.}
                     OPTIONAL {?publication schema:volumeNumber ?volume.}
               FILTER CONTAINS(?VenueId,""","'",str(journalId),"'",""") }
                                   
                """]
        stringa = ("".join(query))
        df_sparql=get(self.endpointUrl, stringa, True)

        query_authors_and_cited=["""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                    PREFIX schema: <https://schema.org/>
                                    PREFIX cito:<http://purl.org/spar/cito/>
                                    prefix dcterms: <http://purl.org/dc/terms/> 
                                    prefix fabio: <http://purl.org/spar/fabio/> 
                                    SELECT ?publication (GROUP_CONCAT(DISTINCT ?author_single;separator=" ") AS ?author) (GROUP_CONCAT(DISTINCT ?cited_pub;separator=", ") AS ?cited)
                                    WHERE { ?publication schema:isPartOf ?publicationVenue.
                                        ?publicationVenue schema:identifier ?VenueId.
                                        ?publicationVenue rdf:type fabio:Journal.
                                            
                                            ?publication schema:author ?author_single .
                                            OPTIONAL{?publication cito:cites ?cited_pub.}
                                           
                                      FILTER CONTAINS(?VenueId,""","'",str(journalId),"'",""")  }
                                  
                                    GROUP BY ?publication 
                                 """]
        stringa_au_cit = ("".join(query_authors_and_cited))
        df_sparql2=get(self.endpointUrl, stringa_au_cit, True)
        df_sparql=pd.merge(df_sparql,df_sparql2, on='publication')
        df_sparql = df_sparql.fillna('')
        #filter per la stringa da cercare dentro l'id delle venues

        return df_sparql
     
    def getProceedingsByEvent(self, eventPartialName: str):
            eventPartialName = eventPartialName.lower()
            query=["""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX schema: <https://schema.org/>
                    PREFIX cito:<http://purl.org/spar/cito/>
                    prefix dcterms: <http://purl.org/dc/terms/> 
                    prefix fabio: <http://purl.org/spar/fabio/> 
                    SELECT ?VenueId ?title ?publisher ?event
                    WHERE {?venue schema:description ?event.
                    FILTER CONTAINS(?VenueId,""","'",str(eventPartialName),"'",""")} 
                    """]

            stringa = ("".join(query))
            df_sparql=get(self.endpointUrl, stringa, True)
            df_sparql = df_sparql.fillna('')
            return df_sparql

    def getPublicationAuthors(self, publicationId: str):
        query=["""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX schema: <https://schema.org/>
                    PREFIX cito:<http://purl.org/spar/cito/>
                    prefix dcterms: <http://purl.org/dc/terms/> 
                    prefix fabio: <http://purl.org/spar/fabio/> 
                    SELECT ?PersonId ?givenName ?familyName
                    WHERE {
                    ?publication schema:identifier """ ,str("'"+publicationId+"'"), """;
                                schema:author ?author.
                        ?author schema:givenName ?givenName;
                                schema:familyName ?familyName;
                                schema:identifier ?PersonId.
                    }
            """]
        stringa = ("".join(query))
        df_sparql=get(self.endpointUrl, stringa, True)
        df_sparql = df_sparql.fillna('')
        return df_sparql

    def getPublicationsByAuthorName(self, authorPartialName: str):
        authorPartialName = authorPartialName.lower()
        query=["""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX schema: <https://schema.org/>
                    PREFIX cito:<http://purl.org/spar/cito/>
                    prefix dcterms: <http://purl.org/dc/terms/> 
                    prefix fabio: <http://purl.org/spar/fabio/> 
                    SELECT ?publication ?id ?title ?publicationVenue ?publication_year 
                    WHERE {
                      {?publication schema:author ?author;
                               schema:identifier ?id;
                               dcterms:title ?title;
                               schema:datePublished  ?publication_year;
                               schema:isPartOf ?publicationVenue.
                     ?author schema:givenName ?givenName;
                             schema:familyName ?familyName.
                                                       
                              FILTER CONTAINS( LCASE(?givenName), """, str("'"+authorPartialName+"'"), """) }
                      UNION {?publication schema:author ?author;
                                         schema:identifier ?id;
                               dcterms:title ?title;
                               schema:datePublished  ?publication_year;
                               schema:isPartOf ?publicationVenue.
                     ?author schema:givenName ?givenName;
                             schema:familyName ?familyName.
                                                       
                              FILTER CONTAINS( LCASE(?familyName), """, str("'"+authorPartialName+"'"), """ ) }
                      
                        }  """ ]
        stringa = ("".join(query))
        df_sparql=get(self.endpointUrl, stringa, True)
        query_authors_and_cited=["""PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                    PREFIX schema: <https://schema.org/>
                                    PREFIX cito:<http://purl.org/spar/cito/>
                                    prefix dcterms: <http://purl.org/dc/terms/> 
                                    prefix fabio: <http://purl.org/spar/fabio/> 
                                    SELECT ?publication (GROUP_CONCAT(DISTINCT ?author_single;separator=" ") AS ?author) (GROUP_CONCAT(DISTINCT ?cited_pub;separator=", ") AS ?cited)
                                   WHERE {
                      {?publication schema:author ?author_single.
                     ?author_single schema:givenName ?givenName;
                             schema:familyName ?familyName.
                                 
                                 
                                            OPTIONAL{?publication cito:cites ?cited_pub.}
                                                       
                              FILTER CONTAINS( LCASE(?givenName), """, str("'"+authorPartialName+"'"), """) }
                      UNION {?publication schema:author ?author_single.
                     ?author_single schema:givenName ?givenName;
                             schema:familyName ?familyName.
                                 OPTIONAL{?publication cito:cites ?cited_pub.}
                                                       
                              FILTER CONTAINS( LCASE(?familyName), """, str("'"+authorPartialName+"'"), """) }
                      
                      } 
                      GROUP BY ?publication """]
        stringa_au_cit = ("".join(query_authors_and_cited))
        df_sparql2=get(self.endpointUrl, stringa_au_cit, True)
        df_sparql=pd.merge(df_sparql,df_sparql2, on='publication')
        df_sparql=df_sparql.drop_duplicates() 
        #drop duplicates per non ripetere le pubblicazioni che hanno più di un autore che fa match con la stringa
        df_sparql = df_sparql.fillna('')
        return df_sparql
    
    def getDistinctPublisherOfPublications(self, pubIdList:list):
        df=pd.DataFrame()
        for inputdoi in pubIdList:
            
            query=['''
        
                    PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX schema: <https://schema.org/>

                    SELECT  ?OrganizationId ?name  
                    
                    WHERE {
                    ?publication schema:identifier ''', str("'"+inputdoi+"'"),";",'''
                               
                                schema:isPartOf  ?publicationVenue.
                    ?publicationVenue  schema:publisher  ?publisher.
                    ?publisher schema:name     ?name;
                                schema:identifier ?OrganizationId.

                    }       
                            ''']
            stringa=(" ".join(query))
            df_final = get (self.endpointUrl, stringa,  True)
            df_final = df_final.fillna('')
            df = concat([df, df_final], ignore_index=True)
        
        return df
    
    
    def getOrganization(self, crossref_id):
        
        query= f'PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX schema: <https://schema.org/> PREFIX cito:<http://purl.org/spar/cito/> prefix dcterms: <http://purl.org/dc/terms/> prefix fabio: <http://purl.org/spar/fabio/> SELECT  ?name WHERE{{ ?internalID schema:name ?name. ?internalID  schema:identifier "{crossref_id}".}}'
        df_final = get (self.endpointUrl, query, True)
        df_final = df_final.fillna('')
        for row_idx, row in df_final.iterrows():
            organization_obj = Organization(identifier=crossref_id, name=row["name"])

            
            return organization_obj
        
    def getVenue(self, venue_id):
        #VALUES ?type {{fabio:Book fabio:Journal schema:AcademicProceedings}} ?internalID rdf:type ?type.
        query_1 = f'PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX schema: <https://schema.org/> PREFIX cito:<http://purl.org/spar/cito/> prefix dcterms: <http://purl.org/dc/terms/>  prefix fabio: <http://purl.org/spar/fabio/> SELECT ?internalID ?VenueId ?title ?publisher WHERE{{ ?internalID schema:identifier ?VenueId;   dcterms:title ?title; schema:publisher ?publisheruri. ?publisheruri schema:identifier ?publisher. FILTER CONTAINS(?VenueId, "{venue_id}") }}'
        df_final_venues = get (self.endpointUrl, query_1, True)
        df_final_venues = df_final_venues.fillna('')
        for row_idx,row in df_final_venues.iterrows():
            if "publisher" in row:
                publisher = self.getOrganization(row["publisher"])
            venue_obj=Venue(identifiers=row["VenueId"], title= row["title"], publisher= publisher)
            return venue_obj
        

    def getAuthor(self, orcid):
            query=f'PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>  PREFIX schema: <https://schema.org/>  PREFIX cito:<http://purl.org/spar/cito/> prefix dcterms: <http://purl.org/dc/terms/>  prefix fabio: <http://purl.org/spar/fabio/> SELECT ?internalID ?givenName ?familyName WHERE{{ ?internalID schema:givenName ?givenName. ?internalID schema:familyName ?familyName. ?internalID  schema:identifier "{orcid}".}}'
                 
            df_final = get (self.endpointUrl, query, True)
            #if df_final:
            df_final = df_final.fillna('')
            for row_idx, row in df_final.iterrows():
                person_obj = Person(identifier=orcid, givenName=row["givenName"], familyName=row["familyName"])
              
                return person_obj
        
    
    def getPublication(self, id):
        
            query = f'PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX schema: <https://schema.org/> PREFIX cito:<http://purl.org/spar/cito/> prefix dcterms: <http://purl.org/dc/terms/> prefix fabio: <http://purl.org/spar/fabio/>  SELECT ?internalID ?id ?title ?publicationVenue ?publication_year ?author ?cited WHERE{{ ?internalID schema:identifier "{id}";   schema:author ?authoruri;  dcterms:title ?title; schema:datePublished  ?publication_year;  schema:isPartOf ?publicationVenueuri. ?publicationVenueuri schema:identifier ?publicationVenue. ?authoruri schema:identifier ?author. OPTIONAL{{?internalID cito:cites ?cited_pub. ?cited_pub schema:identifier ?cited.}}  }}'
            df_final = get (self.endpointUrl, query, True)
            df_final = df_final.fillna('')
            pub_authors = []
            pub_cited=[]
            venue_id=df_final.iloc[0]["publicationVenue"].split(" ")[0]
            pubyear=df_final.iloc[0]["publication_year"]
            title=df_final.iloc[0]["title"]
            if venue_id:
                    pub_publicationVenue = self.getVenue(venue_id)
            else:
                    pub_publicationVenue = ''

            for row_idx, row in df_final.iterrows():
                author = row["author"]
                if author:
                    pub_authors.append(self.getAuthor(author))
                #else:
                    #pub_authors = ''
                cited= row['cited']
                if cited:
                    pub_cited.append(self.getPublication(cited))
                
            if len(pub_authors)== 0:
                  pub_authors = '{}'.format(pub_authors)
            if len(pub_cited)== 0:
                  pub_cited = '{}'.format(pub_cited)

            publication_obj = Publication(identifier=id, publicationYear=pubyear, title=title, author=pub_authors, publicationVenue=pub_publicationVenue, citedPublications=pub_cited)

            
            return publication_obj

            
  
   