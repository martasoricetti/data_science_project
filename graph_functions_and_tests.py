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

#CARICARE LE ENTITà VENUES E I LORO ATTRIBUTI E RELAZIONI CON DEI LITERAL PROVVISORI FORMATI DALLE DOI, POI 
# SOSTITUIRE I LITERAL CON DEGLI URI CHE IDENTIFICANO LE VENUES TRAMITE INTERNAL ID
# Sostituisci il nodo Literal con un nodo URI
#nuovo_oggetto_uri = URIRef("http://example.org/alice")
#graph.remove((soggetto, predicato, oggetto_literal))  # Rimuovi la vecchia tripla
#graph.add((soggetto, predicato, nuovo_oggetto_uri))  # Aggiungi la nuova tripla
'''
# Creazione di un oggetto Graph
graph = Graph()


#Sostituisci i nodi Literal con nodi URI
nuovo_oggetto_uri1 = URIRef("http://example.org/alice")
nuovo_oggetto_uri2 = URIRef("http://example.org/bob")

for soggetto, oggetto_literal in graph.subject_objects(predicato):
    if oggetto_literal == Literal("Alice"):
        graph.remove((soggetto, predicato, oggetto_literal))
        graph.add((soggetto, predicato, nuovo_oggetto_uri1))
    elif oggetto_literal == Literal("Bob"):
        graph.remove((soggetto, predicato, oggetto_literal))
        graph.add((soggetto, predicato, nuovo_oggetto_uri2))
'''


#---------------URIs---------------
#--------------baseurl-------------
base_url = "https://github.com/martasoricetti/data_science_project/"

#--------Sub-classes of Publications---:
JournalArticleUri = URIRef("http://purl.org/spar/fabio/JournalArticle")
BookChapterUri = URIRef("http://purl.org/spar/fabio/BookChapter")
ProceedingsPaperUri = URIRef("http://purl.org/spar/fabio/ProceedingsPaper") 

#--------Sub-classes of Venues---------:
JournalUri = URIRef("http://purl.org/spar/fabio/Journal")
BookUri = URIRef("http://purl.org/spar/fabio/Book")
ProceedingsUri = URIRef("http://purl.org/spar/fabio/AcademicProceedings")

#---------Class of publishers----------:
OrganizationUri = URIRef("https://schema.org/Organization")

#---------Class of authors-------------:
PersonUri = URIRef("https://schema.org/Person")

#-------------PROPERTIES---------------:

#-------------General------------------:
hasIdentifier = URIRef("https://schema.org/identifier")                   
hasTitle = URIRef("http://purl.org/dc/terms/title")  

#------Publication and its sub-classes---:
hasPublicationYear = URIRef("https://schema.org/datePublished")                 
hasCitedPublication = URIRef("http://purl.org/spar/cito/cites")                            
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


#upload csv

#separare le venues in una funzione a parte per controllare il numero di triple? 
# print("-- Number of triples added to the graph after processing venues and publications")
#print(len(my_graph))

#caricare prima le venues, eventualmente con internal id, e poi le publications
#eventualmente col metodo del prof, con i dizionari con gli internal id
#solo che lì parte dai singoli dataframe
#potrei usare quelli creati per il relational

#forse la relazione publicationvenue va aggiunta quando si carica il json 
#creando un dizionario invertito con gli id delle venues come chiavi e le doi come venues
#da lì un dataframe da cui ricavare un internal id per le venues da usare negli uri
# e da questo dataframe la relazione publication venue

my_graph=Graph()

def upload_csv_graph(csvpath, graph):
    df_publication = pd.read_csv(csvpath, keep_default_na=False,
                                             dtype={
                                                 "id": "string",
                                                 "title": "string",
                                                 "type": "string",
                                                 "publication_year": "Int64",
                                                 "issue": "string",
                                                 "volume": "string",
                                                 "chapter": "Int64",
                                                 "publication_venue": "string",
                                                 "venue_type": "string",
                                                 "publisher": "string",
                                                 "event": "string"
                                             })
    for idx, row in df_publication.iterrows():

        publication_single_id = "publication-" + str(row["id"])
        publication_single_uri = base_url + publication_single_id
        
    
        graph.add((URIRef(publication_single_uri), hasTitle, Literal(row["title"])))
        graph.add((URIRef(publication_single_uri), hasIdentifier, Literal(row["id"])))

        if row["publication_year"]:
            graph.add((URIRef(publication_single_uri), hasPublicationYear, Literal(row["publication_year"])))

        if row["publication_venue"]:
            #se ogni publication ha 0 oppure 1 venue, nel caso ci sia la venue posso usare la doi dell pub 
            # per creare l'uri della venue

         #Venues
                
                #venue_single_id = "venue-" + str(row["id"])
                #venue_single_uri = base_url + venue_single_id
                graph.add((URIRef(publication_single_uri), hasPublicationVenue, URIRef(base_url+"venue-" + str(row["id"]) ) )) #URIRef(base_url+"venue-" + str(row["id"])
                graph.add((URIRef(base_url+"venue-" + str(row["id"])), hasTitle, Literal(row["publication_venue"]) )) #URIRef(base_url+"venue-" + str(row["id"])
       
        #hasPublisher
                if row["publisher"]:
                  publisher_single_id = "publisher-" + str(row["publisher"])
                  publisher_single_uri = base_url + publisher_single_id
                
                  graph.add(( URIRef(base_url+"venue-" + str(row["id"]) ), hasPublisher, URIRef(publisher_single_uri)))
       

        #Journal
                if row["venue_type"] == "journal":
                    graph.add(( URIRef(base_url+"venue-" + str(row["id"]) ), RDF.type, JournalUri))
        #Book   
                elif row["venue_type"] == "book":
                    graph.add(( URIRef(base_url+"venue-" + str(row["id"]) ), RDF.type, BookUri))
        #Proceedings
                elif row["venue_type"] == "proceedings":
                    graph.add(( URIRef(base_url+"venue-" + str(row["id"]) ), RDF.type, ProceedingsUri))

                    if row["event"]:
                         graph.add((URIRef(base_url+"venue-" + str(row["id"]) ), hasEvent, Literal(row["event"])))
                   
        
        

    # JournalArticle
        if row["type"] == "journal-article":
            graph.add((URIRef(publication_single_uri), RDF.type, JournalArticleUri))
            if row["issue"]:
                graph.add((URIRef(publication_single_uri), hasIssue, Literal(row["issue"])))
            if row["volume"]:
                graph.add((URIRef(publication_single_uri), hasVolume, Literal(row["volume"])))
             
        
    #BookChapter
        elif row["type"] == "book-chapter":
            graph.add((URIRef(publication_single_uri), RDF.type, BookChapterUri))
           
            if row["chapter"]:
                graph.add((URIRef(publication_single_uri), hasChapterNumber, Literal(row["chapter"])))
    #ProceedingsPaper   
        elif row["type"] == "proceedings-paper":
            graph.add((URIRef(publication_single_uri), RDF.type,ProceedingsPaperUri))
    #print('len after csv upload: ', len(graph))    
    return graph
        # return true?
        
            
def test_upload_csv_graph(csvpath, graph, expected_triples_list):
    upload_csv_graph(csvpath, graph)
    
    # Verifica triples
    for expected_triple in expected_triples_list:
        assert expected_triple in graph, f"Triple {expected_triple} not present"

    # Altre verifiche?
    
    return True


#store

def upload_on_store(store,graph, endpoint):
    

    # The URL of the SPARQL endpoint is the same URL of the Blazegraph
    # instance + '/sparql'
    #endpoint = 'http://127.0.0.1:9999/blazegraph/sparql'

    # It opens the connection with the SPARQL endpoint instance
    store.open((endpoint, endpoint))

    for triple in graph.triples((None, None, None)):
        store.add(triple)
        #print(triple)
    # Once finished, remeber to close the connection
    store.close()

    return True

#def test    
def upload_json_authors(jsonpath, graph):
    f = open(jsonpath, encoding='utf8')
    my_dict = json.load(f)
    f.close()
    #id_author_dict={}
    for doi in my_dict["authors"]:
        publication_single_id = "publication-" + doi
        publication_single_uri = base_url + publication_single_id
        #if doi['orcid'] not in id_author_dict.keys():
            #id_author_dict[doi['orcid']]=base_url+doi['orcid']
        for author in my_dict['authors'].get(doi):
            author_uri=base_url+author['orcid']
            graph.add((URIRef(publication_single_uri), hasAuthor, URIRef(author_uri)))
            graph.add((URIRef(author_uri), hasGivenName, Literal(author['given'])))
            graph.add((URIRef(author_uri), hasFamilyName, Literal(author['family'])))
            graph.add((URIRef(author_uri), hasIdentifier, Literal(author['orcid'])))
    #print('len after authors graph: ',len(graph))
    return True

def upload_json_publishers(jsonpath, graph):
    f = open(jsonpath, encoding='utf8')
    my_dict = json.load(f)
    f.close()
    for crossref in my_dict.get('publishers'):
        publisher_single= my_dict['publishers'].get(crossref)
        id_pub="publisher-" + publisher_single['id']
        publisher_single_uri = base_url + id_pub
    
        graph.add((URIRef(publisher_single_uri), hasIdentifier, Literal(id_pub)))
        graph.add((URIRef(publisher_single_uri), hasName, Literal(publisher_single['name'])))
    #print('len after publishers graph: ',len(graph))
    return True

def upload_json_references(jsonpath, graph):
    f = open(jsonpath, encoding='utf8')
    my_dict = json.load(f)
    f.close()
    for doi in my_dict['references']:
        publication_single_id = "publication-" + doi
        publication_single_uri = base_url + publication_single_id
        
        for cited in my_dict['references'].get(doi):
            cited_uri=base_url + 'publication-'+cited
            
            graph.add((URIRef(publication_single_uri), hasCitedPublication, URIRef(cited_uri)))
    #print('len after references graph: ',len(graph))
    return True

def upload_json_venuedf(jsonpath):
    f = open(jsonpath, encoding='utf8')
    my_dict = json.load(f)
    f.close()
    id_venues_dict = {}

    for doi in my_dict["venues_id"]:
      venue_ids = " ".join(my_dict["venues_id"][doi]) #double underscore for joining venues ids in a string
    
      if venue_ids not in id_venues_dict:
        id_venues_dict[venue_ids] = {"DOIs": [doi], "InternalID": "venue-"+ str(len(id_venues_dict) + 1)}
      else:
        id_venues_dict[venue_ids]["DOIs"].append(doi)

# Create a list of dictionaries for DataFrame creation
    data = [{"Venue IDs": venue_ids, "DOIs": item["DOIs"], "Internal ID": item["InternalID"]} for venue_ids, item in id_venues_dict.items()]

# Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(data)
     

    return df
'''
def upload_json_graph(venuedf, graph):
        for idx, row in venuedf.iterrows():
            uri_venue = base_url + row["Internal ID"] 
            graph.add((URIRef(uri_venue), hasIdentifier, Literal(row["Venue IDs"]) ) )
            for el in row["DOIs"]:                        
                new_object = base_url + row["Internal ID"] 

                for soggetto, oggetto_literal in graph.subject_objects(hasPublicationVenue):
                        if oggetto_literal.strip() == "venue-"+str(el).strip():
                            #print(soggetto, hasPublicationVenue, oggetto_literal)
                            graph.remove((soggetto, hasPublicationVenue, oggetto_literal))
                            graph.add((soggetto, hasPublicationVenue, URIRef(new_object)))
                            #print(soggetto, hasPublicationVenue, URIRef(new_object))
                for soggetto, oggetto in graph.subject_objects(RDF.type):       
                #stessa cosa per publisher e type 
                        if soggetto.strip() == "venue-"+str(el).strip():
                            graph.remove((soggetto, RDF.type, oggetto))
                            graph.add((URIRef(new_object), RDF.type, oggetto ))
                # anche id       
        return True
'''
def upload_json_graph(venuedf, my_graph, store, endpoint):
    #print(len(my_graph))
    second_graph=Graph()
    
    for idx, row in venuedf.iterrows():
        uri_venue = base_url + row["Internal ID"]

        my_graph.add((URIRef(uri_venue), hasIdentifier, Literal(row["Venue IDs"])))
    
        for el in row["DOIs"]:
            new_object = base_url + row["Internal ID"]

            #for soggetto, oggetto_literal in my_graph.subject_objects(hasPublicationVenue):

                #if oggetto_literal.strip() == "venue-" + str(el).strip():
            second_graph.add(( URIRef(base_url+"publication-" + str(el).strip()), hasPublicationVenue, URIRef(base_url+"venue-"+ str(el).strip())  ))     
                    #old_uri = URIRef(oggetto_literal)  # Convert old URI to URIRef
            my_graph.add((URIRef(base_url+"publication-" + str(el).strip()), hasPublicationVenue, URIRef(new_object)))
            #print(URIRef(base_url+"publication-" + str(el).strip()), hasPublicationVenue, URIRef(new_object))
                    # Remove the corresponding triples from the triplestore
            
            '''        
            #for soggetto, oggetto in my_graph.subject_objects(RDF.type):
                #if soggetto.strip() == "venue-" + str(el).strip():
                    
                    #old_uri = URIRef(soggetto)  # Convert old URI to URIRef
            my_graph.remove((old_uri, RDF.type, oggetto))
            my_graph.add((URIRef(new_object), RDF.type, oggetto))

            # Remove the corresponding triples from the triplestore
            store.remove(old_uri, RDF.type, oggetto)

            for soggetto, oggetto in my_graph.subject_objects(hasTitle):
                if soggetto.strip() == "venue-" + str(el).strip():
                    print('title')
                    old_uri = URIRef(soggetto)  # Convert old URI to URIRef
                    my_graph.remove((old_uri, hasTitle, oggetto))
                    my_graph.add((URIRef(new_object), hasTitle, oggetto))

                    # Remove the corresponding triples from the triplestore
                    store.remove(old_uri, RDF.type, oggetto) '''
    
    #print('len after venuedf upload: ',len(my_graph))
    return my_graph
