import os.path
import pandas as pd
import json
import numpy as np
import pprint
#rdflib 
from rdflib import Graph
from rdflib import URIRef
from rdflib import RDF
from rdflib import Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from graph_functions_and_tests import upload_csv_graph, upload_on_store, upload_json_authors, upload_json_publishers, upload_json_references, upload_json_venuedf, upload_json_graph


#---------------URIs---------------
#--------------baseurl-------------
base_url = "https://github.com/martasoricetti/data_science_project/"

#--------Sub-classes of Publications---:
JournalArticleUri = URIRef("http://purl.org/spar/fabio/JournalArticle")
BookChapterUri = URIRef("http://purl.org/spar/fabio/BookChapter")
ProceedingsPaperUri = URIRef
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
hasIdentifier = URIRef("http://purl.org/dc/terms/identifier")                   
hasTitle = URIRef("http://purl.org/dc/terms/title")  

#------Publication and its sub-classes---:
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


'''
df_publication = pd.read_csv("data/graph_publications.csv", keep_default_na=False,
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
    if row["publication_year"]:
        print(row["publication_year"])
    else:
        print("-----------------------------------------------------------------------------------------")
        '''
#def upload_json_graph(jsonpath, graph):
'''
f = open('data/graph_other_data.json', encoding='utf8')
my_dict = json.load(f)
f.close()
id_venues_dict=dict()
for doi in my_dict["venues_id"]:
    print  (" ".join(my_dict["venues_id"][doi]))
 
    if [" ".join(my_dict["venues_id"][doi])] not in id_venues_dict.keys():
        id_venues_dict[" ".join(my_dict["venues_id"][doi])]=str()
    else:
        id_venues_dict[" ".join(my_dict["venues_id"][doi])]+=doi
print(id_venues_dict) 
'''
'''
import json

f = open('data/graph_other_data.json', encoding='utf8')
my_dict = json.load(f)
f.close()

id_venues_dict = {}

for doi in my_dict["venues_id"]:
    venue_ids = "__".join(my_dict["venues_id"][doi]) #double underscore
    
    if venue_ids not in id_venues_dict:
        id_venues_dict[venue_ids] = doi
    else:
        id_venues_dict[venue_ids] = id_venues_dict[venue_ids]+ "  " + doi #two whitespaces

#print(pd.DataFrame.from_dict(id_venues_dict))
df = pd.DataFrame(id_venues_dict.items(), columns=['Venue IDs', 'DOIs'])

print(df)
'''

'''
import json
import pandas as pd

f = open('data/graph_other_data.json', encoding='utf8')
my_dict = json.load(f)
f.close()

id_venues_dict = {}

for doi in my_dict["venues_id"]:
    venue_ids = "__".join(my_dict["venues_id"][doi]) #double underscore
    
    if venue_ids not in id_venues_dict:
        id_venues_dict[venue_ids] = {"DOIs": [doi], "InternalID": "venue-"+ str(len(id_venues_dict) + 1)}
    else:
        id_venues_dict[venue_ids]["DOIs"].append(doi)

# Create a list of dictionaries for DataFrame creation
data = [{"Venue IDs": venue_ids, "DOIs": item["DOIs"], "Internal ID": item["InternalID"]} for venue_ids, item in id_venues_dict.items()]

# Create a DataFrame from the list of dictionaries
df = pd.DataFrame(data)

#for lista in df["DOIs"]:
    
    #for el in lista:
        #access to single dois

        '''
"""
graph = Graph()


df_publication = pd.read_csv("data/graph_publications.csv", keep_default_na=False,
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
                graph.add((URIRef(publication_single_uri), hasPublicationVenue, Literal("venue-" + str(row["id"]) ) ))
       
        #hasPublisher
                if row["publisher"]:
                  publisher_single_id = "publisher-" + str(row["publisher"])
                  publisher_single_uri = base_url + publisher_single_id
                
                  graph.add(( Literal("venue-" + str(row["id"]) ), hasPublisher, URIRef(publisher_single_uri)))
       

        #Journal
                if row["venue_type"] == "journal":
                    graph.add(( Literal("venue-" + str(row["id"]) ), RDF.type, JournalUri))
        #Book   
                elif row["venue_type"] == "book":
                    graph.add(( Literal("venue-" + str(row["id"]) ), RDF.type, BookUri))
        #Proceedings
                elif row["venue_type"] == "proceedings":
                    graph.add(( Literal("venue-" + str(row["id"]) ), RDF.type, ProceedingsUri))

                    if row["event"]:
                         graph.add(( Literal("venue-" + str(row["id"]) ), hasEvent, Literal(row["event"])))
                   
        
        

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
        
   
f = open("data/graph_other_data.json", encoding='utf8')
my_dict = json.load(f)
f.close()

id_venues_dict = {}

for doi in my_dict["venues_id"]:
    venue_ids = "__".join(my_dict["venues_id"][doi]) #double underscore
    
    if venue_ids not in id_venues_dict:
        id_venues_dict[venue_ids] = {"DOIs": [doi], "InternalID": "venue-"+ str(len(id_venues_dict) + 1)}
    else:
        id_venues_dict[venue_ids]["DOIs"].append(doi)

# Create a list of dictionaries for DataFrame creation
data = [{"Venue IDs": venue_ids, "DOIs": item["DOIs"], "Internal ID": item["InternalID"]} for venue_ids, item in id_venues_dict.items()]

# Create a DataFrame from the list of dictionaries
df = pd.DataFrame(data)
for idx, row in df.iterrows():
            for el in row["DOIs"]:
            
              
                new_object = base_url + row["Internal ID"]
    
                for soggetto, oggetto_literal in graph.subject_objects(hasPublicationVenue):
                        if oggetto_literal.strip() == "venue-"+str(el).strip():
                            #print(soggetto, hasPublicationVenue, oggetto_literal)
                            graph.remove((soggetto, hasPublicationVenue, oggetto_literal))
                            graph.add((soggetto, hasPublicationVenue, URIRef(new_object)))
                            #print(soggetto, hasPublicationVenue, URIRef(new_object))


def upload_json_references(jsonpath):
    f = open(jsonpath, encoding='utf8')
    my_dict = json.load(f)
    f.close()
    for doi in my_dict['references']:
        publication_single_id = "publication-" + doi
        publication_single_uri = base_url + publication_single_id
        cited_list=[]
        for cited in my_dict['references'].get(doi):
            cited_uri=base_url + 'publication-'+cited
            cited_list.append(URIRef(cited_uri))
        print(publication_single_uri, 'hasCitedPublication', cited_list)

upload_json_references('../data/graph_other_data.json')
"""
my_graph = Graph()
def uploadData( path: str, graph):
        if os.path.exists(path):
            
                    
            if path.endswith(".csv"):               
                #funzione per upload csv
                upload_csv_graph(path, graph)
                return True
            elif path.endswith(".json"):
                #funzione per upload json
                venue_df=upload_json_venuedf(path)
                upload_json_graph(venue_df, graph)
                upload_json_authors(path, graph)
                upload_json_publishers(path, graph)
                upload_json_references(path,graph)
                return True
        #return my_graph
uploadData('../data/graph_publications.csv', my_graph)
#uploadData('../data/graph_other_data.json', my_graph)
print('len:', len(my_graph))
for stmt in my_graph:
    pprint.pprint(stmt)