from pandas import concat
from pandas import DataFrame
from classes import *

class GenericQueryProcessor(object):
    def __init__(self, queryProcessor=[]):
        self.queryProcessor = queryProcessor

    def cleanQueryProcessors(self):
        for obj in self.queryProcessor:
            self.queryProcessor.remove(obj)
            return self.queryProcessor

    def addQueryProcessor(self, input):
        self.queryProcessor.append(input)


    def getPublicationsPublishedInYear(self, inputYear):
        finalresultlist = []

        for processor in self.queryProcessor:
            q = processor.getPublicationsPublishedInYear(inputYear)
            for row_idx, row in q.iterrows():
                pub_object = processor.getPublication(row['id'])
                finalresultlist.append(pub_object)

        return finalresultlist


    def getPublicationsByAuthorId(self, authorId):
        
        finalresultlist = []

        for processor in self.queryProcessor:
            
            q = processor.getPublicationsByAuthorId(authorId)
            
            for row_idx, row in q.iterrows():
                pub_object = processor.getPublication(row['id'])
                finalresultlist.append(pub_object)

        return finalresultlist

    def getMostCitedPublication(self):
        
        result=DataFrame

        for processor in self.queryProcessor:
            q = processor.getMostCitedPublication()
            result = concat([q, result], ignore_index=True)

        # removing the NaN
        result = result.fillna("")
        result.drop_duplicates(subset="id", keep="first", inplace=True)

        result_sorted = result.sort_values(by=["citing_publications"], ascending=False)

        first_row = result_sorted.iloc[0]
        pub_object = processor.getPublication(first_row['id'])

        return pub_object

    def getMostCitedVenue(self):
        
        result = DataFrame()
        
       

        for processor in self.queryProcessor:
            q = processor.getMostCitedVenue()
            result = concat([q, result], ignore_index=True)

        # removing the NaN
        result = result.fillna("")
        result.drop_duplicates(subset="VenueId", keep="first", inplace=True)

        result_sorted = result.sort_values(by=["citing_publications"], ascending=False)

        first_row = result_sorted.iloc[0]

        venue_obj=processor.getVenue(first_row['VenueId'])
        return venue_obj

    def getVenuesByPublisherId(self, publisherId):
        
        finalresultlist = []
        
        for processor in self.queryProcessor:
            q = processor.getVenuesByPublisherId(publisherId)
            for row_idx, row in q.iterrows():
                venue_object = processor.getVenue(row['VenueId'])
                finalresultlist.append(venue_object)
            

        
        return finalresultlist
        # return result
        # for x in self.finalresultlist:
        # for y in x.getPublisher():
        # print (y.getIds())

    def getPublicationInVenue(self, venueId):
        
        finalresultlist = []

        for processor in self.queryProcessor:
            q = processor.getPublicationInVenue(venueId)
            for row_idx, row in q.iterrows():
                pub_object = processor.getPublication(row['id'])
                finalresultlist.append(pub_object)

        return finalresultlist

            
        
    def getJournalArticlesInIssue(self, inputIssue, inputVolume, inputVenueId):
        
        finalresultlist = []

        for processor in self.queryProcessor:
            q = processor.getJournalArticlesInIssue(inputIssue, inputVolume, inputVenueId)

            

            for row_idx, row in q.iterrows():
                    
                    pub_obj=processor.getPublication(row['id'])
                
                    article = JournalArticle(identifier=row["id"], title=row["title"], publicationYear=row["publication_year"],
                                    author=pub_obj.getAuthors(),
                                    publicationVenue=pub_obj.getPublicationVenue(),
                                    citedPublications=pub_obj.getCitedPublications(),
                                    issue=str(inputIssue), volume=str(inputVolume))
                    finalresultlist.append(article) 
        
        return finalresultlist
        # return result
        # for x in self.finalresultlist:
        # print(x, x.getPublicationVenue())

    def getJournalArticlesInVolume(self, inputVolume, inputVenueId):
        
        finalresultlist = []

        for processor in self.queryProcessor:
            q = processor.getJournalArticlesInVolume(inputVolume, inputVenueId)

            for row_idx, row in q.iterrows():
                    
                    pub_obj=processor.getPublication(row['id'])
                
                    article = JournalArticle(identifier=row["id"], title=row["title"], publicationYear=row["publication_year"],
                                    author=pub_obj.getAuthors(),
                                    publicationVenue=pub_obj.getPublicationVenue(),
                                    citedPublications=pub_obj.getCitedPublications(),
                                    issue=row["issue"], volume=str(inputVolume))
                    finalresultlist.append(article) 

           
        return finalresultlist

    def getJournalArticlesInJournal(self, inputVenueId):

        finalresultlist = []

        for processor in self.queryProcessor:
            q = processor.getJournalArticlesInJournal( inputVenueId)

            for row_idx, row in q.iterrows():
                    
                    pub_obj=processor.getPublication(row['id'])
                
                    article = JournalArticle(identifier=row["id"], title=row["title"], publicationYear=row["publication_year"],
                                    author=pub_obj.getAuthors(),
                                    publicationVenue=pub_obj.getPublicationVenue(),
                                    citedPublications=pub_obj.getCitedPublications(),
                                    issue=row["issue"], volume=str(row['volume']) )

                    finalresultlist.append(article)

           
        return finalresultlist
       
        # for x in self.finalresultlist:
        #   print(x.getIds(), x.getAuthors())

    def getProceedingsByEvent(self, inputEvent):
        
        finalresultlist = []
        
        
        for processor in self.queryProcessor:
            q = processor.getProceedingsByEvent(inputEvent)
            
        #result.drop_duplicates(subset="venueID", keep="first", inplace=True)
            for row_idx, row in q.iterrows():
                proceedings_obj = Proceedings(identifiers=row["VenueId"], title=row["title"], publisher=[row["publisher"]], event=row["event"])
                finalresultlist.append(proceedings_obj)

        # for x in self.finalresultlist:
        # print(x, x.getPublicationVenue())
        return finalresultlist

    def getPublicationAuthors(self, inputPubId):
        finalresultlist = []

        for processor in self.queryProcessor:
            q = processor.getPublicationAuthors(inputPubId)

           
            for row_idx, row in q.iterrows():
                aut_obj = processor.getAuthor(row['PersonId'])
                finalresultlist.append(aut_obj)

            # return result
        return finalresultlist

    def getPublicationsByAuthorName(self, authorname):
        
        finalresultlist = []

        for processor in self.queryProcessor:
            q = processor.getPublicationsByAuthorName(authorname)
           

            for row_idx, row in q.iterrows():
                pub_object = processor.getPublication(row['id'])
                finalresultlist.append(pub_object)

        # return result
        return finalresultlist

    def getDistinctPublisherOfPublications(self, inputList):
        
        finalresultlist = []
        for processor in self.queryProcessor:
            q = processor.getDistinctPublisherOfPublications(inputList)

            
            for row_idx, row in q.iterrows():
                publisher_object = processor.getOrganization(row['OrganizationId'])
                finalresultlist.append(publisher_object)

        # return result
        return finalresultlist