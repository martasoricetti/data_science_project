from pandas import concat
from pandas import DataFrame
from classes import *

class GenericQueryProcessor(object):
    def __init__(self, queryProcessor=[]):
        self.queryProcessor = queryProcessor
        self.organizationsDict = dict()
        self.venuesDict = dict()
        self.authorsDict = dict()

    def cleanQueryProcessors(self):
        for obj in self.queryProcessor:
            self.queryProcessor.remove(obj)
            return self.queryProcessor

    def addQueryProcessor(self, input):
        self.queryProcessor.append(input)
        self.organizationsDict = input.getAllOrganizations()
        self.venuesDict = input.getAllVenues()
        self.authorsDict = input.getAllAuthors()

    def getPublicationsPublishedInYear(self, inputYear):
        finalresultlist = []

        for processor in self.queryProcessor:
            q = processor.getPublicationsPublishedInYear(inputYear)
            for row_idx, row in q.iterrows():
                pub_object = processor.getPublication(row['id'])
                finalresultlist.append(pub_object)

        return finalresultlist

        # for x in self.finalresultlist:
        # print("the first publication is")
        # print(x.getIds())
        # print("cites")
        # for y in  x.getCitedPublication():
        #   print(y.getIds())
        # print("---------")

    def getPublicationsByAuthorId(self, authorId):
        result = DataFrame()
        self.finalresultlist = []

        for processor in self.queryProcessor:
            q = processor.getPublicationsByAuthorId(authorId)
            result = concat([result, q], ignore_index=True)

        # removing the NaN
        result = result.fillna("")
        result.drop_duplicates(subset="doi", keep="first", inplace=True)

        for row_idx, row in result.iterrows():
            if row["doi"] in Publications_dict:
                self.finalresultlist.append(Publications_dict[row["doi"]])

        return self.finalresultlist

    def getMostCitedPublication(self):
        self.finalresultlist = []
        result = DataFrame()

        for processor in self.queryProcessor:
            q = processor.getMostCitedPublication()
            result = concat([q, result], ignore_index=True)

        # removing the NaN
        result = result.fillna("")
        result.drop_duplicates(subset="doi", keep="first", inplace=True)

        result_sorted = result.sort_values(by=["Cited"], ascending=False)

        first_row = result_sorted.iloc[0]

        if first_row["doi"] in Publications_dict:
            return Publications_dict[first_row["doi"]]

    def getMostCitedVenue(self):
        self.finalresultlist = []
        result = DataFrame()
        unified_dict = dict()
        for processor in self.queryProcessor:
            venue_first_dict = processor.getAllOrganizations()
            unified_dict.update(venue_first_dict)

        for processor in self.queryProcessor:
            q = processor.getMostCitedVenue()
            result = concat([q, result], ignore_index=True)

        # removing the NaN
        result = result.fillna("")
        result.drop_duplicates(subset="venueID", keep="first", inplace=True)

        result_sorted = result.sort_values(by=["Cited"], ascending=False)

        first_row = result_sorted.iloc[0]

        x = Venue(identifiers=first_row["venueID"], title=first_row["title"],
                  publisher=unified_dict[first_row["organizationID"]])

        return x

    def getVenuesByPublisherId(self, publisherId):
        result = DataFrame()
        self.finalresultlist = []
        unified_dict = dict()
        for processor in self.queryProcessor:
            venue_first_dict = processor.getAllOrganizations()
            unified_dict.update(venue_first_dict)
        for processor in self.queryProcessor:
            q = processor.getVenuesByPublisherId(publisherId)

            result = concat([result, q], ignore_index=True)

        # removing the NaN
        result = result.fillna("")
        result.drop_duplicates(subset="venueID", keep="first", inplace=True)

        for row_idx, row in result.iterrows():
            x = Venue(identifiers=row["venueID"], title=row["title"], publisher=unified_dict[row["organizationID"]])
            self.finalresultlist.append(x)

        return self.finalresultlist
        # return result
        # for x in self.finalresultlist:
        # for y in x.getPublisher():
        # print (y.getIds())

    def getPublicationInVenue(self, venueId):
        result = DataFrame()
        self.finalresultlist = []

        for processor in self.queryProcessor:
            q = processor.getPublicationInVenue(venueId)

            result = concat([result, q], ignore_index=True)

        # removing the NaN
        result = result.fillna("")
        result.drop_duplicates(subset="doi", keep="first", inplace=True)

        for row_idx, row in result.iterrows():
            if row["doi"] in Publications_dict:
                self.finalresultlist.append(Publications_dict[row["doi"]])

        return self.finalresultlist

    def getJournalArticlesInIssue(self, inputIssue, inputVolume, inputVenueId):
        result = DataFrame()
        self.finalresultlist = []

        for processor in self.queryProcessor:
            q = processor.getJournalArticlesInIssue(inputIssue, inputVolume, inputVenueId)

            result = concat([result, q], ignore_index=True)

        # removing the NaN
        result = result.fillna("")
        result.drop_duplicates(subset="doi", keep="first", inplace=True)

        for row_idx, row in result.iterrows():
            if row["doi"] in Publications_dict:
                x = JournalArticle(identifiers=row["doi"], title=row["title"], publicationYear=row["publicationYear"],
                                   author=Publications_dict[row["doi"]].getAuthors(),
                                   publicationVenue=Publications_dict[row["doi"]].getPublicationVenue(),
                                   citedpublication=Publications_dict[row["doi"]].getCitedPublication(),
                                   issue=row["issue"], volume=row["volume"])
                self.finalresultlist.append(x)

        return self.finalresultlist
        # return result
        # for x in self.finalresultlist:
        # print(x, x.getPublicationVenue())

    def getJournalArticlesInVolume(self, inputVolume, inputVenueId):
        result = DataFrame()
        self.finalresultlist = []

        for processor in self.queryProcessor:
            q = processor.getJournalArticlesInVolume(inputVolume, inputVenueId)

            result = concat([result, q], ignore_index=True)

        # removing the NaN
        result = result.fillna("")
        result.drop_duplicates(subset="doi", keep="first", inplace=True)

        for row_idx, row in result.iterrows():
            if row["doi"] in Publications_dict:
                x = JournalArticle(identifiers=row["doi"], title=row["title"], publicationYear=row["publicationYear"],
                                   author=Publications_dict[row["doi"]].getAuthors(),
                                   publicationVenue=Publications_dict[row["doi"]].getPublicationVenue(),
                                   citedpublication=Publications_dict[row["doi"]].getCitedPublication(),
                                   issue=row["issue"], volume=row["volume"])
                self.finalresultlist.append(x)
        # return result
        return self.finalresultlist

    def getJournalArticlesInJournal(self, inputvenueid):
        result = DataFrame()
        self.finalresultlist = []

        for processor in self.queryProcessor:
            q = processor.getJournalArticlesInJournal(inputvenueid)
            result = concat([result, q], ignore_index=True)
        # removing the NaN
        result = result.fillna("")
        result.drop_duplicates(subset="doi", keep="first", inplace=True)

        for row_idx, row in result.iterrows():
            if row["doi"] in Publications_dict:
                x = JournalArticle(identifiers=row["doi"], title=row["title"], publicationYear=row["publicationYear"],
                                   author=Publications_dict[row["doi"]].getAuthors(),
                                   publicationVenue=Publications_dict[row["doi"]].getPublicationVenue(),
                                   citedpublication=Publications_dict[row["doi"]].getCitedPublication(),
                                   issue=row["issue"], volume=row["volume"])
                self.finalresultlist.append(x)
        return self.finalresultlist
        # for x in self.finalresultlist:
        #   print(x.getIds(), x.getAuthors())

    def getProceedingsByEvent(self, inputEvent):
        result = DataFrame()
        self.finalresultlist = []
        unified_dict = dict()
        for processor in self.queryProcessor:
            venue_first_dict = processor.getAllOrganizations()
            unified_dict.update(venue_first_dict)
        for processor in self.queryProcessor:
            q = processor.getProceedingsByEvent(inputEvent)
            result = concat([result, q], ignore_index=True)
        # removing the NaN
        result = result.fillna("")
        result.drop_duplicates(subset="venueID", keep="first", inplace=True)
        for row_idx, row in result.iterrows():
            x = Proceedings(identifiers=row["venueID"], title=row["title"], publisher=unified_dict[row["publisherID"]],
                            event=row["event"])
            self.finalresultlist.append(x)

        # for x in self.finalresultlist:
        # print(x, x.getPublicationVenue())
        return self.finalresultlist

    def getPublicationAuthors(self, inputPubId):
        result = DataFrame()
        self.finalresultlist = []

        for processor in self.queryProcessor:
            q = processor.getPublicationAuthors(inputPubId)

            result = concat([result, q], ignore_index=True)

        # removing the NaN
        result = result.fillna("")
        result.drop_duplicates(subset="authorID", keep="first", inplace=True)

        for row_idx, row in result.iterrows():
            x = Person(identifiers=row["authorID"], givenName=row["givenName"], familyName=row["familyName"])
            self.finalresultlist.append(x)

        # return result
        return self.finalresultlist

    def getPublicationsByAuthorName(self, authorname):
        result = DataFrame()
        self.finalresultlist = []

        for processor in self.queryProcessor:
            q = processor.getPublicationsByAuthorName(authorname)

            result = concat([result, q], ignore_index=True)

        # removing the NaN
        result = result.fillna("")
        result.drop_duplicates(subset="doi", keep="first", inplace=True)

        for row_idx, row in result.iterrows():
            if row["doi"] in Publications_dict:
                self.finalresultlist.append(Publications_dict[row["doi"]])

        # return result
        return self.finalresultlist

    def getDistinctPublisherOfPublications(self, inputList):
        result = DataFrame()
        self.finalresultlist = []
        for processor in self.queryProcessor:
            q = processor.getDistinctPublisherOfPublications(inputList)

            result = concat([result, q], ignore_index=True)

        # removing the NaN
        result = result.fillna("")
        result.drop_duplicates(subset="organizationID", keep="first", inplace=True)

        for row_idx, row in result.iterrows():
            x = Organization(identifiers=row["organizationID"], name=row["name"])
            self.finalresultlist.append(x)

        # return result
        return self.finalresultlist