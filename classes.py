
Publications_dict=dict()

class IdentifiableEntity(object):

    def __init__(self, identifiers):
        self.id = identifiers
    
    def getIds(self):
        return self.id

class Person(IdentifiableEntity):
    def __init__(self, identifier, givenName, familyName):
        self.givenName = givenName
        self.familyName = familyName
        super().__init__(identifier)
    
    def getGivenName(self):
        return self.givenName
    
    def getFamilyName(self):
        return self.familyName

class Publication(IdentifiableEntity):
    def __init__(self, identifier, publicationYear, title, author, publicationVenue, citedPublications):
        self.publicationYear = publicationYear
        self.title = title
        self.author = author
        self.publicationVenue = publicationVenue 
        self.citedPublications = citedPublications
        super().__init__(identifier)
    

    def getPublicationYear(self):
        if self.publicationYear != None:
            return self.publicationYear
        else:
            return None
    
    def getTitle(self):
        return self.title
    
    def getAuthors(self):
       return self.author
            
    def getCitedPublications(self):
        return self.citedPublications               
    
    def getPublicationVenue(self):
        return self.publicationVenue


class JournalArticle(Publication):
    def __init__(self, id, publicationYear, title, author, publicationVenue, issue, volume, citedPublications):
        self.issue = issue
        self.volume = volume
        
        super().__init__(id, publicationYear, title, author, publicationVenue, citedPublications)

    def getIssue(self):
        if self.issue != '':      #decidere come trattare i missing values anche nell'instanziamento (fillNa nel dataframe?)
            return self.issue
        else:
            return None

    def getVolume(self):
        if self.volume != '':
            return self.volume
        else:
            return None


class BookChapter(Publication):
    def __init__(self, id, publicationYear, title, author, publicationVenue, chapterNumber, citedPublications):
        self.chapterNumber = chapterNumber
        
        super().__init__(id, publicationYear, title, author, publicationVenue, citedPublications)
    

    def getChapterNumber(self):
        return self.chapterNumber

class ProceedingsPaper(Publication):
    def __init__(self, id, publicationYear, title, author, publicationVenue, citedPublications):
        super().__init__(id, publicationYear, title, author, publicationVenue, citedPublications)


class Venue(IdentifiableEntity):
    def __init__(self, identifiers, title, publisher):
        self.title = title
        self.publisher = publisher
        super().__init__(identifiers)

    def getTitle(self):
        return self.title
    
    def getPublisher(self):
        return self.publisher


class Journal(Venue):
    def __init__(self, identifiers, title, publisher):
        super().__init__(identifiers, title, publisher)


class Book(Venue):
    def __init__(self, identifiers, title, publisher):
        super().__init__(identifiers, title, publisher)


class Proceedings(Venue):
    def __init__(self, identifiers, title, publisher, event):
        self.event = event

        super().__init__(identifiers, title, publisher)

    def getEvent(self):
        return self.event


class Organization(IdentifiableEntity):
    def __init__(self, identifier, name):
        self.name = name
        super().__init__(identifier)

    def getName(self):
        return self.name

