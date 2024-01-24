import os.path
import pandas as pd
import json
from sqlite3 import connect
from classes import *


class RelationalProcessor:
    def __init__(self):
        self.dbPath = ''


    def getDbPath(self):
        if self.dbPath:
            return self.dbPath

    def setDbPath(self, path: str):
        if path:
            self.dbPath = path
            return True
        else:
            return False


class RelationalDataProcessor(RelationalProcessor):
    def __init__(self):
        super().__init__()

    def uploadData(self, path: str):
        if os.path.exists(path):
            if path.endswith(".csv"):
                df_publication = pd.read_csv(path, keep_default_na=False,
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

                # Journal
                # Data frame of journals
                df_journal = df_publication.query("venue_type == 'journal'")
                df_journal_filtered = df_journal[["id", "publication_venue", "publisher"]]

                # Book
                df_book = df_publication.query("venue_type == 'book'")
                df_book_filtered = df_book[["id", "publication_venue", "publisher"]]

                # Proceedings
                df_proceedings = df_publication.query("venue_type == 'proceedings'")
                df_proceedings_filtered = df_proceedings[["id", "publication_venue", "publisher", "event"]]

                # JournalArticle
                df_journal_article = df_publication.query("type == 'journal-article'")
                df_journal_article_filt = df_journal_article[
                    ["id", "publication_year", "title", "issue", "volume", "publication_venue"]]

                # BookChapter
                df_book_chapter = df_publication.query("type == 'book-chapter'")
                df_book_chapter_filt = df_book_chapter[
                    ["id", "publication_year", "title", "chapter", "publication_venue"]]

                # ProceedingsPaper
                df_proceedings_paper = df_publication.query("type == 'proceedings-paper'")
                df_proceedings_paper_filt = df_proceedings_paper[
                    ["id", "publication_year", "title", "publication_venue"]]

                with connect(self.dbPath) as con:
                    df_journal_filtered.to_sql("Journal", con, if_exists="replace", index=False)
                    df_book_filtered.to_sql("Book", con, if_exists="replace", index=False)
                    df_proceedings_filtered.to_sql("Proceedings", con, if_exists="replace", index=False)
                    df_journal_article_filt.to_sql("JournalArticle", con, if_exists="replace", index=False)
                    df_book_chapter_filt.to_sql("BookChapter", con, if_exists="replace", index=False)
                    df_proceedings_paper_filt.to_sql("ProceedingsPaper", con, if_exists="replace", index=False)
                    con.commit()

            if path.endswith(".json"):
                f = open(path, encoding='utf8')
                my_dict = json.load(f)
                f.close()

                # authors
                authors_list = []
                for doi in my_dict["authors"]:
                    for author_dict in my_dict["authors"][doi]:
                        authors_list.append(author_dict)
                # dataframe with just authors
                df_authors = (pd.DataFrame(authors_list, columns=["orcid", "family", "given"])).drop_duplicates(
                    subset=["orcid"], ignore_index=True)
                df_authors = df_authors.rename(
                    columns={'orcid': 'PersonId', 'family': 'familyName', 'given': 'givenName'})
                df_authors = df_authors.fillna('')

                # 1doi-more authors internal id table
                authors = list()
                for doi in my_dict["authors"]:
                    if my_dict["authors"][doi]:
                        authors_orcid_list = list()
                        for author_dict in my_dict["authors"][doi]:
                            author_id = author_dict["orcid"]
                            authors_orcid_list.append(author_id)
                        dict_df = {'doi': doi,
                                   'author': " ".join(authors_orcid_list)}
                        authors.append(dict_df)
                    else:
                        pass
                df_doi_authorsId = pd.DataFrame(authors, columns=["doi", "author"])

                # organization
                organization_list = []
                for crossref_id in my_dict["publishers"]:
                    organization_list.append(my_dict["publishers"][crossref_id])
                df_organizations = (pd.DataFrame(organization_list, columns=["id", "name"])).drop_duplicates(
                    subset=["id"], ignore_index=True)
                df_organizations = df_organizations.rename(columns={'id': 'OrganizationId'})
                df_organizations = df_organizations.fillna('')
                # venue-doi table
                venues_doi = list()
                for doi in my_dict["venues_id"]:
                    for issn in my_dict["venues_id"][doi]:
                        venues_doi_dict = {"doi": doi,
                                           "issn": issn}
                        venues_doi.append(venues_doi_dict)
                df_venues_doi = pd.DataFrame(venues_doi, columns=["doi", "issn"])

                # 1doi-more venues id table
                venues = list()
                for doi in my_dict["venues_id"]:
                    if my_dict["venues_id"][doi]:
                        dict_df = {'doi': doi,
                                   'publicationVenue': " ".join(my_dict["venues_id"][doi])}
                        venues.append(dict_df)
                    else:
                        pass
                df_doi_venuesId = pd.DataFrame(venues, columns=["doi", "publicationVenue"])


                # Journal
                # join df_venues_doi and journals dataframes
                with connect(self.dbPath) as con:
                    df_journal = pd.read_sql('SELECT * FROM Journal', con)
                df_joined_journals = pd.merge(df_journal, df_doi_venuesId, how="left", left_on="id", right_on="doi")
                df_joined_journals_filtered = df_joined_journals[
                    ["publicationVenue", "publication_venue", "publisher"]].drop_duplicates(ignore_index=True)
                df_journal = df_joined_journals_filtered.rename(
                    columns={'publicationVenue': 'VenueId', 'publication_venue': 'title'})
                df_journal = df_journal.fillna('')

                # Book
                # join df_venues_doi and book dataframes
                with connect(self.dbPath) as con:
                    df_book = pd.read_sql('SELECT * FROM Book', con)
                df_joined_book = pd.merge(df_book, df_doi_venuesId, how="left", left_on="id", right_on="doi")
                df_joined_book_filtered = df_joined_book[["publicationVenue", "publication_venue", "publisher"]].drop_duplicates(
                    ignore_index=True)
                df_book = df_joined_book_filtered.rename(columns={'publication_venue': 'title', 'publicationVenue': 'VenueId'})
                df_book = df_book.fillna('')

                # Proceedings
                # join df_venues_doi and proceedings dataframes
                with connect(self.dbPath) as con:
                    df_proceedings = pd.read_sql('SELECT * FROM Proceedings', con)
                df_joined_proceedings = pd.merge(df_proceedings, df_doi_venuesId, how="left", left_on="id",
                                                 right_on="doi")
                df_joined_proceedings_filtered = df_joined_proceedings[
                    ["publicationVenue", "publication_venue", "publisher", "event"]].drop_duplicates(ignore_index=True)
                df_proceedings = df_joined_proceedings_filtered.rename(
                    columns={'publicationVenue': 'VenueId', 'publication_venue': 'title'})
                df_proceedings = df_proceedings.fillna('')

                # JournalArticle
                with connect(self.dbPath) as con:
                    df_journal_article = pd.read_sql('SELECT * FROM JournalArticle', con)

                # substitute the venue's title in the column publicationVenue with its id
                df_venue_journal_art = pd.merge(df_journal_article, df_doi_venuesId, how="left", left_on="id",
                                                right_on="doi")
                df_venue_journal_updated = df_venue_journal_art[
                    ["doi", "publication_year", "title", "issue", "volume", "publicationVenue"]]
                # authors
                df_venue_author_journal_art = pd.merge(df_venue_journal_updated, df_doi_authorsId, how="left",
                                                       left_on="doi", right_on="doi").fillna('')


                #table Citations
                citing_cited = list()
                for doi in my_dict["references"]:
                    if my_dict["references"][doi]:
                        for cited in my_dict["references"][doi]:
                            dict_df = {'citing':doi,
                                       'cited':cited}
                            citing_cited.append(dict_df)
                df_citing_cited = pd.DataFrame(citing_cited, columns=["citing", "cited"])

                # column cites
                references = list()
                for doi in my_dict["references"]:
                    if my_dict["references"][doi]:
                        dict_df = {'citing': doi,
                                   'cited': " ".join(my_dict["references"][doi])}
                        references.append(dict_df)
                    else:
                        pass
                df_references_doi = pd.DataFrame(references, columns=["citing", "cited"])
                df_journal_article_final = pd.merge(df_venue_author_journal_art, df_references_doi, how="left", left_on="doi", right_on="citing")[
                    ["doi", "publication_year", "title", "issue", "volume", "publicationVenue", "author", "cited"]]
                df_journal_article_final = df_journal_article_final.fillna('')
                df_journal_article_final = df_journal_article_final.rename(columns={'doi': 'id'})
                # BookChapter
                with connect(self.dbPath) as con:
                    df_book_chapter = pd.read_sql('SELECT * FROM BookChapter', con)
                # publicationVenue
                df_venue_book_chapter = pd.merge(df_book_chapter, df_doi_venuesId, how="left", left_on="id", right_on="doi")[
                    ["id", "publication_year", "title", "chapter", "publicationVenue"]].fillna('')
                # author
                df_venue_author_book_chapter = pd.merge(df_venue_book_chapter, df_doi_authorsId, how="left", left_on="id", right_on="doi")[
                    ["id", "publication_year", "title", "chapter", "publicationVenue", "author"]]

                # cites
                df_book_chapter_final = pd.merge(df_venue_author_book_chapter, df_references_doi, how="left", left_on="id", right_on="citing")[
                    ["id", "publication_year", "title", "chapter", "publicationVenue", "author", "cited"]]
                df_book_chapter_final = df_book_chapter_final.fillna('')

                # ProceedingsPaper
                with connect(self.dbPath) as con:
                    df_proceedings_paper = pd.read_sql('SELECT * FROM ProceedingsPaper', con)
                # publicationVenue
                df_venue_proceedings_paper = pd.merge(df_proceedings_paper, df_doi_venuesId, how="left", left_on="id", right_on="doi")[
                    ["id", "publication_year", "title", "publicationVenue"]].fillna('')
                # author
                df_venue_author_proceedings_paper = pd.merge(df_venue_proceedings_paper, df_doi_authorsId, how="left", left_on="id", right_on="doi")[
                    ["id", "publication_year", "title", "publicationVenue", "author"]]
                # cites
                df_proceedings_paper_final = pd.merge(df_venue_author_proceedings_paper, df_references_doi, how="left", left_on="id",
                    right_on="citing")[["id", "publication_year", "title", "publicationVenue", "author", "cited"]]
                df_proceedings_paper_final = df_proceedings_paper_final.fillna('')

                with connect(self.dbPath) as con:
                    df_authors.to_sql("Person", con, if_exists="replace", index=False)
                    df_organizations.to_sql("Organization", con, if_exists="replace", index=False)
                    df_journal.to_sql("Journal", con, if_exists="replace", index=False)
                    df_book.to_sql("Book", con, if_exists="replace", index=False)
                    df_proceedings.to_sql("Proceedings", con, if_exists="replace", index=False)
                    df_journal_article_final.to_sql("JournalArticle", con, if_exists="replace", index=False)
                    df_book_chapter_final.to_sql("BookChapter", con, if_exists="replace", index=False)
                    df_proceedings_paper_final.to_sql("ProceedingsPaper", con, if_exists="replace", index=False)
                    df_citing_cited.to_sql("Citations", con, if_exists="replace", index=False)
                    con.commit()
        con.close()


class RelationalQueryProcessor(RelationalProcessor):
    def __init__(self):
        super().__init__()

    def getPublicationsPublishedInYear(self, year: int):
        with connect(self.dbPath) as con:
            query1 = "SELECT * FROM JournalArticle WHERE publication_year=%s" % (year)
            query2 = "SELECT * FROM BookChapter WHERE publication_year=%s" % (year)
            query3 = "SELECT * FROM ProceedingsPaper WHERE publication_year=%s" % (year)
            df_sql1 = pd.read_sql(query1, con)
            df_sql2 = pd.read_sql(query2, con)
            df_sql3 = pd.read_sql(query3, con)
            df_sql = pd.concat([df_sql1, df_sql2, df_sql3], ignore_index=True)
            df_sql = df_sql.fillna('')
            df_sql['chapter'] = df_sql['chapter'].astype(str).str.replace('.0', '', regex=False)
        con.close()
        return df_sql

    def getPublicationsByAuthorId(self, id: str):
        with connect(self.dbPath) as con:
            query1 = f'SELECT * FROM JournalArticle WHERE author LIKE "%{id}%"'
            query2 = f'SELECT * FROM BookChapter WHERE author LIKE "%{id}%"'
            query3 = f'SELECT * FROM ProceedingsPaper WHERE author LIKE "%{id}%"'
            df_sql1 = pd.read_sql(query1, con)
            df_sql2 = pd.read_sql(query2, con)
            df_sql3 = pd.read_sql(query3, con)
            df_sql = pd.concat([df_sql1, df_sql2, df_sql3], ignore_index=True)
            df_sql = df_sql.fillna('')
            df_sql['chapter'] = df_sql['chapter'].astype(str).str.replace('.0', '', regex=False)
        con.close()
        return df_sql

    def getMostCitedPublication(self):
        with connect(self.dbPath) as con:
            '''The innermost subquery calculates the citation count for each unique value in the cited column of the table Citations
            referring to it as citation_count.
            The middle subquery then finds the maximum citation_count among all the unique values.
            The outer query selects rows from the results of the first subquery 
            (aliased as citation_counts) where the citation_count matches the maximum citation_count 
            found in the middle subquery.'''

            query1 = """SELECT cited, citation_count
                    FROM (SELECT cited, COUNT(*) as citation_count FROM Citations
                    GROUP BY cited) AS citation_counts
                    WHERE citation_count = (
                    SELECT MAX(citation_count) 
                    FROM (
                        SELECT COUNT(*) as citation_count
                        FROM Citations
                        GROUP BY cited
                    ) AS max_citation_count);"""
            df_sql1 = pd.read_sql(query1, con)
            doi_most_cited_entities = list(df_sql1['cited'])
            query2 = 'SELECT * FROM JournalArticle WHERE id IN ({});'
            query2 = query2.format(','.join(["'{}'".format(doi) for doi in doi_most_cited_entities]))
            query3 = 'SELECT * FROM BookChapter WHERE id IN ({});'
            query3 = query3.format(','.join(["'{}'".format(doi) for doi in doi_most_cited_entities]))
            query4 = 'SELECT * FROM ProceedingsPaper WHERE id IN ({});'
            query4 = query4.format(','.join(["'{}'".format(doi) for doi in doi_most_cited_entities]))
            df_journal_art = pd.read_sql(query2, con)
            df_book_chapter = pd.read_sql(query3, con)
            df_proceedings_paper = pd.read_sql(query4, con)
            df_sql = pd.concat([df_journal_art, df_book_chapter, df_proceedings_paper], ignore_index=True)
            df_sql = df_sql.fillna('')
            df_sql['chapter'] = df_sql['chapter'].astype(str).str.replace('.0', '', regex=False)
            df_sql.loc[:, "citing_publications"] = list(df_sql1['citation_count'])
        con.close()
        return df_sql

    def getMostCitedVenue(self):
        # the number of citations for a venue is typically the sum of citations across all publications
        # that were published in that venue.
        with connect(self.dbPath) as con:
            query1 = """
            SELECT cited FROM Citations GROUP BY cited
            """
            df_cited = pd.read_sql(query1, con)
            list_cited = df_cited['cited']
            query2 = 'SELECT * FROM JournalArticle WHERE id IN ({});'
            query2 = query2.format(','.join(["'{}'".format(doi) for doi in list_cited]))
            query3 = 'SELECT * FROM BookChapter WHERE id IN ({});'
            query3 = query3.format(','.join(["'{}'".format(doi) for doi in list_cited]))
            query4 = 'SELECT * FROM ProceedingsPaper WHERE id IN ({});'
            query4 = query4.format(','.join(["'{}'".format(doi) for doi in list_cited]))
            df_journal_art = pd.read_sql(query2, con)
            df_book_chapter = pd.read_sql(query3, con)
            df_proceedings_paper = pd.read_sql(query4, con)
            df_all_cited_pub = pd.concat([df_journal_art, df_book_chapter, df_proceedings_paper], ignore_index=True)
            df_all_cited_pub = df_all_cited_pub.fillna('')
            df_all_cited_pub['chapter'] = df_all_cited_pub['chapter'].astype(str).str.replace('.0', '', regex=False)
            venue_ids_cited = list(df_all_cited_pub['publicationVenue'])
            venues_count = dict()
            for el in venue_ids_cited:
                if el in venues_count:
                    venues_count[el] += 1
                else:
                    venues_count[el] = 1
            max_value = max(venues_count.values())
            most_cited_venues = []
            for key in venues_count:
                if venues_count[key] == max_value:
                    most_cited_venues.append(key)

            query5 = 'SELECT * FROM Journal WHERE VenueId IN ({});'
            query5 = query5.format(','.join(["'{}'".format(venue_id) for venue_id in most_cited_venues]))
            query6 = 'SELECT * FROM Book WHERE VenueId IN ({});'
            query6 = query6.format(','.join(["'{}'".format(venue_id) for venue_id in most_cited_venues]))
            query7 = 'SELECT * FROM Proceedings WHERE VenueId IN ({});'
            query7 = query7.format(','.join(["'{}'".format(venue_id) for venue_id in most_cited_venues]))
            df_journal = pd.read_sql(query5, con)
            df_book = pd.read_sql(query6, con)
            df_proceedings = pd.read_sql(query7, con)
            df_sql = pd.concat([df_journal, df_book, df_proceedings], ignore_index=True)
            df_sql = df_sql.fillna('')
            df_sql.loc[:, "citing_publications"] = max_value
        con.close()
        return df_sql


    def getVenuesByPublisherId(self, id: str):
        with connect(self.dbPath) as con:
            query1 = f'SELECT * FROM Journal WHERE publisher="{id}"'
            query2 = f'SELECT * FROM Book WHERE publisher="{id}"'
            query3 = f'SELECT * FROM Proceedings WHERE publisher="{id}"'
            df_sql1 = pd.read_sql(query1, con)
            df_sql2 = pd.read_sql(query2, con)
            df_sql3 = pd.read_sql(query3, con)
            df_sql = pd.concat([df_sql1, df_sql2, df_sql3], ignore_index=True)
            df_sql = df_sql.fillna('')
        con.close()
        return df_sql

    def getPublicationInVenue(self, venueId: str):
        with connect(self.dbPath) as con:
            query1 = f'SELECT * FROM JournalArticle WHERE publicationVenue LIKE "%{venueId}%"'
            query2 = f'SELECT * FROM BookChapter WHERE publicationVenue LIKE "%{venueId}%"'
            query3 = f'SELECT * FROM ProceedingsPaper WHERE publicationVenue LIKE "%{venueId}%"'
            df_journal_art = pd.read_sql(query1, con)
            df_book_chapter = pd.read_sql(query2, con)
            df_proceedings_paper = pd.read_sql(query3, con)
            df_sql = pd.concat([df_journal_art, df_book_chapter, df_proceedings_paper], ignore_index=True)
            df_sql = df_sql.fillna('')
            df_sql['chapter'] = df_sql['chapter'].astype(str).str.replace('.0', '', regex=False)
        con.close()
        return df_sql

    def getJournalArticlesInIssue(self, issue: str, volume: str, journalId: str):
        with connect(self.dbPath) as con:
            query1 = f'SELECT * FROM JournalArticle WHERE issue ="{issue}" AND volume = "{volume}" AND publicationVenue LIKE "%{journalId}%"'
            df_sql1 = pd.read_sql(query1, con)
        con.close()
        return df_sql1

    def getJournalArticlesInVolume(self, volume: str, journalId: str):
        with connect(self.dbPath) as con:
            query1 = f'SELECT * FROM JournalArticle WHERE volume = "{volume}" AND publicationVenue LIKE "%{journalId}%"'
            df_sql1 = pd.read_sql(query1, con)
        con.close()
        return df_sql1

    def getJournalArticlesInJournal(self, journalId: str):
        with connect(self.dbPath) as con:
            query1 = f'SELECT * FROM JournalArticle WHERE publicationVenue LIKE "%{journalId}%"'
            df_sql1 = pd.read_sql(query1, con)
        con.close()
        return df_sql1

    def getProceedingsByEvent(self, eventPartialName: str):
        eventPartialName = eventPartialName.lower()
        with connect(self.dbPath) as con:
            query1 = f'SELECT * FROM Proceedings WHERE event LIKE "%{eventPartialName}%"'
            df_sql1 = pd.read_sql(query1, con)
        con.close()
        return df_sql1

    def getPublicationAuthors(self, publicationId: str):
        with connect(self.dbPath) as con:
            query1 = f'SELECT author FROM JournalArticle WHERE id="{publicationId}"'
            query2 = f'SELECT author FROM BookChapter WHERE id="{publicationId}"'
            query3 = f'SELECT author FROM ProceedingsPaper WHERE id="{publicationId}"'
            df_sql1 = pd.read_sql(query1, con)
            df_sql2 = pd.read_sql(query2, con)
            df_sql3 = pd.read_sql(query3, con)
            if not df_sql1.empty:
                author = str(df_sql1.iloc[0]['author'])
                author_list = author.split(' ')
                if len(author_list) > 1:
                    df_list = []
                    for author_id in author_list:
                        query4 = f'SELECT * FROM Person WHERE PersonId="{author_id}"'
                        df_sql4 = pd.read_sql(query4, con)
                        df_list.append(df_sql4)
                    new_df = pd.concat(df_list, ignore_index=True)
                    return new_df
                else:
                    query4 = f'SELECT * FROM Person WHERE PersonId="{author}"'
                    df_sql4 = pd.read_sql(query4, con)
                    return df_sql4
            elif not df_sql2.empty:
                author = str(df_sql2.iloc[0]['author'])
                author_list = author.split(' ')
                if len(author_list) > 1:
                    df_list = []
                    for author_id in author_list:
                        query4 = f'SELECT * FROM Person WHERE PersonId="{author_id}"'
                        df_sql4 = pd.read_sql(query4, con)
                        df_list.append(df_sql4)
                    new_df = pd.concat(df_list, ignore_index=True)
                    return new_df
                else:
                    query4 = f'SELECT * FROM Person WHERE PersonId="{author}"'
                    df_sql4 = pd.read_sql(query4, con)
                    return df_sql4
            elif not df_sql3.empty:
                author = str(df_sql2.iloc[0]['author'])
                author_list = author.split(' ')
                if len(author_list) > 1:
                    df_list = []
                    for author_id in author_list:
                        query4 = f'SELECT * FROM Person WHERE PersonId="{author_id}"'
                        df_sql4 = pd.read_sql(query4, con)
                        df_list.append(df_sql4)
                    new_df = pd.concat(df_list, ignore_index=True)
                    return new_df
                else:
                    query4 = f'SELECT * FROM Person WHERE PersonId="{author}"'
                    df_sql4 = pd.read_sql(query4, con)
                    return df_sql4
        con.close()

    def getPublicationsByAuthorName(self, authorPartialName: str):
        with connect(self.dbPath) as con:
            authorPartialName = authorPartialName.lower()
            if len(authorPartialName.split(" ")) > 0:
                names = authorPartialName.split(" ")
                conditions = []
                params = []
                for name in names:
                    conditions.append('(givenName LIKE ? OR familyName LIKE ?)')
                    params.extend(['%' + name + '%', '%' + name + '%'])
                query1 = 'SELECT * FROM Person WHERE ' + ' OR '.join(conditions)
            else:
                query1 = 'SELECT * FROM Person WHERE givenName LIKE ? OR familyName LIKE ?'
                params = ['%' + authorPartialName + '%', '%' + authorPartialName + '%']
            df_sql1 = pd.read_sql(query1, con, params=params)
            list_authors = df_sql1['PersonId'].to_list()
            df_list = []
            for author_id in list_authors:
                query4 = f'SELECT * FROM JournalArticle WHERE author LIKE "%{author_id}%"'
                query5 = f'SELECT * FROM BookChapter WHERE author LIKE "%{author_id}%"'
                query6 = f'SELECT * FROM ProceedingsPaper WHERE author LIKE "%{author_id}%"'
                df_sql4 = pd.read_sql(query4, con)
                df_sql5 = pd.read_sql(query5, con)
                df_sql6 = pd.read_sql(query6, con)
                if not df_sql4.empty:
                    df_list.append(df_sql4)
                if not df_sql5.empty:
                    df_list.append(df_sql5)
                if not df_sql6.empty:
                    df_list.append(df_sql6)
            new_df = pd.concat(df_list, ignore_index=True)
            new_df = new_df.fillna('')
            if 'chapter' in new_df.columns:
                new_df['chapter'] = new_df['chapter'].astype(str).str.replace('.0', '', regex=False)
        con.close()
        return new_df

    def getDistinctPublisherOfPublications(self, pubIdList:list):
        with connect(self.dbPath) as con:
            query1 = 'SELECT publicationVenue FROM JournalArticle WHERE id IN ({});'
            query1 = query1.format(','.join(["'{}'".format(pub_id) for pub_id in pubIdList]))
            query2 = 'SELECT publicationVenue FROM BookChapter WHERE id IN ({});'
            query2 = query2.format(','.join(["'{}'".format(pub_id) for pub_id in pubIdList]))
            query3 = 'SELECT publicationVenue FROM ProceedingsPaper WHERE id IN ({});'
            query3 = query3.format(','.join(["'{}'".format(pub_id) for pub_id in pubIdList]))
            df_sql1 = pd.read_sql(query1, con)
            df_sql2 = pd.read_sql(query2, con)
            df_sql3 = pd.read_sql(query3, con)
            new_df = pd.concat([df_sql1, df_sql2, df_sql3], ignore_index=True)
            venues_set = set(new_df['publicationVenue'])
            venues_list = list(venues_set)
            query4 = 'SELECT publisher FROM Journal WHERE VenueId IN ({});'
            query4 = query4.format(','.join(["'{}'".format(venue_id) for venue_id in venues_list]))
            query5 = 'SELECT publisher FROM Book WHERE VenueId IN ({});'
            query5 = query5.format(','.join(["'{}'".format(venue_id) for venue_id in venues_list]))
            query6 = 'SELECT publisher FROM Proceedings WHERE VenueId IN ({});'
            query6 = query6.format(','.join(["'{}'".format(venue_id) for venue_id in venues_list]))
            df_sql4 = pd.read_sql(query4, con)
            df_sql5 = pd.read_sql(query5, con)
            df_sql6 = pd.read_sql(query6, con)
            df_publisher = pd.concat([df_sql4, df_sql5, df_sql6], ignore_index=True)
            publisher_set = set(df_publisher['publisher'])
            publisher_list = list(publisher_set)
            query7 = 'SELECT * FROM Organization WHERE OrganizationId IN ({});'
            query7 = query7.format(','.join(["'{}'".format(org_id) for org_id in publisher_list]))
            df_sql7 = pd.read_sql(query7, con)

        con.close()
        return df_sql7

    def getOrganization(self, crossref_id):
        with connect(self.dbPath) as con:
            query = f'SELECT OrganizationId, name FROM Organization WHERE OrganizationId="{crossref_id}";'
            df = pd.read_sql(query, con)

        for row_idx, row in df.iterrows():
            organization_obj = Organization(identifier=row["OrganizationId"], name=row["name"])

            con.close()
            return organization_obj

    def getVenue(self, venue_id):
        with connect(self.dbPath) as con:
            query1 = f'SELECT * FROM Journal WHERE VenueId="{venue_id}"'
            query2 = f'SELECT * FROM Book WHERE VenueId="{venue_id}"'
            query3 = f'SELECT * FROM Proceedings WHERE VenueId="{venue_id}"'
            df_sql1 = pd.read_sql(query1, con)
            df_sql2 = pd.read_sql(query2, con)
            df_sql3 = pd.read_sql(query3, con)
            df_sql = pd.concat([df_sql1, df_sql2, df_sql3], ignore_index=True)
            df_sql = df_sql.fillna('')

        for row_idx, row in df_sql.iterrows():
            if "publisher" in row:
                publisher = self.getOrganization(row["publisher"])
                venue_obj = Venue(identifiers=row["VenueId"], title=row["title"], publisher=publisher)

                con.close()
                return venue_obj

    def getAuthor(self, orcid):
        with connect(self.dbPath) as con:
            query = f'SELECT givenName, familyName, PersonId FROM Person WHERE PersonId="{orcid}"'
            df_final = pd.read_sql(query, con)

        for row_idx, row in df_final.iterrows():
            person_obj = Person(identifier=row["PersonId"], givenName=row["givenName"], familyName=row["familyName"])
            con.close()
            return person_obj

    def getPublication(self, identifier):
        with connect(self.dbPath) as con:
            query1 = f'SELECT * FROM JournalArticle WHERE id="{identifier}"'
            query2 = f'SELECT * FROM BookChapter WHERE id="{identifier}"'
            query3 = f'SELECT * FROM ProceedingsPaper WHERE id="{identifier}"'
            df_sql1 = pd.read_sql(query1, con)
            df_sql2 = pd.read_sql(query2, con)
            df_sql3 = pd.read_sql(query3, con)
            df_sql = pd.concat([df_sql1, df_sql2, df_sql3], ignore_index=True)
            df_sql = df_sql.fillna('')

        for row_idx, row in df_sql.iterrows():
            venue_id = row["publicationVenue"].split(" ")[0]
            if venue_id:
                pub_publicationVenue = self.getVenue(venue_id)
            else:
                pub_publicationVenue = ''

            authors = row["author"].split(" ")
            if authors:
                pub_authors = []
                for author in authors:
                    pub_authors.append(self.getAuthor(author))
            else:
                pub_authors = ''

            cited = row["cited"].split(" ")
            if cited:
                pub_cited = []
                for el in cited:
                    pub_cited.append(self.getPublication(el))
            else:
                pub_cited = ''

            publication_obj = Publication(identifier=row['id'], publicationYear=row['publication_year'], title=row['title'], author=pub_authors, publicationVenue=pub_publicationVenue, citedPublications=pub_cited)

            con.close()
            return publication_obj
























