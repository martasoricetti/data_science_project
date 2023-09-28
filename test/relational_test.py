import os.path
import unittest
from impl import *
from os.path import join
import sqlite3

class TestRelational(unittest.TestCase):

    def setUp(self) -> None:
        self.test_dir = join("test", "data_for_testing")
        self.relational_data = join(self.test_dir, "relational_data")
        self.relational_csv = join(self.relational_data, "relational_publications.csv")
        self.relational_json = join(self.relational_data, "relational_other_data.json")
        self.relational_db = join(self.relational_data, "publications.db")
        self.data_test_most_cited = join(self.test_dir, "relational_data_one_most_cited")
        self.csv_most_cited = join(self.data_test_most_cited, "relational_publications.csv")
        self.json_most_cited = join(self.data_test_most_cited, "relational_other_data.json")

    def uploadData(self):
        rel_dp = RelationalDataProcessor()
        rel_dp.setDbPath(self.relational_db)
        rel_dp.uploadData(self.relational_csv)
        rel_dp.uploadData(self.relational_json)

    def instantiateRelQP(self):
        if os.path.exists(self.relational_db):
            rel_qp = RelationalQueryProcessor()
            rel_qp.setDbPath(self.relational_db)
            return rel_qp

    def test_uploadData(self):

        self.uploadData()

        # Create a SQL connection to our SQLite database
        con = sqlite3.connect(self.relational_db)

        # creating cursor
        cur = con.cursor()

        # reading all table names
        cur.execute("SELECT name FROM sqlite_master WHERE type = 'table';")
        table_tuples = cur.fetchall()
        table_names = [row[0] for row in table_tuples]
        expected_names = ['Person', 'Organization', 'Journal', 'Book', 'Proceedings', 'JournalArticle', 'BookChapter',
                          'ProceedingsPaper', 'Citations']
        self.assertTrue(set(table_names) == set(expected_names))

        # check that just the table Proceedings is empty
        empty_tables = []
        for table in table_tuples:
            table_name = table[0]

            cur.execute(f"SELECT COUNT(*) FROM {table_name};")

            row_count = cur.fetchone()[0]
            if row_count == 0:
                empty_tables.append(table_name)
        self.assertTrue(len(empty_tables) ==0)

        # Be sure to close the connection
        con.close()

        os.remove(self.relational_db)

    def test_getPublicationsPublishedInYear(self):
        self.uploadData()

        rel_qp = self.instantiateRelQP()
        df = rel_qp.getPublicationsPublishedInYear(2020)
        dois = list(df['id'])
        expected_dois = ['doi:10.1007/978-3-030-61244-3_6', 'doi:10.1038/s41597-020-00749-y', 'doi:10.1162/qss_a_00023']
        self.assertTrue(set(dois) == set(expected_dois))

        # no publications with publication_year = 1999
        df2 = rel_qp.getPublicationsPublishedInYear(1999)
        dois_df2 = list(df2['id'])
        self.assertTrue(len(dois_df2) == 0)

        os.remove(self.relational_db)

    def test_getPublicationsByAuthorId(self):
        self.uploadData()

        rel_qp = self.instantiateRelQP()
        df = rel_qp.getPublicationsByAuthorId('0000-0001-7814-8951')
        dois = list(df['id'])
        expected_dois = ["doi:10.1038/s41597-020-00749-y", "doi:10.3390/proceedings2023086045"]
        self.assertTrue(set(dois) == set(expected_dois))

        os.remove(self.relational_db)

    def test_getMostCitedPublication_one_most_cited(self):
        rel_dp = RelationalDataProcessor()
        rel_dp.setDbPath(self.relational_db)
        rel_dp.uploadData(self.csv_most_cited)
        rel_dp.uploadData(self.json_most_cited)

        rel_qp = self.instantiateRelQP()
        df = rel_qp.getMostCitedPublication()
        dois = list(df['id'])
        expected_dois = ["doi:10.1162/qss_a_00023"]
        self.assertTrue(set(dois) == set(expected_dois))

        os.remove(self.relational_db)

    def test_getMostCitedPublication_more_most_cited(self):
        self.uploadData()
        rel_qp = self.instantiateRelQP()
        df = rel_qp.getMostCitedPublication()
        dois = list(df['id'])
        expected_dois = ["doi:10.1162/qss_a_00023", "doi:10.1371/journal.pbio.3000385", "doi:10.1162/qss_a_00146"]
        self.assertTrue(set(dois) == set(expected_dois))

        os.remove(self.relational_db)

    def test_getMostCitedVenue(self):
        self.uploadData()
        rel_qp = self.instantiateRelQP()
        df = rel_qp.getMostCitedVenue()
        venues_id = list(df['VenueId'])
        expected_venues_id = ['issn:2641-3337']
        self.assertEqual(venues_id, expected_venues_id)

        os.remove(self.relational_db)

    def test_getVenuesByPublisherId(self):
        self.uploadData()
        rel_qp = self.instantiateRelQP()
        df = rel_qp.getVenuesByPublisherId('crossref:297')
        venues_id = list(df['VenueId'])
        expected_venues_id = ["isbn:9783030612436", "isbn:9783030612443", "issn:2052-4463"]

        self.assertTrue(set(venues_id) == set(expected_venues_id))

        os.remove(self.relational_db)

    def test_getPublicationInVenue(self):
        self.uploadData()
        rel_qp = self.instantiateRelQP()
        df = rel_qp.getPublicationInVenue("isbn:9783030612443")
        dois = list(df['id'])
        expected_dois = ["doi:10.1007/978-3-030-61244-3_6"]
        self.assertEqual(dois, expected_dois)

        os.remove(self.relational_db)

















