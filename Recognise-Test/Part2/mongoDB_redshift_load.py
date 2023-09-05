import pymongo
import redshift


##This class creates a Mongo DB connection
class MongoClient:
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    def connect(self):
        return pymongo.MongoClient(self.host, self.port, username=self.username, password=self.password)

##This class creates a Redshift connection
class RedshiftConnection:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    def connect(self):
        return redshift.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )

##This class creates drop, create, copy table work
class MongoToRedshift:
    def __init__(self, mongo_client, redshift_connection):
        self.mongo_client = mongo_client
        self.redshift_connection = redshift_connection

    def _drop_table(self, table_name):
        self.redshift_connection.execute(f"DROP TABLE IF EXISTS {table_name}")

    def _create_table(self, table_name, schema):
        self.redshift_connection.execute(f"""
            CREATE TABLE {table_name} (
                {schema}
            )
        """)

    def _copy_data(self, table_name, mongo_cursor):
        for document in mongo_cursor:
            self.redshift_connection.execute(
                f"""
                INSERT INTO {table_name} (
                    {",".join(document.keys())}
                )
                VALUES (
                    {",".join([str(value) for value in document.values()])}
                )
            """
            )

    def run(self, mongo_database, mongo_collection, redshift_table):
        mongo_cursor = self.mongo_client[mongo_database][mongo_collection].find()

        self._drop_table(redshift_table)
        self._create_table(redshift_table, mongo_cursor.first().keys())
        self._copy_data(redshift_table, mongo_cursor)


if __name__ == "__main__":
    mongo_client = MongoClient("mongodb://localhost:27017")
    redshift_connection = RedshiftConnection(
        "my-redshift-cluster.amazonaws.com", 5439, "my_username", "my_password", "my_database"
    )
    mongo_to_redshift = MongoToRedshift(mongo_client, redshift_connection)
    mongo_to_redshift.run("my_database", "my_collection", "my_table")
