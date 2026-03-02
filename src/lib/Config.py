import snowflake.connector

from lib.Variable import Variables


class Config:
    def __init__(self, v: Variables):
        self.v = v
        self.log = v.get("LOG")
        self.USER = self.v.get("user")
        self.PASSWORD = self.v.get("password")
        self.ACCOUNT = self.v.get("account")
        self.DATABASE = self.v.get("database")
        self.SCHEMA = self.v.get("schema")
        self.DATA_WAREHOUSE = self.v.get("warehouse")
        self.ROLE = self.v.get("role")

        ctx = snowflake.connector.connect(
            user=self.USER,
            password=self.PASSWORD,
            account=self.ACCOUNT,
            database=self.DATABASE,
            schema=self.SCHEMA,
            warehouse=self.DATA_WAREHOUSE,
            role=self.ROLE,
            client_telemetry_enabled=False,
        )
        self.cs = ctx.cursor()

    def execute_query(self, query):
        try:
            self.log.message(f"Executing query: {query}")
            self.cs.execute(query)
            val = self.cs.fetchall()
            self.log.message(f"Query Result: {val}")
            return val
        except Exception as e:
            self.log.message(f"query error: {query}")
            self.log.message(f"Error: {e}")

    def executemany(self, query, params):
        try:
            self.log.message(f"Executing query: {query} . Params: {params} ")
            self.cs.executemany(query, params)
            val = self.cs.fetchall()
            self.log.message(f"Query Result: {val}")
            return val
        except Exception as e:
            self.log.message(f"query error: {query}")
            self.log.message(f"Error: {e}")
