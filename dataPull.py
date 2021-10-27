import psycopg2
import os
import pandas.io.sql as sqlio
from flask import Flask, Response

app = Flask(__name__)

"""
Setting up postgres credentials.
"""
postgres_config = {
    "host": os.environ.get("DB_HOST"),
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_RO_USER"),
    "password": os.environ.get("DB_RO_USER_PW"),
}

"""
Creates the connection with Postgres database.
"""
conn = psycopg2.connect(
    "host="
    + postgres_config["host"]
    + " dbname="
    + postgres_config["dbname"]
    + " user="
    + postgres_config["user"]
    + " password="
    + postgres_config["password"]
)

"""
This uses the connection with the database to run the specified query.
"""


def get_view(schema, view, conn):
    sql = f"select * from {schema}.{schema}_{view}_vw"
    dat = sqlio.read_sql_query(sql, conn)
    conn = None
    return dat


"""
This provides a dynamic end point for returning the data based on the requested
schema and the view sought for said schema. URL filled out should look
something like ".../<schema_name>_<view_name>". Make sure to remove brackets.
"""


@app.route("/<schm>_<vw>")
def get_board(schm, vw):
    frejm = get_view(schema=schm, view=vw, conn=conn)
    return Response(
        frejm.to_csv(encoding="utf-8", index=False),
        mimetype="text/csv",
        headers={"Content-disposition": f"attachment; filename={schm}_{vw}.csv"},
    )


if __name__ == "__main__":
    app.run()
