import duckdb
from flask import Flask, request
from flask_cors import CORS
from smokedduck_lineage import extractMetadata, runQuery, serializeLineage

app = Flask(__name__)
CORS(app)
con = duckdb.connect(database=':memory:', read_only=False)

# Just for testing
con.execute("CALL dbgen(sf=0.001);")

@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'  # Replace '*' with your desired origin or origins
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    return response

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


def DropLineageTables(con):
    tables = con.execute("PRAGMA show_tables").fetchdf()
    # TODO: clear in-mem lineage
    for index, row in tables.iterrows():
        if row["name"][:7] == "LINEAGE":
            print("drop: ", row["name"])
            con.execute("DROP TABLE "+row["name"])

@app.post("/sql")
def execute_sql():
    body = request.json
    query = body['query']
    print(query)
    df, qid, plan = runQuery(con, query, "query")
    extractMetadata(con, qid, plan, 0)
    lineage_json = serializeLineage(qid, plan, 0, {})
    # drop all lineage tables
    #print("*****", lineage_json)
    DropLineageTables(con)
    return lineage_json

@app.post("/csv")
def execute_csv():
    body = request.json
    table_name = body['name']
    csv = body['csv']
    # TODO: parse text as csv and loaded in the database
    print(table_name, csv)
    return ""

@app.post("/schema")
def execute_schema():
    df = con.execute("pragma show_tables").fetchdf()
    tables = [[row['name']] for index, row in df.iterrows()]
    for t in tables:
        df = con.execute("pragma table_info('{}')".format(t[0])).fetchdf()
        df['col'] = df['name'].str.cat(df['type'], sep=':')
        col_string = df['col'].str.cat(sep='; ')
        t.append(col_string)
    return tables

if __name__ == '__main__':
    app.run(debug=True)
