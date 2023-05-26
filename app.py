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

@app.post("/sql")
def execute_sql():
    body = request.json
    query = body['query']
    df, qid, plan = runQuery(query, "query")
    print("\noutput:\n", df)
    print("\nPlan:\n", plan)
    extractMetadata(qid, plan, 0)
    lineage_json = serializeLineage(qid, plan, 0, {})
    return lineage_json

if __name__ == '__main__':
    app.run(debug=True)
