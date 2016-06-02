import psycopg2

from flask import Flask, jsonify


app = Flask(__name__)


@app.route("/")
def index():
    conn = psycopg2.connect(host="localhost", dbname="postgres", user="postgres")
    cur = conn.cursor()
    cur.execute("SELECT row_to_json(row(pg_postmaster_start_time()))")
    return jsonify(cur.fetchone()[0])


if __name__ == "__main__":
    app.run()
