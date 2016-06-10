import contextlib
import io
import os
import selectors
import subprocess

from collections import namedtuple

import psycopg2
import requests

from flask import Flask, request, jsonify


app = Flask(__name__)


DB_HOST = os.environ.get("DB_HOST", "localhost")
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
APP_USER = os.environ.get("APP_USER", "app")
APP_PASSWORD = os.environ["APP_PASSWORD"]
APP_DB = os.environ.get("APP_DB", APP_USER)


def db_conn():
    return psycopg2.connect(host=DB_HOST, dbname="postgres", user="postgres", password=POSTGRES_PASSWORD)


@app.route("/")
def index():
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT row_to_json(row(pg_postmaster_start_time()))")
        return jsonify(cur.fetchone()[0])


@app.route("/cleardb")
def cleardb():
    conn = db_conn()
    conn.autocommit = True
    try:
        cur = conn.cursor()
        close_connections(cur)
        recreate_db(cur)
    finally:
        conn.close()
    return jsonify({"status": "ok"})


@contextlib.contextmanager
def pgpass(**ctx):
    pgpass = os.path.expanduser("~/.pgpass")
    with open(pgpass, "wb") as fp:
        fp.write("{host}:5432:{db}:{user}:{password}\n".format(**ctx).encode("utf-8"))
    os.chmod(pgpass, 0o0600)
    yield namedtuple("Creds", "host dbname username")(
        host=ctx["host"],
        dbname=ctx["db"],
        username=ctx["user"],
    )
    os.unlink(pgpass)


@app.route("/load", methods=["POST"])
def load():
    r = requests.get(request.form["url"], stream=True)
    r.raise_for_status()
    params = {
        "url": request.form["url"],
        "content_type": r.headers["content-type"],
    }
    if params["content_type"] == "text/plain":
        with pgpass(host=DB_HOST, db=APP_DB, user=APP_USER, password=APP_PASSWORD) as creds:
            returncode, out = subprocess_io(
                [
                    "psql",
                    "--host={}".format(creds.host),
                    "--username={}".format(creds.username),
                    "--no-password",
                    "--dbname={}".format(creds.dbname),
                    "--file=-",
                    "--quiet",
                ],
                stdin=r,
            )
    elif params["content_type"] == "binary/octet-stream":
        with pgpass(host=DB_HOST, db=APP_DB, user=APP_USER, password=APP_PASSWORD) as creds:
            returncode, out = subprocess_io(
                [
                    "pg_restore",
                    "--host={}".format(creds.host),
                    "--username={}".format(creds.username),
                    "--no-password",
                    "--dbname={}".format(creds.dbname),
                    "--role=app",
                    "--no-owner",
                    "--no-acl",
                ],
                stdin=r,
            )
    else:
        return jsonify({
            "status": "error",
            "reason": "unknown content type",
            "params": params,
        })
    return jsonify({
        "status": "ok",
        "params": params,
        "result": {
            "returncode": returncode,
            "out": out
        }
    })


def subprocess_io(args, stdin):
    p = subprocess.Popen(
        args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    chunks = stdin.iter_content(32 * 1024)
    with io.StringIO() as out:
        with selectors.DefaultSelector() as sel:
            sel.register(p.stdin, selectors.EVENT_WRITE)
            sel.register(p.stdout, selectors.EVENT_READ)
            sel.register(p.stderr, selectors.EVENT_READ)
            while sel.get_map():
                ready = sel.select()
                for key, events in ready:
                    if key.fileobj is p.stdin:
                        try:
                            os.write(key.fd, next(chunks))
                        except (BrokenPipeError, StopIteration):
                            sel.unregister(key.fileobj)
                            key.fileobj.close()
                    elif key.fileobj in (p.stdout, p.stderr):
                        data = os.read(key.fd, 32768)
                        if not data:
                            sel.unregister(key.fileobj)
                            key.fileobj.close()
                        out.write(data.decode("utf-8"))
        p.wait()
        return p.returncode, out.getvalue()


def close_connections(cur):
    cur.execute('REVOKE CONNECT ON DATABASE "{db}" FROM public'.format(db=APP_DB))
    cur.execute('ALTER DATABASE {db} CONNECTION LIMIT 0'.format(db=APP_DB))
    cur.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname='{db}'".format(db=APP_DB))


def recreate_db(cur):
    cur.execute('DROP DATABASE "{db}"'.format(db=APP_DB))
    cur.execute('CREATE DATABASE "{db}" WITH OWNER "{user}"'.format(db=APP_DB, user=APP_USER))


if __name__ == "__main__":
    app.run()
