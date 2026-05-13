print("VERSION NUEVA CARGADA")
import time
import sqlite3
import feedparser
import requests
import random
import os
from urllib.parse import quote
from flask import Flask, jsonify
import threading

# =========================
# DB
# =========================

conn = sqlite3.connect("libros.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("PRAGMA journal_mode=WAL;")

cursor.execute("""
CREATE TABLE IF NOT EXISTS libros (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tema TEXT,
    nombre TEXT,
    link_descarga TEXT,
    tamaño INTEGER,
    fuente TEXT,
    estado TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS estado (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dominio TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")

conn.commit()

# =========================
# FLASK
# =========================

app = Flask(__name__)

@app.route("/")
def home():
    return "AGENTE ACTIVO"

@app.route("/libros")
def libros():
    cursor.execute("SELECT * FROM libros")
    return jsonify(cursor.fetchall())

# =========================
# DOMINIOS
# =========================

DOMINIOS = [
    "mathematics", "algebra", "geometry", "topology",
    "analysis", "graph theory", "probability",
    "physics", "machine learning", "biology", "chemistry"
]

# =========================
# ROUTER
# =========================

MAP = {
    "mathematics": ["arxiv", "semantic"],
    "algebra": ["arxiv", "semantic"],
    "geometry": ["arxiv", "openalex"],
    "topology": ["arxiv", "openalex"],
    "analysis": ["arxiv", "semantic"],
    "graph theory": ["arxiv", "semantic"],
    "probability": ["semantic", "openalex"],
    "physics": ["arxiv", "semantic"],
    "machine learning": ["semantic", "arxiv"],
    "biology": ["semantic", "openalex"],
    "chemistry": ["semantic", "openalex"]
}

def elegir_fuente(d):
    return random.choice(MAP.get(d, ["arxiv"]))

# =========================
# VALIDACIÓN
# =========================

def valido(l):
    if not l["link"]:
        return False
    if l["tamaño"] == 0:
        return False
    return True

# =========================
# ARXIV
# =========================

def arxiv(q):
    url = f"http://export.arxiv.org/api/query?search_query=all:{quote(q)}&start=0&max_results=10"
    feed = feedparser.parse(url)

    out = []

    for e in feed.entries:
        pdf = None
        for l in getattr(e, "links", []):
            if "pdf" in l.get("href", ""):
                pdf = l["href"]

        out.append({
            "tema": q,
            "nombre": getattr(e, "title", ""),
            "link": pdf,
            "tamaño": len(getattr(e, "title", "")),
            "fuente": "arxiv"
        })

    return out

# =========================
# SEMANTIC
# =========================

def semantic(q):
    try:
        r = requests.get(
            "https://api.semanticscholar.org/graph/v1/paper/search",
            params={
                "query": q,
                "limit": 10,
                "fields": "title,url,openAccessPdf"
            }
        )

        data = r.json().get("data", [])

        return [
            {
                "tema": q,
                "nombre": x.get("title", ""),
                "link": (x.get("openAccessPdf") or {}).get("url"),
                "tamaño": len(x.get("title", "")),
                "fuente": "semantic"
            }
            for x in data
        ]

    except:
        return []

# =========================
# OPENALEX
# =========================

def openalex(q):
    try:
        r = requests.get(
            "https://api.openalex.org/works",
            params={"search": q, "per-page": 10}
        )

        data = r.json().get("results", [])

        return [
            {
                "tema": q,
                "nombre": x.get("display_name", ""),
                "link": None,
                "tamaño": len(x.get("display_name", "")),
                "fuente": "openalex"
            }
            for x in data
        ]

    except:
        return []

# =========================
# VISTOS
# =========================

vistos = set()

# =========================
# STORAGE
# =========================

def guardar(l):
    if not valido(l):
        return False

    if l["link"] in vistos:
        return False

    cursor.execute("""
        INSERT INTO libros (tema, nombre, link_descarga, tamaño, fuente, estado)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        l["tema"],
        l["nombre"],
        l["link"],
        l["tamaño"],
        l["fuente"],
        "ok"
    ))

    conn.commit()
    vistos.add(l["link"])
    return True

# =========================
# PIPELINE
# =========================

def procesar():
    d = random.choice(DOMINIOS)

    cursor.execute("INSERT INTO estado (dominio) VALUES (?)", (d,))
    conn.commit()

    f = elegir_fuente(d)

    if f == "arxiv":
        items = arxiv(d)
    elif f == "semantic":
        items = semantic(d)
    else:
        items = openalex(d)

    for i in items:
        if guardar(i):
            print("GUARDADO:", i["nombre"])
        else:
            print("RECHAZADO:", i["nombre"])

# =========================
# AGENTE
# =========================

def loop():
    while True:
        try:
            procesar()
        except Exception as e:
            print("ERROR:", e)

        time.sleep(20)

# =========================
# ENTRYPOINT
# =========================

if __name__ == "__main__":
    threading.Thread(target=loop, daemon=True).start()

    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
