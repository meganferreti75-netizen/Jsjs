import time
import sqlite3
import feedparser
import random
import requests
import os
from urllib.parse import quote
from flask import Flask, jsonify
import threading

# =========================
# BASE DE DATOS
# =========================

conn = sqlite3.connect("libros.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("PRAGMA journal_mode=WAL;")
conn.commit()

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
# FLASK APP
# =========================

app = Flask(__name__)

@app.route("/")
def home():
    return "AGENTE ACTIVO"

@app.route("/libros")
def libros():
    cursor.execute("SELECT * FROM libros")
    rows = cursor.fetchall()
    return jsonify(rows)

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

DOMINIO_MAP = {
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

def elegir_fuente(dominio):
    return random.choice(DOMINIO_MAP.get(dominio, ["arxiv"]))

# =========================
# VALIDACIÓN
# =========================

def es_valido(libro):
    if libro["tamaño"] == 0:
        return False
    if not libro["link_descarga"]:
        return False
    if libro["link_descarga"] == "no existente":
        return False
    return True

# =========================
# ARXIV
# =========================

def fetch_arxiv(query, max_results=10):
    query = quote(query)
    url = f"http://export.arxiv.org/api/query?search_query=all:{query}&start=0&max_results={max_results}"
    feed = feedparser.parse(url)

    results = []

    for entry in feed.entries:
        pdf = None

        if hasattr(entry, "links"):
            for l in entry.links:
                if "pdf" in l.get("href", ""):
                    pdf = l.get("href")

        results.append({
            "tema": query,
            "nombre": getattr(entry, "title", ""),
            "link_descarga": pdf,
            "tamaño": len(getattr(entry, "title", "")),
            "fuente": "arxiv"
        })

    return results

# =========================
# SEMANTIC SCHOLAR
# =========================

def fetch_semantic(query, max_results=10):
    url = "https://api.semanticscholar.org/graph/v1/paper/search"

    params = {
        "query": query,
        "limit": max_results,
        "fields": "title,url,openAccessPdf"
    }

    try:
        r = requests.get(url, params=params)
        data = r.json().get("data", [])

        return [
            {
                "tema": query,
                "nombre": e.get("title", ""),
                "link_descarga": (e.get("openAccessPdf") or {}).get("url"),
                "tamaño": len(e.get("title", ""
