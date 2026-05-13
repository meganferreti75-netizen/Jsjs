import time
import sqlite3
import feedparser
import random
from urllib.parse import quote

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
# DOMINIOS
# =========================

DOMINIOS = [
    "mathematics", "algebra", "geometry", "topology",
    "analysis", "graph theory", "probability",
    "physics", "machine learning", "biology", "chemistry"
]

# =========================
# ROUTER DE FUENTES
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
    opciones = DOMINIO_MAP.get(dominio, ["arxiv"])
    return random.choice(opciones)

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
# FUENTE ARXIV
# =========================

def fetch_arxiv(query, max_results=10):
    query = quote(query)

    url = (
        "http://export.arxiv.org/api/query?"
        f"search_query=all:{query}&start=0&max_results={max_results}"
    )

    feed = feedparser.parse(url)

    resultados = []

    for entry in feed.entries:
        pdf = None

        if hasattr(entry, "links"):
            for l in entry.links:
                if "pdf" in l.get("href", ""):
                    pdf = l.get("href")

        resultados.append({
            "tema": query,
            "nombre": getattr(entry, "title", ""),
            "link_descarga": pdf,
            "tamaño": len(getattr(entry, "title", "")),
            "fuente": "arxiv"
        })

    return resultados

# =========================
# FUENTE SEMANTIC SCHOLAR
# =========================

import requests

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

        results = []

        for e in data:
            results.append({
                "tema": query,
                "nombre": e.get("title", ""),
                "link_descarga": (e.get("openAccessPdf") or {}).get("url"),
                "tamaño": len(e.get("title", "")),
                "fuente": "semantic"
            })

        return results
    except:
        return []

# =========================
# FUENTE OPENALEX
# =========================

def fetch_openalex(query, max_results=10):
    url = "https://api.openalex.org/works"

    params = {
        "search": query,
        "per-page": max_results
    }

    try:
        r = requests.get(url, params=params)
        data = r.json().get("results", [])

        results = []

        for e in data:
            results.append({
                "tema": query,
                "nombre": e.get("display_name", ""),
                "link_descarga": None,
                "tamaño": len(e.get("display_name", "")),
                "fuente": "openalex"
            })

        return results
    except:
        return []

# =========================
# CONTROL DE DUPLICADOS
# =========================

def cargar_vistos():
    cursor.execute("SELECT link_descarga FROM libros")
    rows = cursor.fetchall()
    return set([r[0] for r in rows if r[0]])

vistos = cargar_vistos()

# =========================
# STORAGE
# =========================

def guardar_libro(libro):
    if not es_valido(libro):
        return False

    if libro["link_descarga"] in vistos:
        return False

    cursor.execute("""
        INSERT INTO libros (tema, nombre, link_descarga, tamaño, fuente, estado)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        libro["tema"],
        libro["nombre"],
        libro["link_descarga"],
        libro["tamaño"],
        libro["fuente"],
        "validado"
    ))

    conn.commit()
    vistos.add(libro["link_descarga"])
    return True

# =========================
# PIPELINE
# =========================

def procesar():
    dominio = random.choice(DOMINIOS)

    cursor.execute("INSERT INTO estado (dominio) VALUES (?)", (dominio,))
    conn.commit()

    fuente = elegir_fuente(dominio)

    print("\nDOMINIO:", dominio)
    print("FUENTE:", fuente)

    if fuente == "arxiv":
        items = fetch_arxiv(dominio)
    elif fuente == "semantic":
        items = fetch_semantic(dominio)
    else:
        items = fetch_openalex(dominio)

    for libro in items:
        if guardar_libro(libro):
            print("GUARDADO:", libro["nombre"], "|", fuente)
        else:
            print("RECHAZADO:", libro["nombre"])

# =========================
# LOOP
# =========================

def agente():
    print("SISTEMA MULTIFUENTE INICIADO")

    while True:
        try:
            procesar()
        except Exception as e:
            print("ERROR:", str(e))

        time.sleep(20)

# =========================
# ENTRYPOINT
# =========================

if __name__ == "__main__":
    try:
        agente()
    except Exception as e:
        print("FATAL ERROR:", str(e))
