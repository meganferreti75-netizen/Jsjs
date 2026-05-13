import time
import sqlite3
import feedparser
import random
from urllib.parse import quote

# =========================
# BASE DE DATOS LOCAL
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
    tamaño INTEGER
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
            "tamaño": len(getattr(entry, "title", ""))
        })

    return resultados

# =========================
# CONTROL DE DUPLICADOS
# =========================

def cargar_vistos():
    cursor.execute("SELECT link_descarga FROM libros")
    rows = cursor.fetchall()
    return set([r[0] for r in rows if r[0]])

vistos = cargar_vistos()

# =========================
# GUARDAR LIBRO
# =========================

def guardar_libro(libro):
    if not es_valido(libro):
        return False

    if libro["link_descarga"] in vistos:
        return False

    cursor.execute("""
        INSERT INTO libros (tema, nombre, link_descarga, tamaño)
        VALUES (?, ?, ?, ?)
    """, (
        libro["tema"],
        libro["nombre"],
        libro["link_descarga"],
        libro["tamaño"]
    ))

    conn.commit()
    vistos.add(libro["link_descarga"])

    print("GUARDADO:", libro["nombre"])
    return True

# =========================
# PIPELINE
# =========================

def procesar():
    dominio = random.choice(DOMINIOS)

    cursor.execute("INSERT INTO estado (dominio) VALUES (?)", (dominio,))
    conn.commit()

    print("\nDOMINIO:", dominio)

    libros = fetch_arxiv(dominio, max_results=10)

    for libro in libros:
        if guardar_libro(libro):
            print("✔ válido:", libro["nombre"])
        else:
            print("✖ rechazado:", libro["nombre"])

# =========================
# LOOP
# =========================

def agente():
    print("INICIO SISTEMA SQLITE EVOLUCIONADO")

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
    agente()
