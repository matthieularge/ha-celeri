from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import mysql.connector
from datetime import date
from fastapi.responses import JSONResponse
import json

def load_config():
    with open("/data/options.json") as f:
        return json.load(f)

config = load_config()

DB_CONFIG = {
    "host": config["DB_HOST"],
    "user": config["DB_USER"],
    "password": config["DB_PASSWORD"],
    "database": config["DB_NAME"]
}

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello from Celeri addon"}

def get_connection():
    return mysql.connector.connect(**DB_CONFIG)    

class LoueEntry(BaseModel):
    jour: date
    loue: bool

@app.get("/loue/{jour}")
def read_loue(jour: date):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT loue FROM airbnb_loue WHERE jour = %s", (jour,))
    result = cursor.fetchone()
    conn.close()
    if result is None:
        raise HTTPException(status_code=404, detail="Date non trouvée")
    return {"jour": jour, "loue": bool(result[0])}

@app.post("/loue")
def add_loue(entry: LoueEntry):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO airbnb_loue (jour, loue) VALUES (%s, %s)", (entry.jour, entry.loue))
        conn.commit()
    except mysql.connector.IntegrityError:
        raise HTTPException(status_code=409, detail="Date déjà existante")
    finally:
        conn.close()
    return {"message": "Ajouté"}

@app.put("/loue/{jour}")
def update_loue(jour: date, entry: LoueEntry):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE airbnb_loue SET loue = %s WHERE jour = %s", (entry.loue, jour))
    conn.commit()
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Date non trouvée")
    conn.close()
    return {"message": "Modifié"}
