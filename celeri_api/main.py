from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import mysql.connector
from datetime import date, datetime, timedelta
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
def get_loue(jour: str):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT statut FROM airbnb_loue WHERE jour = %s", (jour,))
        row = cursor.fetchone()

        if row:
            statut = bool(row[0])
        else:
            # Créer une entrée par défaut si elle n'existe pas
            cursor.execute(
                "INSERT INTO airbnb_loue (jour, statut) VALUES (%s, %s)",
                (jour, False)
            )
            conn.commit()
            statut = False

        return {"jour": jour, "statut": statut}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


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
def update_loue(jour: str, payload: dict):
    statut = payload.get("statut", False)
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Vérifie si la date existe déjà
        cursor.execute("SELECT COUNT(*) FROM airbnb_loue WHERE jour = %s", (jour,))
        exists = cursor.fetchone()[0] > 0

        if exists:
            # Met à jour l'entrée existante
            cursor.execute(
                "UPDATE airbnb_loue SET statut = %s WHERE jour = %s",
                (statut, jour)
            )
        else:
            # Crée une nouvelle entrée
            cursor.execute(
                "INSERT INTO airbnb_loue (jour, statut) VALUES (%s, %s)",
                (jour, statut)
            )

        conn.commit()
        return {"message": "Statut mis à jour", "jour": jour, "statut": statut}

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.post("/loue/init")
def init_dates(data: dict):
    try:
        start = datetime.strptime(data["start"], "%Y-%m-%d").date()
        end = datetime.strptime(data["end"], "%Y-%m-%d").date()
        statut = bool(data.get("statut", False))

        if end < start:
            raise HTTPException(status_code=400, detail="end date must be after start date")

        conn = get_connection()
        cursor = conn.cursor()

        current = start
        while current <= end:
            cursor.execute(
                """
                INSERT INTO airbnb_loue (jour, statut)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE statut = VALUES(statut)
                """,
                (current, statut)
            )
            current += timedelta(days=1)

        conn.commit()
        cursor.close()
        conn.close()
        return {"status": "ok", "message": f"{(end - start).days + 1} days processed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
