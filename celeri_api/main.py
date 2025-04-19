import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import mysql.connector
from datetime import date, datetime, timedelta
from fastapi.responses import JSONResponse
import json

# -----------------------------
# Logging configuration
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# -----------------------------
# Load DB config
# -----------------------------
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

from fastapi.requests import Request

@app.middleware("http")
async def log_requests(request: Request, call_next):
    ip = request.client.host
    path = request.url.path
    method = request.method

    logger.info(f"🔹 {method} request to {path} from {ip}")

    try:
        response = await call_next(request)
        return response
    except Exception as e:
        logger.error(f"❌ Error during {method} {path} from {ip}: {e}")
        raise
        

@app.get("/")
def read_root():
    logger.info("GET / called")
    return {"message": "Hello from Celeri addon"}

def get_connection():
    logger.debug("Creating new database connection")
    return mysql.connector.connect(**DB_CONFIG)    

class LoueEntry(BaseModel):
    jour: date
    loue: bool

@app.get("/loue/{jour}")
def get_loue(jour: str):
    logger.info(f"GET /loue/{jour} called")
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT loue FROM airbnb_loue WHERE jour = %s", (jour,))
        row = cursor.fetchone()

        if row:
            statut = bool(row[0])
            logger.info(f"Entry found for {jour}: statut={statut}")
        else:
            logger.info(f"No entry for {jour}, inserting default (False)")
            cursor.execute(
                "INSERT INTO airbnb_loue (jour, loue) VALUES (%s, %s)",
                (jour, False)
            )
            conn.commit()
            statut = False

        return {"jour": jour, "statut": statut}

    except Exception as e:
        logger.error(f"Error in get_loue: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.post("/loue")
def add_loue(entry: LoueEntry):
    logger.info(f"POST /loue called with jour={entry.jour}, loue={entry.loue}")
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO airbnb_loue (jour, loue) VALUES (%s, %s)", (entry.jour, entry.loue))
        conn.commit()
        logger.info("Entry added successfully.")
    except mysql.connector.IntegrityError as e:
        logger.warning(f"IntegrityError on insert: {e}")
        raise HTTPException(status_code=409, detail="Date déjà existante")
    except Exception as e:
        logger.error(f"Error in add_loue: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
    return {"message": "Ajouté"}

@app.put("/loue/{jour}")
def update_loue(jour: str, payload: dict):
    logger.info(f"PUT /loue/{jour} with payload={payload}")
    statut = payload.get("statut", False)
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM airbnb_loue WHERE jour = %s", (jour,))
        exists = cursor.fetchone()[0] > 0

        if exists:
            logger.info(f"Updating existing entry for {jour}")
            cursor.execute(
                "UPDATE airbnb_loue SET loue = %s WHERE jour = %s",
                (statut, jour)
            )
        else:
            logger.info(f"Inserting new entry for {jour}")
            cursor.execute(
                "INSERT INTO airbnb_loue (jour, loue) VALUES (%s, %s)",
                (jour, statut)
            )

        conn.commit()
        return {"message": "Statut mis à jour", "jour": jour, "statut": statut}

    except Exception as e:
        conn.rollback()
        logger.error(f"Error in update_loue: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.post("/loue/init")
def init_dates(data: dict):
    logger.info(f"POST /loue/init with data={data}")
    try:
        start = datetime.strptime(data["start"], "%Y-%m-%d").date()
        end = datetime.strptime(data["end"], "%Y-%m-%d").date()
        statut = bool(data.get("statut", False))

        if end < start:
            logger.warning("End date is before start date.")
            raise HTTPException(status_code=400, detail="end date must be after start date")

        conn = get_connection()
        cursor = conn.cursor()

        count = 0
        current = start
        while current <= end:
            cursor.execute(
                """
                INSERT INTO airbnb_loue (jour, loue)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE loue = VALUES(statut)
                """,
                (current, statut)
            )
            current += timedelta(days=1)
            count += 1

        conn.commit()
        logger.info(f"{count} jours insérés ou mis à jour entre {start} et {end}")
        return {"status": "ok", "message": f"{count} days processed"}

    except Exception as e:
        logger.error(f"Error in init_dates: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass
