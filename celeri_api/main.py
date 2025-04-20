import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import mysql.connector
from datetime import date, datetime, timedelta
from fastapi.responses import JSONResponse
import json
from ics import Calendar

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

AIRBNB_CAL_URL = "https://www.airbnb.fr/calendar/ical/32053854.ics?s=bee9bbc3a51315a4fa27ea2a09621aef"


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
    logger.info(f"🔎 GET /loue/{jour}")
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT loue FROM airbnb_loue WHERE jour = %s", (jour,))
        row = cursor.fetchone()

        if row:
            loue = bool(row[0])
            logger.debug(f"✔️ Date trouvée: {jour} => loue={loue}")
        else:
            logger.warning(f"❗ Date {jour} absente — ajout avec loue=False")
            cursor.execute(
                "INSERT INTO airbnb_loue (jour, loue) VALUES (%s, %s)",
                (jour, False)
            )
            conn.commit()
            loue = False

        return {"jour": jour, "loue": loue}

    except Exception as e:
        logger.error(f"❌ Erreur GET /loue/{jour}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.post("/loue")
def add_loue(entry: LoueEntry):
    logger.info(f"➕ POST /loue : {entry}")
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            "INSERT INTO airbnb_loue (jour, loue) VALUES (%s, %s)",
            (entry.jour, entry.loue)
        )
        conn.commit()
        logger.debug("✔️ Entrée ajoutée")
        return {"message": "Ajouté"}
    except mysql.connector.IntegrityError:
        logger.warning("⚠️ Date déjà existante")
        raise HTTPException(status_code=409, detail="Date déjà existante")
    except Exception as e:
        logger.error(f"❌ Erreur POST /loue : {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.put("/loue/{jour}")
def update_loue(jour: str, payload: dict):
    logger.info(f"🛠️ PUT /loue/{jour} : {payload}")
    loue = payload.get("loue", False)
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM airbnb_loue WHERE jour = %s", (jour,))
        exists = cursor.fetchone()[0] > 0

        if exists:
            cursor.execute(
                "UPDATE airbnb_loue SET loue = %s WHERE jour = %s",
                (loue, jour)
            )
            logger.debug("🔄 Mise à jour effectuée")
        else:
            cursor.execute(
                "INSERT INTO airbnb_loue (jour, loue) VALUES (%s, %s)",
                (jour, loue)
            )
            logger.debug("➕ Ajout effectué")

        conn.commit()
        return {"message": "Mise à jour effectuée", "jour": jour, "loue": loue}
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Erreur PUT /loue/{jour} : {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


def is_reserved(cal_url: str, check_date: date) -> bool:
    try:
        response = requests.get(cal_url)
        response.raise_for_status()
        calendar = Calendar(response.text)

        for event in calendar.events:
            if event.name == "Reserved":
                start_date = event.begin.date()
                end_date = event.end.date()
                if start_date <= check_date < end_date:
                    return True
        return False
    except Exception as e:
        print(f"Erreur lors de la lecture du calendrier Airbnb : {e}")
        return False

def upsert_loue_date(cursor, jour: date, loue: bool):
    cursor.execute(
        """
        INSERT INTO airbnb_loue (jour, loue)
        VALUES (?, ?)
        ON DUPLICATE KEY UPDATE loue = VALUES(loue)
        """,
        (jour.isoformat(), loue)
    )


@app.post("/loue_sync_calendar")
def loue_sync_calendar():
    try:
        conn = get_connection()
        cur = conn.cursor()

        today = date.today()
        tomorrow = today + timedelta(days=1)

        for check_date in [today, tomorrow]:
            reserved = is_reserved(AIRBNB_CAL_URL, check_date)
            upsert_loue_date(cur, check_date, reserved)

        conn.commit()
        return {"status": "success", "message": "Synchronisation terminée."}

    except mariadb.Error as e:
        print(f"Erreur MariaDB : {e}")
        raise HTTPException(status_code=500, detail="Erreur base de données")

    finally:
        if conn:
            conn.close()
            

@app.post("/loue/init")
def init_dates(data: dict):
    logger.info(f"📅 POST /loue/init : {data}")
    try:
        start = datetime.strptime(data["start"], "%Y-%m-%d").date()
        end = datetime.strptime(data["end"], "%Y-%m-%d").date()
        loue = bool(data.get("loue", True))
        weekend = bool(data.get("weekend", False))  

        if end < start:
            logger.warning("⛔ Date de fin antérieure à la date de début")
            raise HTTPException(status_code=400, detail="end date must be after start date")

        conn = get_connection()
        cursor = conn.cursor()

        current = start
        count = 0
        while current <= end:
            is_weekend = current.weekday() >= 5  # 5 = Saturday, 6 = Sunday
            statut = weekend if is_weekend else loue
            cursor.execute(
                """
                INSERT INTO airbnb_loue (jour, loue)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE loue = VALUES(loue)
                """,
                (current, statut)
            )
            current += timedelta(days=1)
            count += 1

        conn.commit()
        logger.info(f"✅ {count} jours traités entre {start} et {end}")
        return {"status": "ok", "message": f"{count} jours traités"}
    except Exception as e:
        logger.error(f"❌ Erreur dans /loue/init : {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
