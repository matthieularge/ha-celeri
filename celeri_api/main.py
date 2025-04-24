import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import mysql.connector
from datetime import date, datetime, timedelta
from fastapi.responses import JSONResponse
import json
from ics import Calendar
import requests
from enum import Enum
from typing import Optional

# TODO
# laisser des traces uniquement lorsque on insère des datas (et pas pour les capteurs) : ca permettra de debug Presence / Teletravail
# raccourcis qui alimentent Presence (gaffe syncro VB) et Teletravail : a tester car visibkement KO, puis reste à supprimer les appels vers ancienne webapp
# supp Homebridge et déplacer les automatismes vers HA (gafef condition coté Home / Eve)
# scrypted
# actual budget sur ex pi4
# icloud docker et dnla sur ex pi4

# plus tard :
# HTTPS depuis application mobile, réseau local, réseau externe : configurer Home Assistant en HTTPS mais sur un port différent de 443 ? OUI
# stats sur les datas capteurs et rapport : api qui renvoie les resultats de selects à afficher dans une zone HA ?
# repo GitHub en privé


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

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

    logger.debug(f"🔹 {method} request to {path} from {ip}")

    try:
        response = await call_next(request)
        return response
    except Exception as e:
        logger.error(f"❌ Error during {method} {path} from {ip}: {e}")
        raise
        

@app.get("/")
def read_root():
    logger.debug("GET / called")
    return {"message": "Hello from Celeri addon"}

def get_connection():
    logger.debug("Creating new database connection")
    return mysql.connector.connect(**DB_CONFIG)    



@app.get("/presence/{jour}")
def get_presence(jour: str):
    logger.debug(f"🔎 GET /presence/{jour}")
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT presence FROM presence WHERE jour = %s", (jour,))
        row = cursor.fetchone()

        if row:
            presence = bool(row[0])
            logger.debug(f"✔️ Date trouvée: {jour} => presence={presence}")
        else:
            logger.info(f"❗ Date {jour} absente — ajout avec presence=False")
            cursor.execute(
                "INSERT INTO presence (jour, presence) VALUES (%s, %s)",
                (jour, False)
            )
            conn.commit()
            presence = False

        return {"jour": jour, "presence": presence}

    except Exception as e:
        logger.error(f"❌ Erreur GET /presence/{jour}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.put("/presence/{jour}")
def update_presence(jour: str, payload: dict):
    logger.debug(f"🛠️ PUT /presence/{jour} : {payload}")
    presence = payload.get("presence", False)
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM presence WHERE jour = %s", (jour,))
        exists = cursor.fetchone()[0] > 0

        if exists:
            cursor.execute(
                "UPDATE presence SET presence = %s WHERE jour = %s",
                (presence, jour)
            )
            logger.info(f"✔️ Date mise à jour : {jour} => presence={presence}")
        else:
            cursor.execute(
                "INSERT INTO presence (jour, presence) VALUES (%s, %s)",
                (jour, presence)
            )
            logger.info(f"➕ Date absente: {jour} => presence={presence}")

        conn.commit()
        return {"message": "Mise à jour effectuée", "jour": jour, "presence": presence}
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Erreur PUT /presence/{jour} : {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.get("/teletravail/{jour}")
def get_teletravail(jour: str):
    logger.debug(f"🔎 GET /teletravail/{jour}")
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT teletravail FROM teletravail WHERE jour = %s", (jour,))
        row = cursor.fetchone()

        if row:
            teletravail = bool(row[0])
            logger.debug(f"✔️ Date trouvée: {jour} => teletravail={teletravail}")
        else:
            logger.info(f"❗ Date {jour} absente — ajout avec teletravail=False")
            cursor.execute(
                "INSERT INTO teletravail (jour, teletravail) VALUES (%s, %s)",
                (jour, False)
            )
            conn.commit()
            teletravail = False

        return {"jour": jour, "teletravail": teletravail}

    except Exception as e:
        logger.error(f"❌ Erreur GET /teletravail/{jour}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.put("/teletravail/{jour}")
def update_teletravail(jour: str, payload: dict):
    logger.debug(f"🛠️ PUT /teletravail/{jour} : {payload}")
    teletravail = payload.get("teletravail", False)
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM teletravail WHERE jour = %s", (jour,))
        exists = cursor.fetchone()[0] > 0

        if exists:
            cursor.execute(
                "UPDATE teletravail SET teletravail = %s WHERE jour = %s",
                (teletravail, jour)
            )
            logger.info(f"✔️ Date mise à jour : {jour} => teletravail={teletravail}")
        else:
            cursor.execute(
                "INSERT INTO teletravail (jour, teletravail) VALUES (%s, %s)",
                (jour, teletravail)
            )
            logger.info(f"➕ Date absente: {jour} => teletravail={teletravail}")

        conn.commit()
        return {"message": "Mise à jour effectuée", "jour": jour, "teletravail": teletravail}
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Erreur PUT /teletravail/{jour} : {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.get("/cheminee/{jour}")
def get_cheminee(jour: str):
    logger.debug(f"🔎 GET /cheminee/{jour}")
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT cheminee FROM cheminee WHERE jour = %s", (jour,))
        row = cursor.fetchone()

        if row:
            cheminee = bool(row[0])
            logger.debug(f"✔️ Date trouvée: {jour} => cheminee={cheminee}")
        else:
            logger.info(f"❗ Date {jour} absente — ajout avec cheminee=False")
            cursor.execute(
                "INSERT INTO cheminee (jour, cheminee) VALUES (%s, %s)",
                (jour, False)
            )
            conn.commit()
            cheminee = False

        return {"jour": jour, "cheminee": cheminee}

    except Exception as e:
        logger.error(f"❌ Erreur GET /cheminee/{jour}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.put("/cheminee/{jour}")
def update_cheminee(jour: str, payload: dict):
    logger.debug(f"🛠️ PUT /cheminee/{jour} : {payload}")
    cheminee = payload.get("cheminee", False)
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM cheminee WHERE jour = %s", (jour,))
        exists = cursor.fetchone()[0] > 0

        if exists:
            cursor.execute(
                "UPDATE cheminee SET cheminee = %s WHERE jour = %s",
                (cheminee, jour)
            )
            logger.info(f"✔️ Date mise à jour : {jour} => cheminee={cheminee}")
        else:
            cursor.execute(
                "INSERT INTO cheminee (jour, cheminee) VALUES (%s, %s)",
                (jour, cheminee)
            )
            logger.info(f"➕ Date absente: {jour} => cheminee={cheminee}")

        conn.commit()
        return {"message": "Mise à jour effectuée", "jour": jour, "cheminee": cheminee}
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Erreur PUT /cheminee/{jour} : {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


class LoueEntry(BaseModel):
    jour: date
    loue: bool

@app.get("/loue/{jour}")
def get_loue(jour: str):
    logger.debug(f"🔎 GET /loue/{jour}")
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT loue FROM airbnb_loue WHERE jour = %s", (jour,))
        row = cursor.fetchone()

        if row:
            loue = bool(row[0])
            logger.debug(f"✔️ Date trouvée: {jour} => loue={loue}")
        else:
            logger.info(f"❗ Date {jour} absente — ajout avec loue=False")
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
    logger.debug(f"➕ POST /loue : {entry}")
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            "INSERT INTO airbnb_loue (jour, loue) VALUES (%s, %s)",
            (entry.jour, entry.loue)
        )
        conn.commit()
        logger.info(f"➕ Date absente: {entry.jour} => loue={entry.loue}")
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
    logger.debug(f"🛠️ PUT /loue/{jour} : {payload}")
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
            logger.info(f"✔️ Date mise à jour : {jour} => loue={loue}")
        else:
            cursor.execute(
                "INSERT INTO airbnb_loue (jour, loue) VALUES (%s, %s)",
                (jour, loue)
            )
            logger.info(f"➕ Date absente: {jour} => loue={loue}")

        conn.commit()
        return {"message": "Mise à jour effectuée", "jour": jour, "loue": loue}
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Erreur PUT /loue/{jour} : {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@app.post("/loue_sync_calendar")
def loue_sync_calendar():
    try:
        conn = get_connection()
        cur = conn.cursor()

        today = date.today()
        tomorrow = today + timedelta(days=1)

        for check_date in [today, tomorrow]:
            reserved = is_reserved(AIRBNB_CAL_URL, check_date)
            if reserved:
                upsert_loue_date(cur, check_date, reserved)

        conn.commit()
        return {"status": "success", "message": "Synchronisation terminée."}

    except Exception as e:
        logger.error(f"❌ Erreur POST /loue_sync_calendar : {e}")
        raise HTTPException(status_code=500, detail="Erreur base de données")

    finally:
        if conn:
            conn.close()

def is_reserved(cal_url: str, check_date: date) -> bool:
    try:
        response = requests.get(cal_url)
        response.raise_for_status()
        calendar = Calendar(response.text)

        # On filtre uniquement les événements "Reserved" proches de la date recherchée
        for event in calendar.timeline:  # ⚠️ `timeline` trie les événements par date
            
            if event.name != "Reserved":
                continue

            logger.info(f"{event.begin.date().isoformat()} {event.name}")

            # Si l'événement commence après la date recherchée, on peut s'arrêter
            if event.begin.date() > check_date:
                break

            if event.begin.date() <= check_date < event.end.date():
                return True

        return False
    except Exception as e:
        logger.error(f"Erreur lors de la lecture du calendrier Airbnb : {e}")
        return False

def upsert_loue_date(cursor, jour: date, loue: bool):
    logger.info(f"Mis à jour : {jour.isoformat()} loué={loue}")
    
    cursor.execute(
        """
        INSERT INTO airbnb_loue (jour, loue)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE loue = VALUES(loue)
        """,
        (jour.isoformat(), loue)
    )




@app.post("/loue/init")
def init_dates(data: dict):
    logger.debug(f"📅 POST /loue/init : {data}")
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



class CapteurHeureUpdate(BaseModel):
    capteur: str
    jour: date
    heure: int  # entre 0 et 23
    valeur: float


@app.post("/capteurs/heure")
def update_capteur_heure(payload: CapteurHeureUpdate):
    logger.debug(f"🌡️ POST /capteurs/heure : {payload}")
    heure_colonne = f"h{payload.heure:02d}"

    if payload.heure < 0 or payload.heure > 23:
        raise HTTPException(status_code=400, detail="Heure invalide (doit être entre 0 et 23)")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Vérifie si une ligne existe déjà
        cursor.execute(
            "SELECT COUNT(*) FROM capteurs WHERE jour = %s AND capteur = %s",
            (payload.jour, payload.capteur)
        )
        exists = cursor.fetchone()[0] > 0

        if exists:
            query = f"UPDATE capteurs SET {heure_colonne} = %s WHERE jour = %s AND capteur = %s"
            cursor.execute(query, (payload.valeur, payload.jour, payload.capteur))
        else:
            colonnes = ['jour', 'capteur', heure_colonne]
            valeurs = [payload.jour, payload.capteur, payload.valeur]
            placeholders = ', '.join(['%s'] * len(valeurs))
            colonnes_sql = ', '.join(colonnes)
            query = f"INSERT INTO capteurs ({colonnes_sql}) VALUES ({placeholders})"
            cursor.execute(query, valeurs)

        conn.commit()
        return {"message": "Capteur enregistré", "capteur": payload.capteur, "heure": payload.heure}
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Erreur /capteurs/heure : {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


class LieuEnum(str, Enum):
    chambre = "chambre"
    salon = "salon"
    autre = "autre"

class LingerieEnum(str, Enum):
    nue = "nue"
    string = "string"
    pj = "pj"

class EjacEnum(str, Enum):
    corps = "corps"
    vagin = "vagin"
    faciale = "faciale"
    bouche = "bouche"
    anale = "anale"
    aucune = "aucune"

class RapportEntry(BaseModel):
    jour: date
    lieu: LieuEnum = Field(default=LieuEnum.chambre)
    lingerie: LingerieEnum = Field(default=LingerieEnum.pj)
    ejac: EjacEnum = Field(default=EjacEnum.aucune)
    fellation: bool = False
    cunnilingus: bool = False
    levrette: bool = False
    missionnaire: bool = False
    andromaque: bool = False
    sodomie: bool = False
    fouet: bool = False


@app.post("/rapport")
def upsert_rapport(entry: RapportEntry):
    logger.info(f"📘 POST /rapport : {entry}")
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO rapport (
                jour, lieu, lingerie, ejac,
                fellation, cunnilingus, levrette,
                missionnaire, andromaque, sodomie, fouet
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                lieu = VALUES(lieu),
                lingerie = VALUES(lingerie),
                ejac = VALUES(ejac),
                fellation = VALUES(fellation),
                cunnilingus = VALUES(cunnilingus),
                levrette = VALUES(levrette),
                missionnaire = VALUES(missionnaire),
                andromaque = VALUES(andromaque),
                sodomie = VALUES(sodomie),
                fouet = VALUES(fouet)
            """,
            (
                entry.jour,
                entry.lieu.value,
                entry.lingerie.value,
                entry.ejac.value,
                entry.fellation,
                entry.cunnilingus,
                entry.levrette,
                entry.missionnaire,
                entry.andromaque,
                entry.sodomie,
                entry.fouet
            )
        )
        conn.commit()
        return {"status": "ok", "message": f"Rapport enregistré pour {entry.jour}"}
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Erreur POST /rapport : {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
