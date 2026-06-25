import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from datetime import date, datetime, timedelta
from fastapi.responses import JSONResponse
from icalendar import Calendar
import requests
from enum import Enum
from typing import Optional
import json
import logging
import mysql.connector
import time
from threading import Lock
import zoneinfo


# TODO
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
    "database": config["DB_NAME"],
}


def get_connection():
    logger.debug("Creating new database connection")
    return mysql.connector.connect(**DB_CONFIG)


AIRBNB_CAL_URL = "https://www.airbnb.fr/calendar/ical/32053854.ics?s=bee9bbc3a51315a4fa27ea2a09621aef"
AIRBNB_CAL_URL2 = "https://www.airbnb.fr/calendar/ical/32057490.ics?s=0f91f1dc1e6c7f6ba3ddf82e0ca59c92"


app = FastAPI()

from fastapi.requests import Request
from fastapi.responses import PlainTextResponse


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




class Trace(BaseModel):
    automation_name: str
    status: str

@app.post("/trace_automation")
def trace_automation(trace: Trace):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO automation_traces (automation_name, executed_at, status) VALUES (%s, %s, %s)",
        (trace.automation_name, datetime.now(), trace.status)
    )
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Trace Automatisation enregistrée"}


@app.get("/trace_automation_daily_report", response_class=PlainTextResponse)
def trace_automation_daily_report():
    conn = get_connection()
    cursor = conn.cursor()

    today = date.today()
    query = """
    SELECT automation_name,
           status,
           executed_at
    FROM automation_traces
    WHERE DATE(executed_at) = %s
    ORDER BY executed_at ASC
    """

    cursor.execute(query, (today,))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return "Aucune automatisation exécutée aujourd'hui."

    report_lines = ["Rapport d'automatisations du " + today.strftime("%d/%m/%Y") + ":\n"]
    for automation_name, status, executed_at in rows:
        heure = executed_at.strftime("%H:%M")  # extrait uniquement l'heure
        line = f"- {heure} : {automation_name} ({status})"
        report_lines.append(line)
        
    return "\n".join(report_lines)


def execute_get_presence(cursor, conn, jour: str) -> bool:
    cursor.execute("SELECT presence FROM presence WHERE jour = %s", (jour,))
    row = cursor.fetchone()
    if row:
        logger.debug(f"✔️ Date trouvée: {jour} => presence={bool(row[0])}")
        return bool(row[0])
    else:
        logger.info(f"❗ Date {jour} absente — ajout avec presence=False")
        cursor.execute("INSERT INTO presence (jour, presence) VALUES (%s, %s)", (jour, False))
        conn.commit()
        return False

def execute_get_teletravail(cursor, conn, jour: str) -> bool:
    cursor.execute("SELECT teletravail FROM teletravail WHERE jour = %s", (jour,))
    row = cursor.fetchone()
    if row:
        logger.debug(f"✔️ Date trouvée: {jour} => teletravail={bool(row[0])}")
        return bool(row[0])
    else:
        logger.info(f"❗ Date {jour} absente — ajout avec teletravail=False")
        cursor.execute("INSERT INTO teletravail (jour, teletravail) VALUES (%s, %s)", (jour, False))
        conn.commit()
        return False

def execute_get_cheminee(cursor, conn, jour: str) -> bool:
    cursor.execute("SELECT cheminee FROM cheminee WHERE jour = %s", (jour,))
    row = cursor.fetchone()
    if row:
        logger.debug(f"✔️ Date trouvée: {jour} => cheminee={bool(row[0])}")
        return bool(row[0])
    else:
        logger.info(f"❗ Date {jour} absente — ajout avec cheminee=False")
        cursor.execute("INSERT INTO cheminee (jour, cheminee) VALUES (%s, %s)", (jour, False))
        conn.commit()
        return False

def execute_get_loue(cursor, conn, jour: str) -> bool:
    cursor.execute("SELECT loue FROM airbnb_loue WHERE jour = %s", (jour,))
    row = cursor.fetchone()
    if row:
        logger.debug(f"✔️ Date trouvée: {jour} => loue={bool(row[0])}")
        return bool(row[0])
    else:
        logger.info(f"❗ Date {jour} absente — ajout avec loue=False")
        cursor.execute("INSERT INTO airbnb_loue (jour, loue) VALUES (%s, %s)", (jour, False))
        conn.commit()
        return False


@app.get("/api/status_du_jour")
def get_status_du_jour():
    logger.debug("📊 GET /api/status_du_jour (appel unifié)")
    
    today = date.today()
    jour_str = today.isoformat()
    hier_str = (today - timedelta(days=1)).isoformat()
    demain_str = (today + timedelta(days=1)).isoformat()

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # On enchaîne les fonctions sur la même connexion
        presence = execute_get_presence(cursor, conn, jour_str)
        teletravail = execute_get_teletravail(cursor, conn, jour_str)
        cheminee = execute_get_cheminee(cursor, conn, jour_str)
        airbnb_hier = execute_get_loue(cursor, conn, hier_str)
        airbnb_aujourdhui = execute_get_loue(cursor, conn, jour_str)
        airbnb_demain = execute_get_loue(cursor, conn, demain_str)

        logger.info(f"📊 {jour_str} : Présence {presence} - Teletravail {teletravail} - Airbnb {airbnb_aujourdhui} - Airbnb demain {airbnb_demain}")

        return {
            "jour": jour_str,
            "presence": presence,
            "teletravail": teletravail,
            "cheminee": cheminee,
            "airbnb_hier": airbnb_hier,
            "airbnb_aujourdhui": airbnb_aujourdhui,
            "airbnb_demain": airbnb_demain
        }

    except Exception as e:
        logger.error(f"❌ Erreur /api/status_du_jour : {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()




@app.get("/presence/{jour}")
def get_presence(jour: str):
    logger.debug(f"🔎 GET /presence/{jour}")
    conn = get_connection()
    cursor = conn.cursor()
    try:
        presence = execute_get_presence(cursor, conn, jour)
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
        teletravail = execute_get_teletravail(cursor, conn, jour)
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
        cheminee = execute_get_cheminee(cursor, conn, jour)
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
        loue = execute_get_loue(cursor, conn, jour)
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

        # 1. On force explicitement le fuseau horaire de Paris
        tz_paris = zoneinfo.ZoneInfo("Europe/Paris")
        
        # À 22h à Paris, "today" restera bien le jour même et ne basculera pas à minuit UTC
        today = datetime.now(tz_paris).date()
        tomorrow = today + timedelta(days=1)

        # Calendrier studio (AIRBNB_CAL_URL2)
        for check_date in [today, tomorrow]:
            logger.info(f"Airbnb - Analyse AIRBNB_CAL_URL2 pour la date : {check_date}")
            reserved = is_reserved(AIRBNB_CAL_URL2, check_date)
            logger.info(f"Airbnb - Analyse AIRBNB_CAL_URL2 pour la date : {check_date} - reserved {reserved}")
            if reserved:
                upsert_loue_date(cur, check_date, reserved)
        
        # Calendrier maison (AIRBNB_CAL_URL)
        for check_date in [today, tomorrow]:
            logger.info(f"Airbnb - Analyse AIRBNB_CAL_URL pour la date : {check_date}")
            reserved = is_reserved(AIRBNB_CAL_URL, check_date)
            logger.info(f"Airbnb - Analyse AIRBNB_CAL_URL pour la date : {check_date} - reserved {reserved}")
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
        logger.debug(f"📅 Analyse du calendrier pour la date : {check_date}")
        
        response = requests.get(cal_url, timeout=7)
        response.raise_for_status()
        
        cal = Calendar.from_ical(response.text)
        
        for component in cal.walk():
            if component.name == "VEVENT":
                # Extraction et normalisation des dates de début et de fin
                dtstart = component.get('dtstart').dt
                dtend = component.get('dtend').dt
                
                if isinstance(dtstart, datetime):
                    dtstart = dtstart.date()
                if isinstance(dtend, datetime):
                    dtend = dtend.date()

                logger.info(f"Réservation recherche : {dtstart} -> {dtend})")
                
                # Une date est réservée si : dtstart <= check_date < dtend
                # (Le jour du départ 'dtend' est libéré à 11h, donc non loué pour la nuit qui suit)
                if dtstart <= check_date < dtend:
                    summary = component.get('summary', 'Réservé')
                    logger.info(f"Event trouvée : {check_date} est dans l'événement '{summary}' ({dtstart} -> {dtend})")
                    if summary == "Réservé":
                        logger.info(f"Réservation trouvée : {check_date} est dans l'événement '{summary}' ({dtstart} -> {dtend})")
                        return True
                    
        logger.info(f"🔓 Aucune réservation pour {check_date}. Statut : Libre.")
        return False
        
    except requests.exceptions.Timeout:
        logger.error(f"⏱️ Timeout de 7s dépassé lors de l'appel au calendrier Airbnb.")
        return False
    except Exception as e:
        logger.error(f"❌ Erreur lors de la lecture du calendrier Airbnb : {e}")
        return False


def upsert_loue_date(cursor, jour: date, loue: bool):
    logger.info(f"📅 Airbnb {jour.isoformat()} loué={loue}")
    
    cursor.execute(
        """
        INSERT INTO airbnb_loue (jour, loue)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE loue = VALUES(loue)
        """,
        (jour.isoformat(), loue)
    )


def to_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in ("true", "on", "yes", "1")
    return bool(value)


@app.post("/loue/init")
def init_dates(payload: dict):
    logger.info(f"📅 POST /loue/init : {payload}")
    try:
        start = datetime.strptime(payload["start"], "%Y-%m-%d").date()
        end = datetime.strptime(payload["end"], "%Y-%m-%d").date()
        resa = to_bool(payload.get("resa", False))
        weekend = to_bool(payload.get("weekend", False))

        logger.info(f"Airbnb init dates {resa} entre {start} et {end} (weekend {weekend})")

        if end < start:
            logger.warning("⛔ Date de fin antérieure à la date de début")
            raise HTTPException(status_code=400, detail="end date must be after start date")

        conn = get_connection()
        cursor = conn.cursor()

        current = start
        count = 0
        while current <= end:
            is_weekend = current.weekday() >= 5  # 5 = Saturday, 6 = Sunday
            statut = weekend if is_weekend else resa
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
        logger.info(f"✅ {count} jours {resa} entre {start} et {end} (weekend {weekend})")
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




# ======================================================
# CACHE MÉMOIRE (Home Assistant friendly)
# ======================================================

CACHE = {}
CACHE_TTL = 7200  # 2h

def cached(key: str, compute_func):
    now = time.time()

    if key in CACHE:
        entry = CACHE[key]
        if now - entry["time"] < CACHE_TTL:
            return entry["data"]

    data = compute_func()
    CACHE[key] = {"time": now, "data": data}
    return data


# ======================================================
# AIRBNB
# ======================================================

@app.get("/stats/airbnb/annee")
def airbnb_par_annee():
    def compute():
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT YEAR(jour) AS annee, COUNT(*) AS nb_jours_loues
            FROM airbnb_loue
            WHERE loue = 1
            GROUP BY YEAR(jour)
            ORDER BY annee
        """)
        rows = cur.fetchall()
        conn.close()
        return {"data": rows}

    return cached("airbnb_annee", compute)


@app.get("/stats/airbnb/mois")
def airbnb_par_mois_et_annee():
    def compute():
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT
                YEAR(jour) AS annee,
                MONTH(jour) AS mois,
                COUNT(*) AS nb_jours_loues
            FROM airbnb_loue
            WHERE loue = 1
            GROUP BY YEAR(jour), MONTH(jour)
            ORDER BY annee, mois
        """)
        rows = cur.fetchall()
        conn.close()
        return {"data": rows}

    return cached("airbnb_mois", compute)


# ======================================================
# PRÉSENCE
# ======================================================

@app.get("/stats/presence/annee")
def presence_par_annee():
    def compute():
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT YEAR(jour) AS annee, COUNT(*) AS nb_jours
            FROM presence
            WHERE presence = 1
            GROUP BY YEAR(jour)
            ORDER BY annee
        """)
        rows = cur.fetchall()
        conn.close()
        return {"data": rows}

    return cached("presence_annee", compute)


@app.get("/stats/presence/mois")
def presence_par_mois_et_annee():
    def compute():
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT
                YEAR(jour) AS annee,
                MONTH(jour) AS mois,
                COUNT(*) AS nb_jours
            FROM presence
            WHERE presence = 1
            GROUP BY YEAR(jour), MONTH(jour)
            ORDER BY annee, mois
        """)
        rows = cur.fetchall()
        conn.close()
        return {"data": rows}

    return cached("presence_mois", compute)


# ======================================================
# TÉLÉTRAVAIL
# ======================================================

@app.get("/stats/teletravail/annee")
def teletravail_par_annee():
    def compute():
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT YEAR(jour) AS annee, COUNT(*) AS nb_jours
            FROM teletravail
            WHERE teletravail = 1
            GROUP BY YEAR(jour)
            ORDER BY annee
        """)
        rows = cur.fetchall()
        conn.close()
        return {"data": rows}

    return cached("teletravail_annee", compute)


@app.get("/stats/teletravail/mois")
def teletravail_par_mois_et_annee():
    def compute():
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT
                YEAR(jour) AS annee,
                MONTH(jour) AS mois,
                COUNT(*) AS nb_jours
            FROM teletravail
            WHERE teletravail = 1
            GROUP BY YEAR(jour), MONTH(jour)
            ORDER BY annee, mois
        """)
        rows = cur.fetchall()
        conn.close()
        return {"data": rows}

    return cached("teletravail_mois", compute)


# ======================================================
# CHEMINÉE
# ======================================================

@app.get("/stats/cheminee/annee")
def cheminee_par_annee():
    def compute():
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT YEAR(jour) AS annee, COUNT(*) AS nb_jours
            FROM cheminee
            WHERE cheminee = 1
            GROUP BY YEAR(jour)
            ORDER BY annee
        """)
        rows = cur.fetchall()
        conn.close()
        return {"data": rows}

    return cached("cheminee_annee", compute)


@app.get("/stats/cheminee/mois")
def cheminee_par_mois_et_annee():
    def compute():
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT
                YEAR(jour) AS annee,
                MONTH(jour) AS mois,
                COUNT(*) AS nb_jours
            FROM cheminee
            WHERE cheminee = 1
            GROUP BY YEAR(jour), MONTH(jour)
            ORDER BY annee, mois
        """)
        rows = cur.fetchall()
        conn.close()
        return {"data": rows}

    return cached("cheminee_mois", compute)


# ======================================================
# RAPPORTS
# ======================================================

@app.get("/stats/rapports/annee")
def rapports_par_annee():
    def compute():
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT YEAR(jour) AS annee, COUNT(*) AS nb_rapports
            FROM rapport
            GROUP BY YEAR(jour)
            ORDER BY annee
        """)
        rows = cur.fetchall()
        conn.close()
        return {"data": rows}

    return cached("rapports_annee", compute)


@app.get("/stats/rapports/mois")
def rapports_par_mois_et_annee():
    def compute():
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT
                YEAR(jour) AS annee,
                MONTH(jour) AS mois,
                COUNT(*) AS nb_rapports
            FROM rapport
            GROUP BY YEAR(jour), MONTH(jour)
            ORDER BY annee, mois
        """)
        rows = cur.fetchall()
        conn.close()
        return {"data": rows}

    return cached("rapports_mois", compute)


@app.get("/stats/rapports/pratiques/annee")
def rapports_pratiques_par_annee():
    def compute():
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT
                YEAR(jour) AS annee,
                SUM(fellation) AS fellation,
                SUM(cunnilingus) AS cunnilingus,
                SUM(levrette) AS levrette,
                SUM(missionnaire) AS missionnaire,
                SUM(andromaque) AS andromaque,
                SUM(sodomie) AS sodomie,
                SUM(fouet) AS fouet,
                sum(ejac = 'corps') as ejac_corps,
                sum(ejac = 'vagin') as ejac_vagin,
                sum(ejac = 'bouche') as ejac_bouche,
                sum(ejac = 'faciale') as ejac_faciale,
                sum(ejac = 'anale') as ejac_anale,
                sum(ejac = 'aucune') as ejac_aucune,
                sum(lingerie = 'nue') as lingerie_nue,
                sum(lingerie = 'string') as lingerie_string,
                sum(lingerie = 'pj') as lingerie_pj,
                sum(lieu = 'chambre') as lieu_chambre,
                sum(lieu = 'salon') as lieu_salon,
                sum(lieu = 'autre') as lieu_autre
            FROM rapport
            GROUP BY YEAR(jour)
            ORDER BY annee
        """)
        rows = cur.fetchall()
        conn.close()
        return {"data": rows}

    return cached("rapports_pratiques_annee", compute)


# ======================================================
# CAPTEURS (MOYENNE PAR MOIS / ANNÉE)
# ======================================================

@app.get("/stats/capteurs/mois")
def capteurs_moyenne_mois(capteur: str):
    cache_key = f"capteurs_{capteur}_mois"

    def compute():
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT
                YEAR(jour) AS annee,
                MONTH(jour) AS mois,
                ROUND(AVG(
                    (h00+h01+h02+h03+h04+h05+h06+h07+h08+h09+h10+h11+
                     h12+h13+h14+h15+h16+h17+h18+h19+h20+h21+h22+h23) / 24
                ), 2) AS moyenne
            FROM capteurs
            WHERE capteur = %s
            GROUP BY YEAR(jour), MONTH(jour)
            ORDER BY annee, mois
        """, (capteur,))
        rows = cur.fetchall()
        conn.close()
        return {"data": rows}

    return cached(cache_key, compute)
