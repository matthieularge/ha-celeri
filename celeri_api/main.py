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
from fastapi.responses import HTMLResponse
from html import escape



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


@app.post("/loue_sync_calendar")
def loue_sync_calendar():
    try:
        conn = get_connection()
        cur = conn.cursor()

        tz_paris = zoneinfo.ZoneInfo("Europe/Paris")
        today = datetime.now(tz_paris).date()
        #tomorrow = today + timedelta(days=1)
        #check_dates = [today, tomorrow]
        first_next_month = (today.replace(day=1) + timedelta(days=32)).replace(day=1)
        first_month_after = (first_next_month + timedelta(days=32)).replace(day=1)
        end_next_month = first_month_after - timedelta(days=1)
        nb_days = (end_next_month - today).days + 1
        check_dates = [today + timedelta(days=i) for i in range(nb_days)]
        

        for url in [AIRBNB_CAL_URL, AIRBNB_CAL_URL2]:
            events = get_relevant_events(url, check_dates)
            for check_date in check_dates:
                is_res = any(e['start'] <= check_date < e['end'] and "Reserved" in e['summary'] 
                             for e in events)
                logger.info(f"Airbnb {url} - {check_date} - reserved: {is_res}")
                if is_res:
                    upsert_loue_date(cur, check_date, is_res)

        conn.commit()
        return {"status": "success", "message": "Synchronisation terminée."}

    except Exception as e:
        logger.error(f"❌ Erreur POST /loue_sync_calendar : {e}")
        raise HTTPException(status_code=500, detail="Erreur base de données")
    finally:
        if conn:
            conn.close()


def get_relevant_events(cal_url: str, dates: list):
    try:
        response = requests.get(cal_url, timeout=7)
        response.raise_for_status()
        cal = Calendar.from_ical(response.text)
        
        events = []
        min_date, max_date = min(dates), max(dates)
 
        logger.info(f"🔍 Scan du calendrier : {cal_url} pour la période {min_date} à {max_date}")

        for component in cal.walk('VEVENT'):
            
            start_val = component.get('dtstart')
            end_val = component.get('dtend')

            if not start_val or not end_val:
                continue

            dtstart = start_val.dt
            dtend = end_val.dt
            
            if isinstance(dtstart, datetime): dtstart = dtstart.date()
            if isinstance(dtend, datetime): dtend = dtend.date()

            if dtstart <= max_date and dtend > min_date:
                summary = str(component.get('summary', ''))
                logger.info(f"✅ Événement trouvé : '{summary}' ({dtstart} -> {dtend})")
                events.append({
                    'start': dtstart,
                    'end': dtend,
                    'summary': summary
                })
                
        return events
    except Exception as e:
        logger.error(f"Erreur téléchargement {cal_url}: {e}")
        return []


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





FR_DAYS = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]


def to_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def build_stays(nights: list) -> list:
    """Regroupe les nuits louées consécutives en séjours."""
    stays = []
    for n in sorted(nights):
        if stays and n == stays[-1]["last"] + timedelta(days=1):
            stays[-1]["last"] = n
        else:
            stays.append({"first": n, "last": n})
    for s in stays:
        s["start"] = s["first"]                      # jour d'arrivée
        s["end"] = s["last"] + timedelta(days=1)     # jour de départ
    return stays


def serialize_stay(s: dict) -> dict:
    return {
        "arrivee": s["start"].isoformat(),
        "jour_arrivee": FR_DAYS[s["start"].weekday()],
        "depart": s["end"].isoformat(),
        "jour_depart": FR_DAYS[s["end"].weekday()],
        "nuits": (s["end"] - s["start"]).days,
    }


def build_week(stays: list, nights: set, monday: date) -> dict:
    sunday = monday + timedelta(days=6)
    jours = []
    for i in range(7):
        d = monday + timedelta(days=i)
        jours.append({
            "date": d.isoformat(),
            "jour": FR_DAYS[d.weekday()],
            "loue": d in nights,
            "arrivee": any(s["start"] == d for s in stays),
            "depart": any(s["end"] == d for s in stays),
        })

    in_week = [s for s in stays if s["first"] <= sunday and s["last"] >= monday]
    return {
        "du": monday.isoformat(),
        "au": sunday.isoformat(),
        "nb_reservations": len(in_week),
        "nb_nuits": sum(1 for j in jours if j["loue"]),
        "reservations": [serialize_stay(s) for s in in_week],
        "jours": jours,
    }


def build_period_stats(stays: list, nights: set, p_start: date, p_end: date) -> dict:
    """p_start et p_end inclus."""
    touching = [s for s in stays if s["first"] <= p_end and s["last"] >= p_start]
    nb_nuits = sum(1 for n in nights if p_start <= n <= p_end)
    return {
        "du": p_start.isoformat(),
        "au": p_end.isoformat(),
        "nb_reservations": len(touching),
        "nb_nuits": nb_nuits,
        "reservations": [serialize_stay(s) for s in touching],
    }


MOIS_FR = ["janvier", "février", "mars", "avril", "mai", "juin", "juillet",
           "août", "septembre", "octobre", "novembre", "décembre"]


def compute_airbnb_reservations() -> dict:
    tz_paris = zoneinfo.ZoneInfo("Europe/Paris")
    today = datetime.now(tz_paris).date()

    monday = today - timedelta(days=today.weekday())
    next_monday = monday + timedelta(days=7)

    first_next_month = (today.replace(day=1) + timedelta(days=32)).replace(day=1)
    first_month_after = (first_next_month + timedelta(days=32)).replace(day=1)
    end_this_month = first_next_month - timedelta(days=1)
    end_next_month = first_month_after - timedelta(days=1)

    range_start = min(monday, today) - timedelta(days=30)
    range_end = max(end_next_month, next_monday + timedelta(days=6))

    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT jour FROM airbnb_loue WHERE loue = 1 AND jour BETWEEN %s AND %s ORDER BY jour",
            (range_start, range_end)
        )
        nights_list = [to_date(row[0]) for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

    nights = set(nights_list)
    stays = build_stays(nights_list)
    logger.info(f"📅 {len(stays)} séjours trouvés en base ({range_start} -> {range_end})")

    return {
        "today": today,
        "semaine_en_cours": build_week(stays, nights, monday),
        "semaine_prochaine": build_week(stays, nights, next_monday),
        "fin_du_mois": build_period_stats(stays, nights, today, end_this_month),
        "mois_prochain": build_period_stats(stays, nights, first_next_month, end_next_month),
        "mois_prochain_nom": f"{MOIS_FR[first_next_month.month - 1]} {first_next_month.year}",
        "mois_en_cours_nom": f"{MOIS_FR[today.month - 1]} {today.year}",
    }


def fmt_fr(iso: str) -> str:
    d = date.fromisoformat(iso)
    return f"{FR_DAYS[d.weekday()]} {d.strftime('%d/%m')}"


def html_week(titre: str, week: dict) -> str:
    rows = ""
    for j in week["jours"]:
        if j["loue"]:
            bg, statut = "#d4edda", "Loué"
        else:
            bg, statut = "#f5f5f5", "Libre"
        notes = []
        if j["arrivee"]:
            notes.append("➡️ Arrivée")
        if j["depart"]:
            notes.append("⬅️ Départ")
        d = date.fromisoformat(j["date"])
        rows += (
            f'<tr style="background:{bg};">'
            f'<td style="padding:8px 12px;border-bottom:1px solid #ffffff;"><b>{escape(j["jour"].capitalize())}</b> {d.strftime("%d/%m")}</td>'
            f'<td style="padding:8px 12px;border-bottom:1px solid #ffffff;">{statut}</td>'
            f'<td style="padding:8px 12px;border-bottom:1px solid #ffffff;">{" · ".join(notes)}</td>'
            f'</tr>'
        )

    if week["reservations"]:
        resa = "".join(
            f'<li style="margin:4px 0;">{escape(fmt_fr(r["arrivee"]))} → {escape(fmt_fr(r["depart"]))} '
            f'({r["nuits"]} nuit{"s" if r["nuits"] > 1 else ""})</li>'
            for r in week["reservations"]
        )
        resa_html = f'<ul style="margin:8px 0 12px 0;padding-left:20px;">{resa}</ul>'
    else:
        resa_html = '<p style="margin:8px 0 12px 0;color:#777;">Aucune réservation.</p>'

    return (
        f'<h2 style="font-size:18px;margin:24px 0 4px 0;color:#222;">{escape(titre)}</h2>'
        f'<p style="margin:0;color:#777;font-size:13px;">du {fmt_fr(week["du"])} au {fmt_fr(week["au"])} '
        f'— {week["nb_reservations"]} réservation{"s" if week["nb_reservations"] > 1 else ""}, '
        f'{week["nb_nuits"]} nuit{"s" if week["nb_nuits"] > 1 else ""}</p>'
        f'{resa_html}'
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        f'style="border-collapse:collapse;font-size:14px;">{rows}</table>'
    )


def html_stat_card(titre: str, stats: dict) -> str:
    return (
        f'<td width="50%" valign="top" style="padding:6px;">'
        f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
        f'style="background:#eef3fb;border-radius:8px;">'
        f'<tr><td style="padding:14px;text-align:center;">'
        f'<div style="font-size:13px;color:#555;">{escape(titre)}</div>'
        f'<div style="font-size:32px;font-weight:bold;color:#1a4fa0;margin:6px 0;">{stats["nb_reservations"]}</div>'
        f'<div style="font-size:13px;color:#555;">réservation{"s" if stats["nb_reservations"] > 1 else ""} '
        f'· {stats["nb_nuits"]} nuit{"s" if stats["nb_nuits"] > 1 else ""}</div>'
        f'</td></tr></table></td>'
    )


def build_html(data: dict) -> str:
    today = data["today"]
    titre_jour = f'{FR_DAYS[today.weekday()].capitalize()} {today.strftime("%d/%m/%Y")}'

    return f"""<!DOCTYPE html>
<html lang="fr">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"></head>
<body style="margin:0;padding:0;background:#e9ecef;font-family:Arial,Helvetica,sans-serif;color:#222;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#e9ecef;">
<tr><td align="center" style="padding:20px 10px;">
  <table role="presentation" width="600" cellpadding="0" cellspacing="0"
         style="width:100%;max-width:600px;background:#ffffff;border-radius:10px;">
    <tr><td style="background:#ff5a5f;color:#ffffff;padding:20px;border-radius:10px 10px 0 0;">
      <div style="font-size:22px;font-weight:bold;">🏠 Réservations Airbnb</div>
      <div style="font-size:14px;margin-top:4px;">{escape(titre_jour)}</div>
    </td></tr>
    <tr><td style="padding:10px 20px 24px 20px;">

      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-top:12px;">
        <tr>
          {html_stat_card(f"D'ici fin {data['mois_en_cours_nom']}", data["fin_du_mois"])}
          {html_stat_card(data["mois_prochain_nom"].capitalize(), data["mois_prochain"])}
        </tr>
      </table>

      {html_week("Semaine en cours", data["semaine_en_cours"])}
      {html_week("Semaine prochaine", data["semaine_prochaine"])}

    </td></tr>
  </table>
</td></tr>
</table>
</body>
</html>"""


@app.get("/airbnb/reservations", response_class=HTMLResponse)
def airbnb_reservations():
    try:
        data = compute_airbnb_reservations()
        return HTMLResponse(content=build_html(data))
    except Exception as e:
        logger.error(f"❌ Erreur GET /airbnb/reservations : {e}")
        raise HTTPException(status_code=500, detail=str(e))












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
