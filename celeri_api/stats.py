import logging
import time
from fastapi import APIRouter
from celeri_api.db import get_connection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/stats", tags=["stats"])

# ======================================================
# CACHE MÉMOIRE (Home Assistant friendly)
# ======================================================

CACHE = {}
CACHE_TTL = 300  # 5 minutes

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

@router.get("/airbnb/annee")
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
        return rows

    return cached("airbnb_annee", compute)


@router.get("/airbnb/mois")
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
        return rows

    return cached("airbnb_mois", compute)


# ======================================================
# PRÉSENCE
# ======================================================

@router.get("/presence/annee")
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
        return rows

    return cached("presence_annee", compute)


@router.get("/presence/mois")
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
        return rows

    return cached("presence_mois", compute)


# ======================================================
# TÉLÉTRAVAIL
# ======================================================

@router.get("/teletravail/annee")
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
        return rows

    return cached("teletravail_annee", compute)


@router.get("/teletravail/mois")
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
        return rows

    return cached("teletravail_mois", compute)


# ======================================================
# CHEMINÉE
# ======================================================

@router.get("/cheminee/annee")
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
        return rows

    return cached("cheminee_annee", compute)


@router.get("/cheminee/mois")
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
        return rows

    return cached("cheminee_mois", compute)


# ======================================================
# RAPPORTS
# ======================================================

@router.get("/rapports/annee")
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
        return rows

    return cached("rapports_annee", compute)


@router.get("/rapports/mois")
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
        return rows

    return cached("rapports_mois", compute)


@router.get("/rapports/pratiques/annee")
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
                SUM(fouet) AS fouet
            FROM rapport
            GROUP BY YEAR(jour)
            ORDER BY annee
        """)
        rows = cur.fetchall()
        conn.close()
        return rows

    return cached("rapports_pratiques_annee", compute)


# ======================================================
# CAPTEURS (MOYENNE PAR MOIS / ANNÉE)
# ======================================================

@router.get("/capteurs/mois")
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
        return rows

    return cached(cache_key, compute)
