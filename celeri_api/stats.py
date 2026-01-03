import logging
from fastapi import APIRouter
from main import get_connection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/stats", tags=["stats"])

# ======================================================
# AIRBNB
# ======================================================

@router.get("/airbnb/annee")
def airbnb_par_annee():
    """
    Exemple:
    2024 -> 100 jours loués
    2025 -> 120 jours loués
    """
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            YEAR(jour) AS annee,
            COUNT(*) AS nb_jours_loues
        FROM airbnb_loue
        WHERE loue = 1
        GROUP BY YEAR(jour)
        ORDER BY annee
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows


@router.get("/airbnb/mois")
def airbnb_par_mois_et_annee():
    """
    Exemple:
    Août 2024 -> 10
    Août 2025 -> 8
    """
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            YEAR(jour) AS annee,
            MONTH(jour) AS mois,
            COUNT(*) AS nb_jours_loues
        FROM airbnb_loue
        WHERE loue = 1
        GROUP BY YEAR(jour), MONTH(jour)
        ORDER BY annee, mois
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows


# ======================================================
# PRÉSENCE
# ======================================================

@router.get("/presence/annee")
def presence_par_annee():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            YEAR(jour) AS annee,
            COUNT(*) AS nb_jours
        FROM presence
        WHERE presence = 1
        GROUP BY YEAR(jour)
        ORDER BY annee
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows


@router.get("/presence/mois")
def presence_par_mois_et_annee():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            YEAR(jour) AS annee,
            MONTH(jour) AS mois,
            COUNT(*) AS nb_jours
        FROM presence
        WHERE presence = 1
        GROUP BY YEAR(jour), MONTH(jour)
        ORDER BY annee, mois
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows


# ======================================================
# TÉLÉTRAVAIL
# ======================================================

@router.get("/teletravail/annee")
def teletravail_par_annee():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            YEAR(jour) AS annee,
            COUNT(*) AS nb_jours
        FROM teletravail
        WHERE teletravail = 1
        GROUP BY YEAR(jour)
        ORDER BY annee
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows


@router.get("/teletravail/mois")
def teletravail_par_mois_et_annee():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            YEAR(jour) AS annee,
            MONTH(jour) AS mois,
            COUNT(*) AS nb_jours
        FROM teletravail
        WHERE teletravail = 1
        GROUP BY YEAR(jour), MONTH(jour)
        ORDER BY annee, mois
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows


# ======================================================
# CHEMINÉE
# ======================================================

@router.get("/cheminee/annee")
def cheminee_par_annee():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            YEAR(jour) AS annee,
            COUNT(*) AS nb_jours
        FROM cheminee
        WHERE cheminee = 1
        GROUP BY YEAR(jour)
        ORDER BY annee
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows


@router.get("/cheminee/mois")
def cheminee_par_mois_et_annee():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            YEAR(jour) AS annee,
            MONTH(jour) AS mois,
            COUNT(*) AS nb_jours
        FROM cheminee
        WHERE cheminee = 1
        GROUP BY YEAR(jour), MONTH(jour)
        ORDER BY annee, mois
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows


# ======================================================
# RAPPORTS
# ======================================================

@router.get("/rapports/annee")
def rapports_par_annee():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            YEAR(jour) AS annee,
            COUNT(*) AS nb_rapports
        FROM rapport
        GROUP BY YEAR(jour)
        ORDER BY annee
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows


@router.get("/rapports/mois")
def rapports_par_mois_et_annee():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            YEAR(jour) AS annee,
            MONTH(jour) AS mois,
            COUNT(*) AS nb_rapports
        FROM rapport
        GROUP BY YEAR(jour), MONTH(jour)
        ORDER BY annee, mois
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows


@router.get("/rapports/pratiques/annee")
def rapports_pratiques_par_annee():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
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
    rows = cursor.fetchall()
    conn.close()
    return rows
