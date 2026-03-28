import os
import re
import sys
import logging
import functools
import csv
import io
from datetime import datetime, timedelta

from flask import (
    Flask, render_template, request, redirect, url_for,
    session, flash, Response
)
import requests as http_requests
from pyairtable import Api
from werkzeug.security import generate_password_hash, check_password_hash
from dotenv import load_dotenv

load_dotenv()

# ── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("pauco-comptable")

app = Flask(__name__)
app.secret_key = os.environ["SECRET_KEY"]
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = os.environ.get("FLASK_ENV") == "production"
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=8)

# Airtable config — try all possible env var names
AIRTABLE_TOKEN = os.environ.get("AIRTABLE_PAT") or os.environ.get("AIRTABLE_API_KEY") or os.environ.get("AIRTABLE_TOKEN", "")
BASE_ID = "app37TquPqedRoJ96"
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "1606156456")

# Log which env var was found
if os.environ.get("AIRTABLE_PAT"):
    logger.info("Airtable: using AIRTABLE_PAT (%s...)", AIRTABLE_TOKEN[:10])
elif os.environ.get("AIRTABLE_API_KEY"):
    logger.info("Airtable: using AIRTABLE_API_KEY (%s...)", AIRTABLE_TOKEN[:10])
elif os.environ.get("AIRTABLE_TOKEN"):
    logger.info("Airtable: using AIRTABLE_TOKEN (%s...)", AIRTABLE_TOKEN[:10])
else:
    logger.error("AUCUNE VARIABLE AIRTABLE TROUVEE — définir AIRTABLE_PAT dans Railway")

api = Api(AIRTABLE_TOKEN)

# Startup connectivity test
def test_airtable_connection():
    if not AIRTABLE_TOKEN:
        logger.error("Airtable: token vide — impossible de se connecter")
        return
    try:
        table = api.table(BASE_ID, "Comptables")
        table.all(max_records=1)
        logger.info("Airtable: connexion OK — base %s table Comptables accessible", BASE_ID)
    except Exception as e:
        logger.error("Airtable: ECHEC connexion — %s", e)

test_airtable_connection()


def get_table(table_name):
    return api.table(BASE_ID, table_name)


# ── Security helpers ────────────────────────────────────────────
def sanitize_for_formula(value):
    """Escape single quotes to prevent Airtable formula injection."""
    return value.replace("'", "\\'")


# ── Auth decorator ──────────────────────────────────────────────
def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if "user_email" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


# ── Helpers ─────────────────────────────────────────────────────
def get_restaurants_for_comptable(comptable_email):
    """Get restaurants assigned to this accountant."""
    table = get_table("Comptables")
    safe_email = sanitize_for_formula(comptable_email)
    records = table.all(formula=f"{{Email}} = '{safe_email}'")
    if not records:
        return []
    comptable = records[0]["fields"]
    restaurant_ids = comptable.get("Restaurants_IDs", comptable.get("Restaurants", []))
    if not restaurant_ids:
        return []

    restaurants_table = get_table("Restaurants")
    restaurants = []
    for rid in restaurant_ids:
        try:
            rec = restaurants_table.get(rid)
            restaurants.append(rec)
        except Exception as e:
            logger.warning("Restaurant %s inaccessible: %s", rid, e)
            continue
    return restaurants


def get_depenses(restaurant_id, months=1):
    """Get expenses for a restaurant over the last N months."""
    table = get_table("Dépenses")
    date_limit = (datetime.now() - timedelta(days=30 * months)).strftime("%Y-%m-%d")
    safe_id = sanitize_for_formula(restaurant_id)
    formula = f"AND({{Restaurant}} = '{safe_id}', {{Date}} >= '{date_limit}')"
    try:
        return table.all(formula=formula)
    except Exception as e:
        logger.error("Dépenses — erreur Airtable: %s", e)
        return []


def get_revenus(restaurant_id, months=1):
    """Get revenue for a restaurant over the last N months."""
    table = get_table("Revenus")
    date_limit = (datetime.now() - timedelta(days=30 * months)).strftime("%Y-%m-%d")
    safe_id = sanitize_for_formula(restaurant_id)
    formula = f"AND({{Restaurant}} = '{safe_id}', {{Date}} >= '{date_limit}')"
    try:
        return table.all(formula=formula)
    except Exception as e:
        logger.error("Revenus — erreur Airtable: %s", e)
        return []


def compute_monthly_ca(revenus_records):
    """Compute CA grouped by month."""
    monthly = {}
    for rec in revenus_records:
        fields = rec["fields"]
        date_str = fields.get("Date", "")
        montant = fields.get("Montant", 0)
        if date_str:
            key = date_str[:7]  # YYYY-MM
            monthly[key] = monthly.get(key, 0) + montant
    return dict(sorted(monthly.items()))


def compute_depenses_by_category(depenses_records):
    """Group expenses by category."""
    categories = {}
    for rec in depenses_records:
        fields = rec["fields"]
        cat = fields.get("Catégorie", "Autre")
        montant = fields.get("Montant", 0)
        categories[cat] = categories.get(cat, 0) + montant
    return dict(sorted(categories.items()))


def get_depenses_list(depenses_records):
    """Return flat list of expenses for table display."""
    rows = []
    for rec in depenses_records:
        f = rec["fields"]
        rows.append({
            "date": f.get("Date", ""),
            "categorie": f.get("Catégorie", "Autre"),
            "description": f.get("Description", ""),
            "montant": f.get("Montant", 0),
        })
    rows.sort(key=lambda x: x["date"], reverse=True)
    return rows


# ── Telegram ────────────────────────────────────────────────────
def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        http_requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=5)
    except Exception:
        pass


# ── Routes ──────────────────────────────────────────────────────
@app.route("/health")
def health():
    return "OK", 200


@app.route("/health/airtable")
def health_airtable():
    if not AIRTABLE_TOKEN:
        return "FAIL: aucune variable Airtable configurée (AIRTABLE_PAT / AIRTABLE_API_KEY / AIRTABLE_TOKEN)", 500
    try:
        table = api.table(BASE_ID, "Comptables")
        records = table.all(max_records=1)
        return f"OK: Airtable connecté — base {BASE_ID} — {len(records)} record(s) test", 200
    except Exception as e:
        return f"FAIL: {e}", 500


@app.route("/inscription", methods=["GET", "POST"])
def inscription():
    if request.method == "POST":
        prenom = request.form.get("prenom", "").strip()
        nom = request.form.get("nom", "").strip()
        cabinet = request.form.get("cabinet", "").strip()
        email = request.form.get("email", "").strip()
        telephone = request.form.get("telephone", "").strip()
        password = request.form.get("password", "").strip()
        password_confirm = request.form.get("password_confirm", "").strip()

        if not all([prenom, nom, cabinet, email, password, password_confirm]):
            flash("Veuillez remplir tous les champs obligatoires.", "error")
            return render_template("inscription.html")

        if password != password_confirm:
            flash("Les mots de passe ne correspondent pas.", "error")
            return render_template("inscription.html")

        if len(password) < 8:
            flash("Le mot de passe doit contenir au moins 8 caractères.", "error")
            return render_template("inscription.html")

        if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
            flash("Adresse email invalide.", "error")
            return render_template("inscription.html")

        if not AIRTABLE_TOKEN:
            flash("Configuration serveur incomplète (variable AIRTABLE_PAT manquante). Contactez l'administrateur.", "error")
            return render_template("inscription.html")

        table = get_table("Comptables")
        try:
            safe_email = sanitize_for_formula(email)
            existing = table.all(formula=f"{{Email}} = '{safe_email}'")
            if existing:
                flash("Un compte avec cet email existe déjà.", "error")
                return render_template("inscription.html")
        except Exception as e:
            logger.error("Inscription — erreur lecture Comptables: %s", e)
            flash("Erreur de connexion à la base de données. Réessayez dans quelques minutes.", "error")
            return render_template("inscription.html")

        try:
            table.create({
                "Prenom": prenom,
                "Nom": nom,
                "Cabinet": cabinet,
                "Email": email,
                "Telephone": telephone,
                "Password_hash": generate_password_hash(password),
                "Statut": "en_attente",
                "Created_at": datetime.now().isoformat(),
            })
        except Exception as e:
            logger.error("Inscription — erreur création Comptables: %s", e)
            flash("Erreur lors de la création du compte. Réessayez dans quelques minutes.", "error")
            return render_template("inscription.html")

        send_telegram(f"Nouveau comptable inscrit : {prenom} {nom} — {cabinet} — {email}")

        flash("Votre demande est en cours de validation. Vous recevrez un email sous 24h.", "success")
        return redirect(url_for("login"))

    return render_template("inscription.html")


@app.route("/")
def index():
    if "user_email" in session:
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "").strip()

        if not email or not password:
            flash("Veuillez remplir tous les champs.", "error")
            return render_template("login.html")

        if not AIRTABLE_TOKEN:
            flash("Configuration serveur incomplète (variable AIRTABLE_PAT manquante). Contactez l'administrateur.", "error")
            return render_template("login.html")

        table = get_table("Comptables")
        try:
            safe_email = sanitize_for_formula(email)
            records = table.all(formula=f"{{Email}} = '{safe_email}'")
        except Exception as e:
            logger.error("Login — erreur Airtable: %s", e)
            flash("Erreur de connexion à la base de données. Réessayez dans quelques minutes.", "error")
            return render_template("login.html")

        if not records:
            flash("Email ou mot de passe incorrect.", "error")
            return render_template("login.html")

        comptable = records[0]["fields"]
        stored_hash = comptable.get("Password_hash", "")

        if not stored_hash or not check_password_hash(stored_hash, password):
            flash("Email ou mot de passe incorrect.", "error")
            return render_template("login.html")

        statut = comptable.get("Statut", "")
        if statut != "actif":
            flash("Votre compte est en attente de validation.", "error")
            return render_template("login.html")

        session.permanent = True
        session["user_email"] = email
        session["user_name"] = comptable.get("Nom", email)
        session["user_cabinet"] = comptable.get("Cabinet", "")
        return redirect(url_for("dashboard"))

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/dashboard")
@login_required
def dashboard():
    restaurants = get_restaurants_for_comptable(session["user_email"])
    cards = []

    for resto in restaurants:
        fields = resto["fields"]
        resto_id = resto["id"]
        name = fields.get("Nom", "Restaurant")
        ville = fields.get("Ville", "")

        revenus = get_revenus(resto_id, months=1)
        depenses = get_depenses(resto_id, months=1)

        ca = sum(r["fields"].get("Montant", 0) for r in revenus)
        total_depenses = sum(d["fields"].get("Montant", 0) for d in depenses)
        resultat = ca - total_depenses

        cards.append({
            "id": resto_id,
            "nom": name,
            "ville": ville,
            "ca": ca,
            "resultat": resultat,
            "nb_depenses": len(depenses),
        })

    return render_template("dashboard.html", cards=cards)


@app.route("/restaurant/<resto_id>")
@login_required
def restaurant_view(resto_id):
    restaurants = get_restaurants_for_comptable(session["user_email"])
    resto_ids = [r["id"] for r in restaurants]
    if resto_id not in resto_ids:
        flash("Accès non autorisé.", "error")
        return redirect(url_for("dashboard"))

    resto_table = get_table("Restaurants")
    try:
        resto = resto_table.get(resto_id)
    except Exception:
        flash("Restaurant introuvable.", "error")
        return redirect(url_for("dashboard"))

    fields = resto["fields"]
    name = fields.get("Nom", "Restaurant")

    revenus_12m = get_revenus(resto_id, months=12)
    depenses_12m = get_depenses(resto_id, months=12)

    monthly_ca = compute_monthly_ca(revenus_12m)
    depenses_by_cat = compute_depenses_by_category(depenses_12m)
    depenses_table = get_depenses_list(depenses_12m)

    total_ca = sum(monthly_ca.values())
    total_depenses = sum(depenses_by_cat.values())

    food_cost = depenses_by_cat.get("Matières premières", 0) + depenses_by_cat.get("Food", 0)
    personnel = depenses_by_cat.get("Personnel", 0) + depenses_by_cat.get("Salaires", 0)

    ratio_food = (food_cost / total_ca * 100) if total_ca > 0 else 0
    ratio_personnel = (personnel / total_ca * 100) if total_ca > 0 else 0

    return render_template(
        "restaurant.html",
        name=name,
        resto_id=resto_id,
        monthly_ca=monthly_ca,
        depenses_by_cat=depenses_by_cat,
        depenses_table=depenses_table,
        total_ca=total_ca,
        total_depenses=total_depenses,
        ratio_food=ratio_food,
        ratio_personnel=ratio_personnel,
    )


@app.route("/restaurant/<resto_id>/export")
@login_required
def export_csv(resto_id):
    restaurants = get_restaurants_for_comptable(session["user_email"])
    resto_ids = [r["id"] for r in restaurants]
    if resto_id not in resto_ids:
        flash("Accès non autorisé.", "error")
        return redirect(url_for("dashboard"))

    revenus = get_revenus(resto_id, months=12)
    depenses = get_depenses(resto_id, months=12)

    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")

    writer.writerow(["Type", "Date", "Catégorie", "Montant"])

    for r in revenus:
        f = r["fields"]
        writer.writerow(["Revenu", f.get("Date", ""), f.get("Catégorie", ""), f.get("Montant", 0)])

    for d in depenses:
        f = d["fields"]
        writer.writerow(["Dépense", f.get("Date", ""), f.get("Catégorie", ""), f.get("Montant", 0)])

    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=export_{resto_id}.csv"},
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
