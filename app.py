import os
import functools
import csv
import io
from datetime import datetime, timedelta

from flask import (
    Flask, render_template, request, redirect, url_for,
    session, flash, Response
)
from pyairtable import Api
from werkzeug.security import check_password_hash
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "pauco-comptable-secret-key-change-me")

# Airtable config
AIRTABLE_TOKEN = os.environ.get("AIRTABLE_TOKEN", "")
BASE_ID = "app37TquPqedRoJ96"

api = Api(AIRTABLE_TOKEN)


def get_table(table_name):
    return api.table(BASE_ID, table_name)


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
    records = table.all(formula=f"{{Email}} = '{comptable_email}'")
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
        except Exception:
            continue
    return restaurants


def get_depenses(restaurant_id, months=1):
    """Get expenses for a restaurant over the last N months."""
    table = get_table("Dépenses")
    date_limit = (datetime.now() - timedelta(days=30 * months)).strftime("%Y-%m-%d")
    formula = f"AND({{Restaurant}} = '{restaurant_id}', {{Date}} >= '{date_limit}')"
    try:
        return table.all(formula=formula)
    except Exception:
        return []


def get_revenus(restaurant_id, months=1):
    """Get revenue for a restaurant over the last N months."""
    table = get_table("Revenus")
    date_limit = (datetime.now() - timedelta(days=30 * months)).strftime("%Y-%m-%d")
    formula = f"AND({{Restaurant}} = '{restaurant_id}', {{Date}} >= '{date_limit}')"
    try:
        return table.all(formula=formula)
    except Exception:
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
    # Sort by month
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


# ── Routes ──────────────────────────────────────────────────────
@app.route("/health")
def health():
    return "OK", 200


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

        table = get_table("Comptables")
        try:
            records = table.all(formula=f"{{Email}} = '{email}'")
        except Exception:
            flash("Erreur de connexion à la base de données.", "error")
            return render_template("login.html")

        if not records:
            flash("Email ou mot de passe incorrect.", "error")
            return render_template("login.html")

        comptable = records[0]["fields"]
        stored_hash = comptable.get("Password_hash", "")

        if not stored_hash or not check_password_hash(stored_hash, password):
            flash("Email ou mot de passe incorrect.", "error")
            return render_template("login.html")

        session["user_email"] = email
        session["user_name"] = comptable.get("Nom", email)
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

        revenus = get_revenus(resto_id, months=1)
        depenses = get_depenses(resto_id, months=1)

        ca = sum(r["fields"].get("Montant", 0) for r in revenus)
        total_depenses = sum(d["fields"].get("Montant", 0) for d in depenses)
        resultat = ca - total_depenses

        cards.append({
            "id": resto_id,
            "nom": name,
            "ca": ca,
            "resultat": resultat,
            "nb_depenses": len(depenses),
        })

    return render_template("dashboard.html", cards=cards)


@app.route("/restaurant/<resto_id>")
@login_required
def restaurant_view(resto_id):
    # Verify access
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

    # 12 months data
    revenus_12m = get_revenus(resto_id, months=12)
    depenses_12m = get_depenses(resto_id, months=12)

    monthly_ca = compute_monthly_ca(revenus_12m)
    depenses_by_cat = compute_depenses_by_category(depenses_12m)

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
        total_ca=total_ca,
        total_depenses=total_depenses,
        ratio_food=ratio_food,
        ratio_personnel=ratio_personnel,
    )


@app.route("/restaurant/<resto_id>/export")
@login_required
def export_csv(resto_id):
    # Verify access
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
