# dashboard/app.py

import streamlit as st

st.set_page_config(page_title="🚕 Yellow Taxi", layout="wide")
st.title("🚕 Yellow Taxi – Dashboard")

# 1. Connexion
conn = st.connection("postgresql", type="sql")

# 2. KPI en haut
kpi_cols = st.columns(4)
queries = {
    "Total trips":       "SELECT COUNT(*) FROM fact_trips;",
    "Total revenue":     "SELECT SUM(total_amount) FROM fact_trips;",
    "Avg distance":      "SELECT AVG(trip_distance) FROM fact_trips;",
    "Avg tip amount":    "SELECT AVG(tip_amount) FROM fact_trips;"
}
for col, (label, q) in zip(kpi_cols, queries.items()):
    val = conn.query(q).iloc[0,0] or 0
    if isinstance(val, float):
        display = f"{val:,.2f}"
    else:
        display = f"{val:,}"
    col.metric(label, display)

st.markdown("---")

st.subheader("📈 Nombre de courses par jour (2024)")
df_trips = conn.query("""
  SELECT 
    d.date_value AS date, 
    COUNT(*)     AS trips
  FROM fact_trips f
  JOIN dim_datetime d
    ON f.pickup_datetime_id = d.datetime_id
  WHERE d.date_value >= '2024-01-01' 
    AND d.date_value <  '2025-01-01'
  GROUP BY d.date_value
  ORDER BY d.date_value
""", ttl="10m")
df_trips.index = df_trips["date"]
st.line_chart(df_trips["trips"], use_container_width=True)

st.subheader("💰 Chiffre d'affaires par jour (2024)")
df_rev = conn.query("""
  SELECT 
    d.date_value    AS date, 
    SUM(f.total_amount) AS revenue
  FROM fact_trips f
  JOIN dim_datetime d
    ON f.pickup_datetime_id = d.datetime_id
  WHERE d.date_value >= '2024-01-01' 
    AND d.date_value <  '2025-01-01'
  GROUP BY d.date_value
  ORDER BY d.date_value
""", ttl="10m")
df_rev.index = df_rev["date"]
st.line_chart(df_rev["revenue"], use_container_width=True)

st.subheader("📊 Distribution des distances")
df_dist = conn.query("SELECT trip_distance FROM fact_trips LIMIT 10000", ttl="10m")
st.bar_chart(df_dist["trip_distance"].value_counts().sort_index(), use_container_width=True)

st.subheader("💳 Répartition des types de paiement")
df_pay = conn.query("""
  SELECT p.payment_type_name, COUNT(*) AS cnt
    FROM fact_trips f
    JOIN dim_payment p ON f.payment_type_id = p.payment_type_id
   GROUP BY p.payment_type_name
   ORDER BY cnt DESC
""", ttl="10m")
st.bar_chart(df_pay.set_index("payment_type_name")["cnt"], use_container_width=True)
