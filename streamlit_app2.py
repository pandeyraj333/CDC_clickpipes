import streamlit as st
import pandas as pd
import clickhouse_connect
import time

# Refresh every 2 mins
st.query_params.update(update=int(time.time() // 120))

st.title("🌤️ Mumbai Weather Dashboard (Live, IST)")
st.caption("Powered by MySQL → ClickPipes → ClickHouse → Streamlit")

# 🔐 Secure credentials
client = clickhouse_connect.get_client(
    host=st.secrets["clickhouse"]["host"],
    user=st.secrets["clickhouse"]["user"],
    password=st.secrets["clickhouse"]["password"],
    secure=True,
    database='MySQL-CDC'  # make sure this matches your database name
)

# 🔹 Visual 1: Trends
st.subheader("📈 Temperature Trends (Every 2 Minutes)")

query_mv = """
SELECT *
FROM temp_trend_mv
ORDER BY interval_time_ist DESC
"""
result = client.query(query_mv)
df_mv = pd.DataFrame(result.result_rows, columns=result.column_names)
df_mv = df_mv.sort_values("interval_time_ist")

st.line_chart(df_mv.set_index("interval_time_ist")[["avg_temp", "min_temp", "max_temp"]])
with st.expander("📄 View Aggregated Data"):
    st.dataframe(df_mv)

# 🔹 Visual 2: Latest Snapshot
st.subheader("🌡️ Latest Live Weather Snapshot")

query_latest = """
SELECT
    city,
    temperature,
    humidity,
    weather_description,
    toTimeZone(toDateTime(timestamp), 'Asia/Kolkata') AS ist_time
FROM live_weather_db_weather_data
ORDER BY timestamp DESC
LIMIT 1
"""

latest = client.query(query_latest)
latest_df = pd.DataFrame(latest.result_rows, columns=latest.column_names)

if not latest_df.empty:
    row = latest_df.iloc[0]
    st.metric(label="Temperature (°C)", value=f"{row['temperature']}°")
    st.metric(label="Humidity (%)", value=f"{row['humidity']}%")
    st.write(f"**Description:** {row['weather_description']}")
    st.write(f"**Updated at:** {row['ist_time']} IST")
else:
    st.warning("No data found.")
