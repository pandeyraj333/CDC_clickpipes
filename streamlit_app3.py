import streamlit as st
import pandas as pd
import clickhouse_connect
import time
import datetime

# ⏱️ Auto-refresh every 2 minutes
st.query_params.update(update=int(time.time() // 120))

st.title("🌤️ Weather Dashboard (Live, IST)")
st.caption("Powered by MySQL → ClickPipes → ClickHouse → Streamlit")

# 🔐 Secure credentials
client = clickhouse_connect.get_client(
    host=st.secrets["clickhouse"]["host"],
    user=st.secrets["clickhouse"]["user"],
    password=st.secrets["clickhouse"]["password"],
    secure=True,
    database='MySQL-CDC'  # Adjust if your DB name differs
)

query_city = f"""
SELECT
    city,
FROM trend_table_del2
GROUP BY city
"""

result = client.query(query_city)
df_city = pd.DataFrame(result.result_rows, columns=result.column_names)
unique_values = df_city['city'].unique()
city = st.selectbox("Choose a value:", unique_values)

st.write(f"You selected: {city}")

# 🔹 Visual 1: Hourly Temperature Trends
st.subheader("📈 Temperature Trends (Hourly)")

# Optional date range filter (can be removed if not needed)
# start_date = st.date_input("Start date", datetime.date.today() - datetime.timedelta(days=1))
# end_date = st.date_input("End date", datetime.date.today())

query_mv = f"""
SELECT
    city,
    StartHour,
    round(avgMerge(avg_temperature_state),2) AS avg_temp,
    minMerge(min_temperature_state) AS min_temp,
    maxMerge(max_temperature_state) AS max_temp
FROM trend_table_del2
WHERE city = '{city}'
GROUP BY city, StartHour
ORDER BY StartHour
"""

result = client.query(query_mv)
df_mv = pd.DataFrame(result.result_rows, columns=result.column_names)

if not df_mv.empty:
    df_mv = df_mv.sort_values("StartHour")
    st.line_chart(df_mv.set_index("StartHour")[["avg_temp", "min_temp", "max_temp"]])
    with st.expander("📄 View Aggregated Data"):
        st.dataframe(df_mv)
else:
    st.warning("No data found in this time range.")

# 🔹 Visual 2: Latest Snapshot
st.subheader("🌡️ Latest Live Weather Snapshot")

query_city = f"""
SELECT
    city,
FROM (
SELECT city, parseDateTimeBestEffort(timestamp) AS parsed_timestamp, argMax(_peerdb_is_deleted, _peerdb_synced_at) AS latest_deleted from live_weather_db_weather_data group by city, parsed_timestamp
)
where latest_deleted = 0
GROUP BY city
"""

result = client.query(query_city)
df_city = pd.DataFrame(result.result_rows, columns=result.column_names)
unique_values = df_city['city'].unique()
city = st.selectbox("Choose a value:", unique_values)

today = datetime.date.today()
selected_date = st.date_input("Select a date", today)

selected_date_str = selected_date.strftime("%Y-%m-%d")

query_latest = f"""
SELECT
    city,
    temperature,
    humidity,
    weather_description,
    toTimeZone(parseDateTimeBestEffort(timestamp), 'Asia/Kolkata') AS ist_time
FROM live_weather_db_weather_data
WHERE city = '{city}' and ist_time <= parseDateTimeBestEffort('{selected_date_str}')
ORDER BY parseDateTimeBestEffort(timestamp) DESC
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
    st.warning("No latest weather data found.")
