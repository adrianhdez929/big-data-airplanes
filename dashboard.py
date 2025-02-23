import streamlit as st
import pandas as pd
import plotly.express as px
from kafka import KafkaConsumer
import json
from datetime import datetime
import time

st.set_page_config(
    page_title="Real-time Flight Tracker",
    page_icon="✈️",
    layout="wide"
)

# inicializar el consumidor de kafka
@st.cache_resource
def get_kafka_consumer():
    return KafkaConsumer(
        'airplanes_data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='airplanes_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

# guardar el estado de la pagina para minimizar el tiempo entre renderizados
if 'data' not in st.session_state:
    st.session_state.data = None
if 'metrics' not in st.session_state:
    st.session_state.metrics = {
        'active_flights': 0,
        'countries': 0,
        'avg_altitude': 0,
        'avg_velocity': 0
    }
if 'country_data' not in st.session_state:
    st.session_state.country_data = None

st.title("✈️ Real-time Flight Tracker")
st.markdown("Data from OpenSky Network - Updates every 10 seconds")

# columnas que cntienen las metricas
col1, col2, col3, col4 = st.columns(4)

# contenedor del mapa
map_container = st.container()

# columnas con estadisticas
left_col, right_col = st.columns(2)

consumer = get_kafka_consumer()

def process_data(df):
    # actualizar el estado de la pagina
    st.session_state.metrics = {
        'active_flights': len(df),
        'countries': df['origin_country'].nunique(),
        'avg_altitude': df[df['baro_altitude'].notna()]['baro_altitude'].mean(),
        'avg_velocity': df[df['velocity'].notna()]['velocity'].mean()
    }
    
    st.session_state.country_data = df['origin_country'].value_counts().head(10)    
    st.session_state.data = df

def render_metrics():
    with col1:
        st.metric("Active Flights", st.session_state.metrics['active_flights'])
    with col2:
        st.metric("Countries", st.session_state.metrics['countries'])
    with col3:
        st.metric("Avg. Altitude (m)", f"{st.session_state.metrics['avg_altitude']:,.0f}")
    with col4:
        st.metric("Avg. Velocity (m/s)", f"{st.session_state.metrics['avg_velocity']:,.0f}")

def render_map():
    if st.session_state.data is not None:
        with map_container:
            st.subheader("Live Flight Map")
            fig = px.scatter_mapbox(
                st.session_state.data,
                lat='latitude',
                lon='longitude',
                hover_name='callsign',
                hover_data=['origin_country', 'velocity', 'baro_altitude'],
                color='origin_country',
                zoom=1,
                height=500
            )
            fig.update_layout(
                mapbox_style="carto-positron",
                margin={"r":0,"t":0,"l":0,"b":0}
            )
            st.plotly_chart(fig, use_container_width=True)

def render_charts():
    if st.session_state.country_data is not None:
        with left_col:
            st.subheader("Top 10 Countries")
            fig = px.bar(
                x=st.session_state.country_data.values,
                y=st.session_state.country_data.index,
                orientation='h',
                title="Flights by Country"
            )
            st.plotly_chart(fig, use_container_width=True)

def update_data():
    for message in consumer:
        data = message.value
        if data and 'states' in data:
            df = pd.DataFrame(data['states'])
            
            df['last_contact'] = pd.to_datetime(df['last_contact'], unit='s')
            df['time_position'] = pd.to_datetime(df['time_position'], unit='s')
            
            # procesar los datos y renderizar los graficos
            process_data(df)
            
            render_metrics()
            render_map()
            render_charts()

            with right_col:
                st.subheader("Altitude Distribution")
                fig = px.histogram(
                    df[df['baro_altitude'].notna()],
                    x='baro_altitude',
                    nbins=30,
                    title="Altitude Distribution"
                )
                st.plotly_chart(fig, use_container_width=True)

            st.subheader("Flight Data")
            selected_countries = st.multiselect(
                "Filter by Country",
                options=sorted(df['origin_country'].unique()),
                default=[]
            )
            
            filtered_df = df if not selected_countries else df[df['origin_country'].isin(selected_countries)]
            st.dataframe(
                filtered_df[[
                    'callsign', 'origin_country', 'latitude', 'longitude',
                    'baro_altitude', 'velocity', 'vertical_rate'
                ]],
                hide_index=True
            )

            time.sleep(5)
            st.rerun()

update_data()
