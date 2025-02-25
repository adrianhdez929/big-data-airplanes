import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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
if 'flight_delays' not in st.session_state:
    st.session_state.flight_delays = pd.DataFrame()
if 'airport_delays' not in st.session_state:
    st.session_state.airport_delays = {}

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

def process_flight_delays(data):
    if 'flight_delays' in data and data['flight_delays']:
        # Convert flight delays to DataFrame
        df_delays = pd.DataFrame(data['flight_delays'])
        
        # Update session state with new flight delays
        if not st.session_state.flight_delays.empty:
            st.session_state.flight_delays = pd.concat([
                st.session_state.flight_delays,
                df_delays
            ]).drop_duplicates(subset=['flight_icao24', 'timestamp']).reset_index(drop=True)
        else:
            st.session_state.flight_delays = df_delays
        
        # Update airport delay statistics
        for _, flight in df_delays.iterrows():
            if 'arrival_airport' not in flight or not flight['arrival_airport']:
                continue
                
            airport = flight['arrival_airport']
            if airport not in st.session_state.airport_delays:
                st.session_state.airport_delays[airport] = {
                    'total_delays': 0,
                    'flight_count': 0,
                    'avg_delay': 0
                }
            
            if 'estimated_arrival_delay' in flight and pd.notna(flight['estimated_arrival_delay']):
                st.session_state.airport_delays[airport]['total_delays'] += flight['estimated_arrival_delay']
                st.session_state.airport_delays[airport]['flight_count'] += 1
                st.session_state.airport_delays[airport]['avg_delay'] = (
                    st.session_state.airport_delays[airport]['total_delays'] / 
                    st.session_state.airport_delays[airport]['flight_count']
                )

# Define color schemes as constants
DELAY_COLORS = {
    0: 'green',
    1: 'lightgreen',
    2: 'yellow',
    3: 'orange',
    4: 'red',
    5: 'darkred'
}

DELAY_CATEGORY_COLORS = {
    'Minor': 'green',
    'Moderate': 'yellow',
    'Significant': 'orange',
    'Severe': 'red',
    'Critical': 'darkred'
}

def get_delay_color(magnitude):
    return DELAY_COLORS.get(magnitude if isinstance(magnitude, int) else 0, 'gray')

def render_delay_stats():
    st.markdown("""
    <style>
    .delay-status {
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Create two columns for delay statistics
    delay_col1, delay_col2 = st.columns(2)
    
    with delay_col1:
        st.subheader("🔴 Top 10 Delayed Flights")
        if not st.session_state.flight_delays.empty:
            # Get latest delays for each flight
            latest_delays = st.session_state.flight_delays.sort_values('timestamp').groupby('flight_icao24').last()
            
            # Make sure delay_magnitude exists and has valid values
            if 'delay_magnitude' in latest_delays.columns:
                # Filter out rows with NaN delays
                latest_delays = latest_delays[pd.notna(latest_delays['delay_magnitude'])]
                
                if not latest_delays.empty:
                    # Sort by delay magnitude (primary) and estimated delay (secondary)
                    top_delayed_flights = latest_delays.sort_values(
                        ['delay_magnitude', 'estimated_arrival_delay'],
                        ascending=[False, False]
                    ).head(10)
                    
                    # Create a more readable DataFrame for display
                    display_df = pd.DataFrame({
                        'Flight': top_delayed_flights['callsign'],
                        'From': top_delayed_flights['departure_airport'],
                        'To': top_delayed_flights['arrival_airport'],
                        'Est. Delay (min)': top_delayed_flights['estimated_arrival_delay'].round(2),
                        'Magnitude': top_delayed_flights['delay_magnitude'],
                        'Status': top_delayed_flights['status'].apply(
                            lambda x: x.replace('_', ' ').title()
                        )
                    })
                    
                    # Create styled dataframe
                    styled_df = display_df.style.apply(
                        lambda x: ['color: ' + get_delay_color(x['Magnitude'])]*len(x)
                        if x.name == 'Status' else [''] * len(x),
                        axis=1
                    )
                    
                    st.dataframe(
                        styled_df,
                        hide_index=True,
                        height=400
                    )
                    
                    # Show delay distribution
                    status_counts = display_df['Status'].value_counts().reset_index()
                    status_counts.columns = ['Status', 'Count']
                    
                    fig = go.Figure(data=[go.Pie(
                        labels=status_counts['Status'],
                        values=status_counts['Count'],
                        marker=dict(
                            colors=[DELAY_COLORS[int(display_df[display_df['Status'] == status]['Magnitude'].iloc[0])] 
                                for status in status_counts['Status']]
                        )
                    )])
                    
                    fig.update_layout(
                        title='Current Delay Distribution',
                        showlegend=True
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No valid delay data available")
            else:
                st.info("No delay information available")
        else:
            st.info("No flight delay data available yet")
    
    with delay_col2:
        st.subheader("🛫 Top 10 Airports with Most Delays")
        if st.session_state.airport_delays:
            # Convert airport delays to DataFrame
            airport_df = pd.DataFrame.from_dict(st.session_state.airport_delays, orient='index')
            
            if not airport_df.empty:
                # Sort by average delay and get top 10
                top_delayed_airports = airport_df.nlargest(10, 'avg_delay')
                
                # Create a more readable DataFrame for display
                display_df = pd.DataFrame({
                    'Airport': top_delayed_airports.index,
                    'Avg Delay (min)': top_delayed_airports['avg_delay'].round(2),
                    'Total Flights': top_delayed_airports['flight_count'],
                    'Delay Category': pd.cut(
                        top_delayed_airports['avg_delay'],
                        bins=[-float('inf'), 15, 30, 60, 120, float('inf')],
                        labels=['Minor', 'Moderate', 'Significant', 'Severe', 'Critical']
                    )
                })
                
                # Show the dataframe
                st.dataframe(display_df, hide_index=True, height=400)
                
                # Create a bar chart of average delays by airport
                # Get only the categories that exist in our data
                # existing_categories = display_df['Delay Category'].unique()
                # color_map = {cat: DELAY_CATEGORY_COLORS[cat] for cat in existing_categories}
                
                # fig = px.bar(
                #     display_df,
                #     x='Airport',
                #     y='Avg Delay (min)',
                #     title='Average Delays by Airport',
                #     color='Delay Category',
                #     color_discrete_map=color_map
                # )
                # fig.update_layout(
                #     xaxis_title="Airport Code",
                #     yaxis_title="Average Delay (minutes)",
                #     legend_title="Delay Severity"
                # )
                # st.plotly_chart(fig, use_container_width=True)
                
                # Add a summary metric
                total_delayed_flights = sum(airport_df['flight_count'])
                avg_system_delay = round((
                    sum(airport_df['avg_delay'] * airport_df['flight_count']) / 
                    total_delayed_flights
                ), 2) if total_delayed_flights > 0 else 0
                
                metric_col1, metric_col2 = st.columns(2)
                with metric_col1:
                    st.metric(
                        "Total Monitored Flights",
                        f"{total_delayed_flights:,}"
                    )
                with metric_col2:
                    st.metric(
                        "System-wide Average Delay",
                        f"{avg_system_delay} min"
                    )
            else:
                st.info("No airport delay statistics available")
        else:
            st.info("No airport delay data available yet")

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
            
            # Process flight delays
            if 'flight_delays' in data:
                process_flight_delays(data)
            
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
            
            st.markdown("---")
            st.header("📊 Flight Delay Statistics")
            render_delay_stats()
            
            time.sleep(5)
            st.rerun()

update_data()
