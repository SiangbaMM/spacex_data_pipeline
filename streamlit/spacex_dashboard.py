"""
SpaceX Data Dashboard

This Streamlit app provides visualizations for SpaceX launch, payload, and Starlink data
from the fact tables in the SpaceX data pipeline.
"""

import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
from dotenv import load_dotenv

import streamlit as st

# Load environment variables if .env file exists
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="SpaceX Data Dashboard",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Snowflake connection parameters
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")


# Function to connect to Snowflake
# @st.cache_resource(ttl=600)
def get_snowflake_connection() -> Optional[snowflake.connector.SnowflakeConnection]:
    """Create a connection to Snowflake"""
    try:
        conn = snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            role=SNOWFLAKE_ROLE,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None


# Function to execute a query and return a DataFrame
# @st.cache_data(ttl=600)
def run_query(query: str) -> pd.DataFrame:
    """Execute a query and return the results as a DataFrame"""
    conn = get_snowflake_connection()
    if conn:
        try:
            # Use cursor to execute query instead of pd.read_sql
            cursor = conn.cursor()
            cursor.execute(query)

            # Fetch column names and data
            columns = [col[0] for col in cursor.description]
            data = cursor.fetchall()

            # Close cursor
            cursor.close()

            # Create DataFrame from data
            df = pd.DataFrame(data, columns=columns)
            return df
        except Exception as e:
            st.error(f"Error executing query: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    return pd.DataFrame()


# Function to load launch data
def load_launch_data(limit: int = 1000) -> pd.DataFrame:
    """Load data from the launch fact table"""
    query = f"""
    SELECT
        LAUNCH_ID,
        LAUNCH_FLIGHT_NUMBER,
        LAUNCH_MISSION_NAME,
        LAUNCH_DATE_UTC,
        LAUNCH_IS_SUCCESS,
        LAUNCH_MISSION_DETAILS,
        CORE_COUNT,
        REUSED_CORE_COUNT,
        SUCCESSFUL_LANDINGS,
        CREW_COUNT,
        PAYLOAD_COUNT,
        TOTAL_PAYLOAD_MASS_KG,
        SHIP_COUNT
    FROM PBL_SPACEX_DATA_FCT_LAUNCH
    ORDER BY LAUNCH_DATE_UTC DESC
    LIMIT {limit}
    """
    return run_query(query)


# Function to load payload data
def load_payload_data(limit: int = 1000) -> pd.DataFrame:
    """Load data from the payload fact table"""
    query = f"""
    SELECT
        PAYLOAD_ID,
        PAYLOAD_LAUNCH_ID,
        PAYLOAD_TYPE,
        PAYLOAD_MASS_KG,
        PAYLOAD_ORBIT,
        LAUNCH_DATE_UTC,
        LAUNCH_IS_SUCCESS,
        PAYLOAD_SUCCESSFUL_MASS_KG,
        PAYLOAD_SUCCESSFUL_DELIVERY
    FROM PBL_SPACEX_DATA_FCT_PAYLOAD
    ORDER BY LAUNCH_DATE_UTC DESC
    LIMIT {limit}
    """
    return run_query(query)


# Function to load Starlink data
def load_starlink_data(limit: int = 1000) -> pd.DataFrame:
    """Load data from the Starlink fact table"""
    query = f"""
    SELECT
        STARLINK_ID,
        STARLINK_LAUNCH_ID,
        STARLINK_SATELLITE_VERSION,
        STARLINK_HEIGHT_KM,
        STARLINK_LATITUDE,
        STARLINK_LONGITUDE,
        STARLINK_VELOCITY_KMS,
        LAUNCH_DATE_UTC,
        LAUNCH_IS_SUCCESS,
        STARLINK_ORBITAL_STATUS,
        VELOCITY_STATUS
    FROM PBL_SPACEX_DATA_FCT_STARLINK
    LIMIT {limit}
    """
    return run_query(query)


# Sidebar for navigation
st.sidebar.title("SpaceX Data Dashboard")
st.sidebar.image(
    "https://upload.wikimedia.org/wikipedia/commons/thumb/2/2e/SpaceX_logo_black.svg/320px-SpaceX_logo_black.svg.png",
    width=200,
)

# Data selection
data_option = st.sidebar.selectbox(
    "Select Data Source", ["Launch Data", "Payload Data", "Starlink Data"]
)

# Environment selection
environment = st.sidebar.selectbox(
    "Select Environment", ["Development", "UAT", "Production"], index=0
)

# Apply environment settings
if environment == "Development":
    SNOWFLAKE_DATABASE = "SPACEX_DATA_DEV"
    SNOWFLAKE_SCHEMA = "PBL_SPACEX_DATA"
    SNOWFLAKE_WAREHOUSE = "SPACEX_DATA_DEV_AD_HOC_WH"
elif environment == "UAT":
    SNOWFLAKE_DATABASE = "SPACEX_DATA_UAT"
    SNOWFLAKE_SCHEMA = "PBL_SPACEX_DATA"
    SNOWFLAKE_WAREHOUSE = "SPACEX_DATA_UAT_AD_HOC_WH"
elif environment == "Production":
    SNOWFLAKE_DATABASE = "SPACEX_DATA_PRD"
    SNOWFLAKE_SCHEMA = "PBL_SPACEX_DATA"
    SNOWFLAKE_WAREHOUSE = "SPACEX_DATA_PRD_HOC_WH"

# Data limit slider
data_limit = st.sidebar.slider("Data Limit", 100, 5000, 1000, 100)

# Date range filter (if applicable)
if data_option in ["Launch Data", "Payload Data"]:
    st.sidebar.subheader("Date Range Filter")
    use_date_filter = st.sidebar.checkbox("Filter by Date", value=False)

    if use_date_filter:
        # Default to last 5 years
        default_start_date = datetime.now() - timedelta(days=5 * 365)
        default_end_date = datetime.now()

        start_date = st.sidebar.date_input("Start Date", default_start_date)
        end_date = st.sidebar.date_input("End Date", default_end_date)

# Main content based on selection
if data_option == "Launch Data":
    st.title("SpaceX Launch Dashboard")

    # Load data
    with st.spinner("Loading launch data..."):
        df_launch = load_launch_data(data_limit)

    if df_launch.empty:
        st.error("No launch data available or error connecting to database.")
    else:
        # Convert date column
        df_launch["LAUNCH_DATE_UTC"] = pd.to_datetime(df_launch["LAUNCH_DATE_UTC"])

        # Apply date filter if selected
        if "use_date_filter" in locals() and use_date_filter:
            df_launch = df_launch[
                (df_launch["LAUNCH_DATE_UTC"].dt.date >= start_date)
                & (df_launch["LAUNCH_DATE_UTC"].dt.date <= end_date)
            ]

        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Launches", df_launch.shape[0])
        with col2:
            success_rate = (
                df_launch["LAUNCH_IS_SUCCESS"].sum() / df_launch.shape[0]
            ) * 100
            st.metric("Success Rate", f"{success_rate:.1f}%")
        with col3:
            total_payload = df_launch["TOTAL_PAYLOAD_MASS_KG"].sum()
            st.metric("Total Payload Mass", f"{total_payload:,.0f} kg")
        with col4:
            reuse_rate = (
                df_launch["REUSED_CORE_COUNT"].sum() / df_launch["CORE_COUNT"].sum()
            ) * 100
            st.metric("Core Reuse Rate", f"{reuse_rate:.1f}%")

        # Timeline of launches
        st.subheader("Launch Timeline")

        # Create a timeline chart with Plotly
        fig = px.scatter(
            df_launch,
            x="LAUNCH_DATE_UTC",
            y="LAUNCH_MISSION_NAME",
            color="LAUNCH_IS_SUCCESS",
            size="TOTAL_PAYLOAD_MASS_KG",
            hover_name="LAUNCH_MISSION_NAME",
            hover_data=[
                "LAUNCH_FLIGHT_NUMBER",
                "PAYLOAD_COUNT",
                "TOTAL_PAYLOAD_MASS_KG",
            ],
            color_discrete_map={True: "green", False: "red"},
            labels={
                "LAUNCH_DATE_UTC": "Launch Date",
                "LAUNCH_MISSION_NAME": "Mission",
                "LAUNCH_IS_SUCCESS": "Success",
                "TOTAL_PAYLOAD_MASS_KG": "Payload Mass (kg)",
            },
            height=500,
        )

        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

        # Launch success over time
        st.subheader("Launch Success Rate Over Time")

        # Group by year and calculate success rate
        df_launch["LAUNCH_YEAR"] = df_launch["LAUNCH_DATE_UTC"].dt.year
        yearly_success = (
            df_launch.groupby("LAUNCH_YEAR")
            .agg(
                success_count=("LAUNCH_IS_SUCCESS", "sum"),
                total_launches=("LAUNCH_ID", "count"),
            )
            .reset_index()
        )
        yearly_success["success_rate"] = (
            yearly_success["success_count"] / yearly_success["total_launches"] * 100
        )

        # Create a bar chart with success rate by year
        fig = px.bar(
            yearly_success,
            x="LAUNCH_YEAR",
            y=[
                "success_count",
                yearly_success["total_launches"] - yearly_success["success_count"],
            ],
            labels={"value": "Count", "LAUNCH_YEAR": "Year", "variable": "Outcome"},
            title="Launches by Year",
            color_discrete_sequence=["green", "red"],
        )

        # Add success rate line
        fig.add_trace(
            go.Scatter(
                x=yearly_success["LAUNCH_YEAR"],
                y=yearly_success["success_rate"],
                mode="lines+markers",
                name="Success Rate (%)",
                yaxis="y2",
                line=dict(color="blue", width=3),
            )
        )

        # Update layout for dual y-axis
        fig.update_layout(
            yaxis2=dict(
                title="Success Rate (%)", overlaying="y", side="right", range=[0, 100]
            ),
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
        )

        st.plotly_chart(fig, use_container_width=True)

        # Payload distribution
        st.subheader("Payload Distribution by Launch")

        fig = px.bar(
            df_launch.sort_values("TOTAL_PAYLOAD_MASS_KG", ascending=False).head(20),
            x="LAUNCH_MISSION_NAME",
            y="TOTAL_PAYLOAD_MASS_KG",
            color="LAUNCH_IS_SUCCESS",
            color_discrete_map={True: "green", False: "red"},
            labels={
                "LAUNCH_MISSION_NAME": "Mission",
                "TOTAL_PAYLOAD_MASS_KG": "Payload Mass (kg)",
                "LAUNCH_IS_SUCCESS": "Success",
            },
            title="Top 20 Missions by Payload Mass",
        )

        fig.update_layout(xaxis={"categoryorder": "total descending"})
        st.plotly_chart(fig, use_container_width=True)

        # Raw data table
        with st.expander("View Raw Launch Data"):
            st.dataframe(df_launch)

elif data_option == "Payload Data":
    st.title("SpaceX Payload Dashboard")

    # Load data
    with st.spinner("Loading payload data..."):
        df_payload = load_payload_data(data_limit)

    if df_payload.empty:
        st.error("No payload data available or error connecting to database.")
    else:
        # Convert date column
        df_payload["LAUNCH_DATE_UTC"] = pd.to_datetime(df_payload["LAUNCH_DATE_UTC"])

        # Apply date filter if selected
        if "use_date_filter" in locals() and use_date_filter:
            df_payload = df_payload[
                (df_payload["LAUNCH_DATE_UTC"].dt.date >= start_date)
                & (df_payload["LAUNCH_DATE_UTC"].dt.date <= end_date)
            ]

        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Payloads", df_payload.shape[0])
        with col2:
            success_rate = (
                df_payload["PAYLOAD_SUCCESSFUL_DELIVERY"].sum() / df_payload.shape[0]
            ) * 100
            st.metric("Delivery Success Rate", f"{success_rate:.1f}%")
        with col3:
            total_mass = df_payload["PAYLOAD_MASS_KG"].sum()
            st.metric("Total Payload Mass", f"{total_mass:,.0f} kg")
        with col4:
            successful_mass = df_payload["PAYLOAD_SUCCESSFUL_MASS_KG"].sum()
            st.metric("Successfully Delivered Mass", f"{successful_mass:,.0f} kg")

        # Payload types distribution
        st.subheader("Payload Types Distribution")

        payload_types = df_payload["PAYLOAD_TYPE"].value_counts().reset_index()
        payload_types.columns = ["PAYLOAD_TYPE", "COUNT"]

        fig = px.pie(
            payload_types,
            values="COUNT",
            names="PAYLOAD_TYPE",
            title="Distribution of Payload Types",
            hole=0.4,
        )

        st.plotly_chart(fig, use_container_width=True)

        # Payload orbits distribution
        st.subheader("Payload Orbits Distribution")

        payload_orbits = df_payload["PAYLOAD_ORBIT"].value_counts().reset_index()
        payload_orbits.columns = ["PAYLOAD_ORBIT", "COUNT"]

        fig = px.bar(
            payload_orbits,
            x="PAYLOAD_ORBIT",
            y="COUNT",
            color="PAYLOAD_ORBIT",
            labels={"PAYLOAD_ORBIT": "Orbit", "COUNT": "Number of Payloads"},
            title="Distribution of Payload Orbits",
        )

        fig.update_layout(xaxis={"categoryorder": "total descending"})
        st.plotly_chart(fig, use_container_width=True)

        # Payload mass by type
        st.subheader("Payload Mass by Type")

        payload_mass_by_type = (
            df_payload.groupby("PAYLOAD_TYPE")
            .agg(
                total_mass=("PAYLOAD_MASS_KG", "sum"),
                successful_mass=("PAYLOAD_SUCCESSFUL_MASS_KG", "sum"),
                count=("PAYLOAD_ID", "count"),
            )
            .reset_index()
        )

        payload_mass_by_type["avg_mass"] = (
            payload_mass_by_type["total_mass"] / payload_mass_by_type["count"]
        )
        payload_mass_by_type = payload_mass_by_type.sort_values(
            "total_mass", ascending=False
        )

        fig = px.bar(
            payload_mass_by_type,
            x="PAYLOAD_TYPE",
            y=[
                "successful_mass",
                payload_mass_by_type["total_mass"]
                - payload_mass_by_type["successful_mass"],
            ],
            labels={
                "PAYLOAD_TYPE": "Payload Type",
                "value": "Total Mass (kg)",
                "variable": "Delivery Status",
            },
            title="Payload Mass by Type",
            color_discrete_sequence=["green", "red"],
        )

        # Add average mass line
        fig.add_trace(
            go.Scatter(
                x=payload_mass_by_type["PAYLOAD_TYPE"],
                y=payload_mass_by_type["avg_mass"],
                mode="lines+markers",
                name="Average Mass per Payload (kg)",
                yaxis="y2",
                line=dict(color="blue", width=3),
            )
        )

        # Update layout for dual y-axis
        fig.update_layout(
            yaxis2=dict(title="Average Mass (kg)", overlaying="y", side="right"),
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
        )

        st.plotly_chart(fig, use_container_width=True)

        # Payload success over time
        st.subheader("Payload Delivery Success Over Time")

        # Group by year and calculate success rate
        df_payload["LAUNCH_YEAR"] = df_payload["LAUNCH_DATE_UTC"].dt.year
        yearly_success = (
            df_payload.groupby("LAUNCH_YEAR")
            .agg(
                success_count=("PAYLOAD_SUCCESSFUL_DELIVERY", "sum"),
                total_payloads=("PAYLOAD_ID", "count"),
                successful_mass=("PAYLOAD_SUCCESSFUL_MASS_KG", "sum"),
                total_mass=("PAYLOAD_MASS_KG", "sum"),
            )
            .reset_index()
        )

        yearly_success["success_rate"] = (
            yearly_success["success_count"] / yearly_success["total_payloads"] * 100
        )
        yearly_success["mass_success_rate"] = (
            yearly_success["successful_mass"] / yearly_success["total_mass"] * 100
        )

        # Create a bar chart with success rate by year
        fig = px.bar(
            yearly_success,
            x="LAUNCH_YEAR",
            y=[
                "success_count",
                yearly_success["total_payloads"] - yearly_success["success_count"],
            ],
            labels={"value": "Count", "LAUNCH_YEAR": "Year", "variable": "Outcome"},
            title="Payload Deliveries by Year",
            color_discrete_sequence=["green", "red"],
        )

        # Add success rate line
        fig.add_trace(
            go.Scatter(
                x=yearly_success["LAUNCH_YEAR"],
                y=yearly_success["success_rate"],
                mode="lines+markers",
                name="Success Rate (%)",
                yaxis="y2",
                line=dict(color="blue", width=3),
            )
        )

        # Update layout for dual y-axis
        fig.update_layout(
            yaxis2=dict(
                title="Success Rate (%)", overlaying="y", side="right", range=[0, 100]
            ),
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
        )

        st.plotly_chart(fig, use_container_width=True)

        # Raw data table
        with st.expander("View Raw Payload Data"):
            st.dataframe(df_payload)

elif data_option == "Starlink Data":
    st.title("SpaceX Starlink Dashboard")

    # Load data
    with st.spinner("Loading Starlink data..."):
        df_starlink = load_starlink_data(data_limit)

    if df_starlink.empty:
        st.error("No Starlink data available or error connecting to database.")
    else:
        # Convert date column
        df_starlink["LAUNCH_DATE_UTC"] = pd.to_datetime(df_starlink["LAUNCH_DATE_UTC"])

        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Satellites", df_starlink.shape[0])
        with col2:
            operational_count = df_starlink[
                df_starlink["STARLINK_ORBITAL_STATUS"] == "Operational"
            ].shape[0]
            operational_pct = (operational_count / df_starlink.shape[0]) * 100
            st.metric(
                "Operational Satellites",
                f"{operational_count} ({operational_pct:.1f}%)",
            )
        with col3:
            avg_height = df_starlink["STARLINK_HEIGHT_KM"].mean()
            st.metric("Average Orbital Height", f"{avg_height:.1f} km")
        with col4:
            avg_velocity = df_starlink["STARLINK_VELOCITY_KMS"].mean()
            st.metric("Average Velocity", f"{avg_velocity:.2f} km/s")

        # Orbital status distribution
        st.subheader("Satellite Orbital Status Distribution")

        orbital_status = (
            df_starlink["STARLINK_ORBITAL_STATUS"].value_counts().reset_index()
        )
        orbital_status.columns = ["ORBITAL_STATUS", "COUNT"]

        fig = px.pie(
            orbital_status,
            values="COUNT",
            names="ORBITAL_STATUS",
            title="Distribution of Satellite Orbital Status",
            color="ORBITAL_STATUS",
            color_discrete_map={
                "Operational": "green",
                "Below Operational": "orange",
                "Above Operational": "blue",
                "Unknown": "gray",
            },
        )

        st.plotly_chart(fig, use_container_width=True)

        # Velocity status distribution
        st.subheader("Satellite Velocity Status Distribution")

        velocity_status = df_starlink["VELOCITY_STATUS"].value_counts().reset_index()
        velocity_status.columns = ["VELOCITY_STATUS", "COUNT"]

        fig = px.pie(
            velocity_status,
            values="COUNT",
            names="VELOCITY_STATUS",
            title="Distribution of Satellite Velocity Status",
            color="VELOCITY_STATUS",
            color_discrete_map={
                "Nominal": "green",
                "Sub-nominal": "orange",
                "Super-nominal": "red",
                "Unknown": "gray",
            },
        )

        st.plotly_chart(fig, use_container_width=True)

        # Satellite version distribution
        st.subheader("Satellite Version Distribution")

        version_counts = (
            df_starlink["STARLINK_SATELLITE_VERSION"].value_counts().reset_index()
        )
        version_counts.columns = ["VERSION", "COUNT"]

        fig = px.bar(
            version_counts,
            x="VERSION",
            y="COUNT",
            color="VERSION",
            labels={"VERSION": "Satellite Version", "COUNT": "Number of Satellites"},
            title="Distribution of Satellite Versions",
        )

        fig.update_layout(xaxis={"categoryorder": "total descending"})
        st.plotly_chart(fig, use_container_width=True)

        # Height vs. Velocity scatter plot
        st.subheader("Satellite Height vs. Velocity")

        fig = px.scatter(
            df_starlink,
            x="STARLINK_HEIGHT_KM",
            y="STARLINK_VELOCITY_KMS",
            color="STARLINK_ORBITAL_STATUS",
            hover_name="STARLINK_ID",
            hover_data=["STARLINK_SATELLITE_VERSION", "LAUNCH_DATE_UTC"],
            labels={
                "STARLINK_HEIGHT_KM": "Orbital Height (km)",
                "STARLINK_VELOCITY_KMS": "Orbital Velocity (km/s)",
                "STARLINK_ORBITAL_STATUS": "Orbital Status",
            },
            color_discrete_map={
                "Operational": "green",
                "Below Operational": "orange",
                "Above Operational": "blue",
                "Unknown": "gray",
            },
        )

        # Add reference lines for operational boundaries
        fig.add_shape(
            type="rect",
            x0=540,
            x1=560,
            y0=7.5,
            y1=7.8,
            line=dict(color="green", width=2),
            fillcolor="rgba(0,255,0,0.1)",
        )

        fig.add_annotation(
            x=550,
            y=7.65,
            text="Optimal Zone",
            showarrow=False,
            font=dict(color="green"),
        )

        st.plotly_chart(fig, use_container_width=True)

        # Satellite map
        st.subheader("Satellite Positions")

        # Filter out rows with missing lat/long
        df_map = df_starlink.dropna(subset=["STARLINK_LATITUDE", "STARLINK_LONGITUDE"])

        if not df_map.empty:
            # Create a map with satellite positions
            fig = px.scatter_geo(
                df_map,
                lat="STARLINK_LATITUDE",
                lon="STARLINK_LONGITUDE",
                color="STARLINK_ORBITAL_STATUS",
                hover_name="STARLINK_ID",
                hover_data=["STARLINK_SATELLITE_VERSION", "STARLINK_HEIGHT_KM"],
                projection="natural earth",
                color_discrete_map={
                    "Operational": "green",
                    "Below Operational": "orange",
                    "Above Operational": "blue",
                    "Unknown": "gray",
                },
            )

            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No latitude/longitude data available for mapping satellites.")

        # Raw data table
        with st.expander("View Raw Starlink Data"):
            st.dataframe(df_starlink)

# Footer
st.markdown("---")
st.markdown("SpaceX Data Dashboard | Created with Streamlit")
