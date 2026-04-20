import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

# 1. Page Configuration (Makes it look wide and professional)
st.set_page_config(page_title="Transfermarkt Analytics", layout="wide")

# Initialize BigQuery client
client = bigquery.Client()

# --- STEP 1: Get absolute bounds for the slider ---
@st.cache_data
def get_season_bounds():
    query = """
        SELECT MIN(season) as min_season, MAX(season) as max_season 
        FROM `transfermarkt-pipeline.transfermarkt_dwh.fact_player_match_stats`
    """
    bounds_df = client.query(query).to_dataframe()
    # Handle potential nulls and convert to integer
    min_s = int(bounds_df['min_season'].fillna(2010).iloc[0])
    max_s = int(bounds_df['max_season'].fillna(2024).iloc[0])
    return min_s, max_s

min_bound, max_bound = get_season_bounds()

# --- STEP 3: Query the database dynamically based on the slider ---
# Notice we pass the slider values into the function and use them in the WHERE clause
@st.cache_data
def load_filtered_data(start_season, end_season):
    query = f"""
        SELECT match_date, player_name, goals, assists, minutes_played, club_name, league_name, season
        FROM `transfermarkt-pipeline.transfermarkt_dwh.fact_player_match_stats`
        WHERE season >= {start_season} AND season <= {end_season}
        ORDER BY match_date DESC
    """
    df = client.query(query).to_dataframe()
    df['match_date'] = pd.to_datetime(df['match_date'])
    return df

years_list = list(range(min_bound, max_bound + 1))
default_year = 2025
year_index = years_list.index(default_year) if default_year in years_list else len(years_list) - 1
st.sidebar.title("⚽ Transfermarkt football Analytics")
st.sidebar.markdown("Exploring player performance, club dominance, and time-series goal trends.")
st.sidebar.header("🔍 Options")
col1, col2 = st.sidebar.columns(2)
start_season = col1.selectbox(
    "Start Season", 
    years_list, 
    index=year_index, 
    format_func=lambda x: f"{x}-{x+1}"
)
end_season = col2.selectbox(
    "End Season", 
    years_list, 
    index=year_index, 
    format_func=lambda x: f"{x}-{x+1}"
)
filtered_df = load_filtered_data(start_season, end_season)


# --- FILTER 2: LEAGUE (Using the dynamically queried data) ---
league_list = ["All Leagues"] + sorted(filtered_df['league_name'].dropna().unique())

# Find the index for "premier-league" if it exists, otherwise default to 0 ("All Leagues")
default_league = "premier-league"
league_index = league_list.index(default_league) if default_league in league_list else 0

selected_league = st.sidebar.selectbox("Filter by League", league_list, index=league_index)

if selected_league != "All Leagues":
    filtered_df = filtered_df[filtered_df['league_name'] == selected_league]

# --- FILTER 3: CLUB ---
club_list = ["All Clubs"] + sorted(filtered_df['club_name'].dropna().unique())

# "All Clubs" is explicitly set as the default by keeping index=0
selected_club = st.sidebar.selectbox("Filter by Club", club_list, index=0)

if selected_club != "All Clubs":
    filtered_df = filtered_df[filtered_df['club_name'] == selected_club]

st.sidebar.divider()
st.sidebar.markdown("### 🏆 Top 5 Scoring Players")
top_players = filtered_df.groupby('player_name')['goals'].sum().sort_values(ascending=False).head(5)
st.sidebar.table(top_players)
st.sidebar.divider()

st.sidebar.markdown("### 📊 High-Level Metrics")
col1, col2, col3 = st.sidebar.columns(3)

total_goals = int(filtered_df['goals'].sum())
total_assists = int(filtered_df['assists'].sum())
matches_analyzed = len(filtered_df)

col1.metric(label="Matches Analyzed", value=matches_analyzed)
col2.metric(label="Total Goals", value=total_goals)
col3.metric(label="Total Assists", value=total_assists)


# Create two columns for a side-by-side layout
col1, col2 = st.columns(2)

# ==========================================
# LEFT COLUMN: League Overview
# ==========================================
with col1:
    st.markdown(f"### 🌍 League Overview: {selected_league}")
    
    # 1. Bypass the Club Filter: Re-filter the data to include ALL clubs in the selected league
    league_comparison_df = load_filtered_data(start_season, end_season)
    
    if selected_league != "All Leagues":
        league_comparison_df = league_comparison_df[league_comparison_df['league_name'] == selected_league]
        
        # Aggregate total goals and assists for every club in the league
        league_stats = league_comparison_df.groupby('club_name')[['goals', 'assists']].sum().reset_index()
        
        # Sort by total offensive output
        league_stats['total_output'] = league_stats['goals'] + league_stats['assists']
        league_stats = league_stats.sort_values('total_output', ascending=True) # Ascending so biggest is on top
        
        # Build a Stacked Horizontal Bar Chart
        fig_league_bar = px.bar(
            league_stats,
            y='club_name',
            x=['goals', 'assists'],
            title="Total Offensive Firepower",
            labels={'value': 'Total Count', 'club_name': 'Club', 'variable': 'Metric'},
            orientation='h', 
            color_discrete_sequence=["#1f77b4", "#ff7f0e"] 
        )
        
        fig_league_bar.update_layout(plot_bgcolor="rgba(0,0,0,0)", legend_title_text="Stat")
        st.plotly_chart(fig_league_bar, use_container_width=True)
    else:
        st.info("No data available for the selected league and season range.")

# ==========================================
# RIGHT COLUMN: Player Efficiency
# ==========================================
with col2:
    st.markdown("### 🎯 Player Efficiency")
    
    # 1. Aggregate stats at the player level
    player_stats = filtered_df.groupby(['player_name', 'club_name']).agg(
        total_goals=('goals', 'sum'),
        total_assists=('assists', 'sum'),
        total_minutes=('minutes_played', 'sum')
    ).reset_index()
    
    # 2. Create the "Insight" Feature: Total Contributions
    player_stats['goal_contributions'] = player_stats['total_goals'] + player_stats['total_assists']
    
    # 3. Filter out the noise (players with less than 90 minutes total are statistically irrelevant)
    player_stats = player_stats[player_stats['total_minutes'] >= 90]
    
    # 4. Build an interactive Bubble Chart
    fig_efficiency = px.scatter(
        player_stats,
        x='total_minutes',
        y='goal_contributions',
        size='total_goals',          # The size of the bubble shows pure goals
        color='club_name',           # Colors distinguish the teams
        hover_name='player_name',
        hover_data={
            'total_minutes': True, 
            'total_goals': True, 
            'total_assists': True, 
            'goal_contributions': False, 
            'club_name': False
        },
        title="Impact vs. Time on Pitch",
        labels={
            'total_minutes': 'Total Minutes Played', 
            'goal_contributions': 'Goal Contributions'
        }
    )
    
    # 5. Clean up the design
    fig_efficiency.update_layout(plot_bgcolor="rgba(0,0,0,0)")
    
    # Add a subtle diagonal line to represent a baseline 1-to-1 ratio (optional visual guide)
    if not player_stats.empty: # Safety check before getting max values
        fig_efficiency.add_shape(
            type="line", line=dict(dash='dash', color="gray", width=1),
            x0=0, y0=0, 
            x1=player_stats['total_minutes'].max(), 
            y1=player_stats['goal_contributions'].max()
        )
    
    st.plotly_chart(fig_efficiency, use_container_width=True)
# --- 7. Player Efficiency Analysis ---
st.divider()

st.markdown("### 📈 Scoring Volatility & Anomaly Detection")

# 1. Group by date and calculate daily goals
daily_goals = filtered_df.groupby('match_date')['goals'].sum().reset_index()
daily_goals = daily_goals.sort_values('match_date')

# 2. Calculate Rolling Mean and Rolling Standard Deviation (14-day window for football)
window = 14 
daily_goals['rolling_mean'] = daily_goals['goals'].rolling(window=window, min_periods=1).mean()
daily_goals['rolling_std'] = daily_goals['goals'].rolling(window=window, min_periods=1).std().fillna(0)

# 3. Create Upper and Lower Control Bounds (95% Confidence Interval)
daily_goals['upper_band'] = daily_goals['rolling_mean'] + (1.96 * daily_goals['rolling_std'])
daily_goals['lower_band'] = daily_goals['rolling_mean'] - (1.96 * daily_goals['rolling_std'])
daily_goals['lower_band'] = daily_goals['lower_band'].clip(lower=0) # Goals can't go below zero

# 4. Identify Statistical Anomalies (Days where goals exceeded the upper band)
anomalies = daily_goals[daily_goals['goals'] > daily_goals['upper_band']]

# 5. Build the Advanced Plotly Chart
fig_ts = go.Figure()

# Add the shaded area for Expected Variance
fig_ts.add_trace(go.Scatter(
    x=daily_goals['match_date'].tolist() + daily_goals['match_date'].tolist()[::-1],
    y=daily_goals['upper_band'].tolist() + daily_goals['lower_band'].tolist()[::-1],
    fill='toself',
    fillcolor='rgba(173, 216, 230, 0.2)', # Light blue shading
    line=dict(color='rgba(255,255,255,0)'),
    hoverinfo="skip",
    showlegend=True,
    name='Expected Range (95% CI)'
))

# Add the actual Daily Goals line
fig_ts.add_trace(go.Scatter(
    x=daily_goals['match_date'], 
    y=daily_goals['goals'],
    mode='lines',
    line=dict(color='#1f77b4', width=2),
    name='Actual Daily Goals'
))

# Add the Rolling Mean line
fig_ts.add_trace(go.Scatter(
    x=daily_goals['match_date'], 
    y=daily_goals['rolling_mean'],
    mode='lines',
    line=dict(color='#ff7f0e', width=2, dash='dash'),
    name=f'{window}-Day Moving Avg'
))

# Overlay the Anomalies as bright red dots
fig_ts.add_trace(go.Scatter(
    x=anomalies['match_date'], 
    y=anomalies['goals'],
    mode='markers',
    marker=dict(color='red', size=8, symbol='circle-open', line=dict(width=2)),
    name='Scoring Anomaly'
))

fig_ts.update_xaxes(range=[datetime.strptime(str(start_season), "%Y"), datetime.strptime(str(end_season + 2), "%Y")])

fig_ts.update_layout(
    plot_bgcolor="rgba(0,0,0,0)",
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
)

st.plotly_chart(fig_ts, use_container_width=True)
st.divider()