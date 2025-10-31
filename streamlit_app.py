"""
⚽ Premier League 2024-2025 Football Dashboard
Simple Streamlit Application with PostgreSQL Connection
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, MetaData, Table, select, func, desc, case

# --- Configuration for the Streamlit Page ---
st.set_page_config(
    page_title="Football Data Insights", # Changed page title
    page_icon="📈", # Changed the icon
    layout="wide"
)

# ============================================
# 1. Database Connection
# ============================================

@st.cache_resource
def establish_db_connection():
    """Connects to the PostgreSQL database"""
    try:
        # NOTE: Keep the original DB_URL for functionality
        DB_URL = "postgresql+psycopg2://postgres:kaoutar2002@localhost:5432/football_db"

        engine = create_engine(DB_URL)
        metadata = MetaData()
        
        # Load tables - using different names for tables dictionary keys
        season_table = Table("saison", metadata, autoload_with=engine)
        competition_table = Table("competition", metadata, autoload_with=engine)
        team_table = Table("team", metadata, autoload_with=engine)
        player_table = Table("player", metadata, autoload_with=engine)
        match_table = Table("match", metadata, autoload_with=engine)
        result_table = Table("match_result", metadata, autoload_with=engine)
        stats_table = Table("player_statistics", metadata, autoload_with=engine)
        
        return engine, {
            "seasons": season_table,
            "comps": competition_table,
            "teams": team_table,
            "players": player_table,
            "matches": match_table,
            "match_results": result_table,
            "player_stats": stats_table
        }
    except Exception as e:
        st.error(f"Database connection error: {e}") # Translation
        return None, None

# Initial Connection
db_engine, tables_map = establish_db_connection()

if db_engine is None:
    st.stop()

# ============================================
# 2. Data Retrieval Functions
# ============================================

def fetch_top_goalscorers(limit_num=10, team_filter=None): # Changed function name and argument name
    """Fetches the top N goalscorers""" # Translation
    stmt = select(
        tables_map["players"].c.Player,
        tables_map["teams"].c.team_name,
        func.sum(tables_map["player_stats"].c.Gls).label("total_goals") # Changed label
    ).join(
        tables_map["player_stats"], 
        tables_map["player_stats"].c.player_id == tables_map["players"].c.player_id
    ).join(
        tables_map["teams"], 
        tables_map["players"].c.team_id == tables_map["teams"].c.team_id
    )
    
    if team_filter:
        stmt = stmt.where(tables_map["teams"].c.team_name == team_filter)
    
    stmt = stmt.group_by(
        tables_map["players"].c.Player, 
        tables_map["teams"].c.team_name
    ).order_by(
        desc("total_goals")
    ).limit(limit_num) # Changed variable name
    
    with db_engine.connect() as conn: # Changed variable name
        results = conn.execute(stmt).fetchall()
    
    df = pd.DataFrame(results, columns=["Player_Name", "Club", "Goals"]) # Changed column names
    return df

def fetch_most_decisive_players(limit_num=10, team_filter=None): # Changed function name and argument name
    """Fetches the most decisive players (Goals + Assists)""" # Translation
    stmt = select(
        tables_map["players"].c.Player,
        tables_map["teams"].c.team_name,
        (func.sum(tables_map["player_stats"].c.Gls) + func.sum(tables_map["player_stats"].c.Ast)).label("decisive_actions") # Changed label
    ).join(
        tables_map["player_stats"], 
        tables_map["players"].c.player_id == tables_map["player_stats"].c.player_id
    ).join(
        tables_map["teams"], 
        tables_map["players"].c.team_id == tables_map["teams"].c.team_id
    )
    
    if team_filter:
        stmt = stmt.where(tables_map["teams"].c.team_name == team_filter)
    
    stmt = stmt.group_by(
        tables_map["players"].c.Player, 
        tables_map["teams"].c.team_name
    ).order_by(
        desc("decisive_actions")
    ).limit(limit_num) # Changed variable name
    
    with db_engine.connect() as conn: # Changed variable name
        results = conn.execute(stmt).fetchall()
    
    df = pd.DataFrame(results, columns=["Player_Name", "Club", "Total_Decisive"]) # Changed column names
    return df

def fetch_total_goals_by_team(): # Changed function name
    """Fetches the total goals scored by each team""" # Translation
    stmt = select(
        tables_map["teams"].c.team_name,
        func.sum(tables_map["player_stats"].c.Gls).label("total_goals_scored") # Changed label
    ).join(
        tables_map["players"], 
        tables_map["players"].c.team_id == tables_map["teams"].c.team_id
    ).join(
        tables_map["player_stats"], 
        tables_map["player_stats"].c.player_id == tables_map["players"].c.player_id
    ).group_by(
        tables_map["teams"].c.team_name
    ).order_by(
        desc("total_goals_scored")
    )
    
    with db_engine.connect() as conn: # Changed variable name
        results = conn.execute(stmt).fetchall()
    
    df = pd.DataFrame(results, columns=["Club", "Goals_Scored"]) # Changed column names
    return df

def fetch_league_standings(): # Changed function name
    """Fetches the team league standings (simplified points)""" # Translation
    stmt = select(
        tables_map["teams"].c.team_name,
        func.sum(
            case(
                (tables_map["match_results"].c.Result == 'W', 3),
                (tables_map["match_results"].c.Result == 'D', 1),
                else_=0
            )
        ).label("league_points") # Changed label
    ).join(
        tables_map["matches"], 
        tables_map["matches"].c.team_id == tables_map["teams"].c.team_id
    ).join(
        tables_map["match_results"], 
        tables_map["match_results"].c.match_id == tables_map["matches"].c.match_id
    ).group_by(
        tables_map["teams"].c.team_name
    ).order_by(
        desc("league_points")
    )
    
    with db_engine.connect() as conn: # Changed variable name
        results = conn.execute(stmt).fetchall()
    
    df = pd.DataFrame(results, columns=["Club", "Points"]) # Changed column names
    return df

def fetch_attack_vs_defense_stats(): # Changed function name
    """Fetches average goals scored and conceded per team""" # Translation
    stmt = select(
        tables_map["teams"].c.team_name,
        func.avg(tables_map["match_results"].c.GF).label("avg_goals_for"), # Changed label
        func.avg(tables_map["match_results"].c.GA).label("avg_goals_against") # Changed label
    ).join(
        tables_map["matches"], 
        tables_map["matches"].c.team_id == tables_map["teams"].c.team_id
    ).join(
        tables_map["match_results"], 
        tables_map["match_results"].c.match_id == tables_map["matches"].c.match_id
    ).group_by(
        tables_map["teams"].c.team_name
    )
    
    with db_engine.connect() as conn: # Changed variable name
        results = conn.execute(stmt).fetchall()
    
    df = pd.DataFrame(results, columns=["Club", "Avg_Goals_For", "Avg_Goals_Against"]) # Changed column names
    return df

def fetch_best_defense_ranking(): # Changed function name
    """Fetches teams with the best defense (lowest goals conceded)""" # Translation
    stmt = select(
        tables_map["teams"].c.team_name,
        func.sum(tables_map["match_results"].c.GA).label("total_goals_conceded") # Changed label
    ).join(
        tables_map["matches"], 
        tables_map["matches"].c.team_id == tables_map["teams"].c.team_id
    ).join(
        tables_map["match_results"], 
        tables_map["match_results"].c.match_id == tables_map["matches"].c.match_id
    ).group_by(
        tables_map["teams"].c.team_name
    ).order_by(
        "total_goals_conceded" # Sorting by new label
    )
    
    with db_engine.connect() as conn: # Changed variable name
        results = conn.execute(stmt).fetchall()
    
    df = pd.DataFrame(results, columns=["Club", "Goals_Conceded"]) # Changed column names
    return df

def fetch_player_nationalities(team_filter=None): # Changed function name and argument name
    """Fetches the distribution of player nationalities""" # Translation
    stmt = select(
        tables_map["teams"].c.team_name,
        tables_map["players"].c.Nation,
        func.count(tables_map["players"].c.player_id).label("player_count") # Changed label
    ).join(
        tables_map["players"], 
        tables_map["players"].c.team_id == tables_map["teams"].c.team_id
    )
    
    if team_filter:
        stmt = stmt.where(tables_map["teams"].c.team_name == team_filter)
    
    stmt = stmt.group_by(
        tables_map["teams"].c.team_name, 
        tables_map["players"].c.Nation
    ).order_by(
        tables_map["teams"].c.team_name, 
        func.count(tables_map["players"].c.player_id).desc()
    )
    
    with db_engine.connect() as conn: # Changed variable name
        results = conn.execute(stmt).fetchall()
    
    df = pd.DataFrame(results, columns=["Club", "Nationality", "Count"]) # Changed column names
    return df

def get_all_teams_list(): # Changed function name
    """Fetches the list of all teams""" # Translation
    stmt = select(tables_map["teams"].c.team_name).order_by(tables_map["teams"].c.team_name)
    
    with db_engine.connect() as conn: # Changed variable name
        results = conn.execute(stmt).fetchall()
    
    return [row[0] for row in results]

# ============================================
# 3. Plotting Functions
# ============================================

def create_bar_chart(df, x_column, y_column, title_text, color_column=None): # Changed function name and argument names
    """Creates a simple horizontal bar chart""" # Translation
    if color_column:
        fig = px.bar(df, x=x_column, y=y_column, color=color_column, orientation='h', title=title_text,
                     color_discrete_sequence=px.colors.qualitative.Dark24) # Changed color palette
    else:
        fig = px.bar(df, x=x_column, y=y_column, orientation='h', title=title_text,
                     color_discrete_sequence=px.colors.qualitative.Dark24) # Changed color palette
        
    fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=450, 
                      plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)') # Adjusted styling
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(200,200,200,0.5)')
    return fig

def create_scatter_plot(df, x_column, y_column, title_text): # Changed function name and argument names
    """Creates a simple scatter plot""" # Translation
    fig = px.scatter(df, x=x_column, y=y_column, size=x_column, hover_data=["Club"], title=title_text,
                     color='Club', size_max=40, color_discrete_sequence=px.colors.qualitative.Vivid) # Added color, size_max, and changed color palette
    fig.update_layout(height=450, plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)') # Adjusted styling
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(200,200,200,0.5)')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(200,200,200,0.5)')
    return fig

# ============================================
# 4. Main Interface
# ============================================

# Using a slightly different title and header
st.title("Premier League Insights 🏆") 
st.subheader("2024-2025 Season")
st.markdown("---")

# Navigation Menu
selected_page = st.sidebar.selectbox( # Changed variable name
    "Navigate to a Section", # Translation
    ["Overview", "Player Statistics", "Team Performance"] # Changed section names
)

# List of teams for filters
all_teams_list = get_all_teams_list() # Changed function name

# ============================================
# Overview Page
# ============================================
if selected_page == "Overview":
    st.header("Dashboard Overview") # Translation
    
    # Fetch some key metrics
    current_standings = fetch_league_standings() # Changed function name
    goals_per_team = fetch_total_goals_by_team() # Changed function name
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Clubs", len(all_teams_list)) # Translation
    with col2:
        st.metric("League Leader", current_standings.iloc[0]["Club"] if len(current_standings) > 0 else "N/A") # Translation
    with col3:
        st.metric("Leader's Points", int(current_standings.iloc[0]["Points"]) if len(current_standings) > 0 else 0) # Translation
    with col4:
        st.metric("Total Goals Scored", int(goals_per_team["Goals_Scored"].sum())) # Translation and column name
    
    st.markdown("---")
    st.info("Use the sidebar menu to explore detailed statistics.") # Changed text and used st.info
    
    # Display simplified table for the overview
    st.subheader("Current League Standings Snapshot") # Translation
    st.dataframe(current_standings.head(5), use_container_width=True)


# ============================================
# Player Statistics Page
# ============================================
elif selected_page == "Player Statistics":
    st.header("Player Performance Metrics") # Translation
    
    # Filters
    st.subheader("Data Filters") # Translation
    col1, col2 = st.columns(2)
    with col1:
        team_choice = st.selectbox("Filter by Club", ["All Clubs"] + all_teams_list) # Translation
    with col2:
        player_limit = st.slider("Number of Players to Display", 5, 20, 10) # Translation and variable name
    
    # Apply filter
    selected_team = None if team_choice == "All Clubs" else team_choice # Changed variable name
    
    # Retrieve data
    top_scorers_data = fetch_top_goalscorers(player_limit, selected_team) # Changed function name and variable name
    decisive_players_data = fetch_most_decisive_players(player_limit, selected_team) # Changed function name and variable name
    nationality_data = fetch_player_nationalities(selected_team) # Changed function name and variable name
    
    # Charts
    st.subheader("Visual Analysis") # Translation
    
    tab1, tab2, tab3 = st.tabs(["Top Goalscorers", "Decisive Players", "Nationality Breakdown"]) # Translation
    
    with tab1:
        fig1 = create_bar_chart(top_scorers_data, "Goals", "Player_Name", "Top Goalscorers Ranking", "Club") # Changed function name and column names
        st.plotly_chart(fig1, use_container_width=True)
    
    with tab2:
        fig2 = create_bar_chart(decisive_players_data, "Total_Decisive", "Player_Name", "Decisive Players (Goals + Assists)", "Club") # Changed function name and column names
        st.plotly_chart(fig2, use_container_width=True)
    
    with tab3:
        # Aggregate by nationality
        nat_agg_data = nationality_data.groupby("Nationality")["Count"].sum().reset_index() # Changed variable and column names
        nat_agg_data = nat_agg_data.sort_values("Count", ascending=False).head(15)
        fig3 = create_bar_chart(nat_agg_data, "Count", "Nationality", "Top 15 Nationalities") # Changed function name and column names
        st.plotly_chart(fig3, use_container_width=True)
    
    # Interactive Table
    st.subheader("Filtered Data - Top Goalscorers") # Translation
    st.dataframe(top_scorers_data, use_container_width=True, height=300)
    
    # Download Button
    csv_export_data = top_scorers_data.to_csv(index=False).encode('utf-8') # Changed variable name
    st.download_button(
        label="Download Data as CSV", # Translation
        data=csv_export_data,
        file_name=f"top_scorers_{team_choice.lower().replace(' ', '_')}.csv",
        mime="text/csv",
        key="player_download" # Added key to prevent errors if button is duplicated
    )

# ============================================
# Team Performance Page
# ============================================
elif selected_page == "Team Performance":
    st.header("Team Performance and Ranking") # Translation
    
    # Retrieve data
    goals_per_team = fetch_total_goals_by_team() # Changed function name
    current_standings = fetch_league_standings() # Changed function name
    attack_defense_stats = fetch_attack_vs_defense_stats() # Changed function name
    defense_ranking = fetch_best_defense_ranking() # Changed function name
    
    # Charts
    st.subheader("Performance Visualizations") # Translation
    
    tab1, tab2, tab3, tab4 = st.tabs(["League Standings", "Goals Scored", "Attack vs Defense", "Defense Ranking"]) # Translation
    
    with tab1:
        fig1 = create_bar_chart(current_standings, "Points", "Club", "Team League Standings") # Changed function name and column names
        st.plotly_chart(fig1, use_container_width=True)
    
    with tab2:
        fig2 = create_bar_chart(goals_per_team, "Goals_Scored", "Club", "Total Goals Scored by Club") # Changed function name and column names
        st.plotly_chart(fig2, use_container_width=True)
    
    with tab3:
        fig3 = create_scatter_plot(attack_defense_stats, "Avg_Goals_For", "Avg_Goals_Against", "Attack vs. Defense (Avg Goals)") # Changed function name and column names
        st.plotly_chart(fig3, use_container_width=True)
    
    with tab4:
        fig4 = create_bar_chart(defense_ranking, "Goals_Conceded", "Club", "Defense Ranking (Lowest Goals Conceded)") # Changed function name and column names
        st.plotly_chart(fig4, use_container_width=True)
    
    # Interactive Table - Standings
    st.subheader("Complete Team Data Table") # Translation
    
    # Merge data for a comprehensive table
    full_team_table = current_standings.merge(goals_per_team, on="Club", how="left") # Changed variable and column names
    full_team_table = full_team_table.merge(
        attack_defense_stats[["Club", "Avg_Goals_For", "Avg_Goals_Against"]], 
        on="Club", 
        how="left"
    )
    
    st.dataframe(full_team_table, use_container_width=True, height=400)
    
    # Download Button
    csv_export_data = full_team_table.to_csv(index=False).encode('utf-8') # Changed variable name
    st.download_button(
        label="Download All Team Stats CSV", # Translation
        data=csv_export_data,
        file_name="team_performance_statistics.csv",
        mime="text/csv",
        key="team_download" # Added key
    )