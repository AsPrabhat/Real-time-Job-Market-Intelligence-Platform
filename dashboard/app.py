import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, size, arrays_overlap, array
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session with Delta Lake support
def create_spark_session():
    """Create Spark session with Delta Lake configuration"""
    return SparkSession.builder \
        .appName("CareerRadar Dashboard") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

# Load data from Delta Lake
def load_delta_data():
    """Load and process data from Delta Lake"""
    try:
        spark = create_spark_session()
        delta_path = "/opt/spark/delta/job_data"
        
        # Read Delta Lake table
        df = spark.read.format("delta").load(delta_path)
        
        # Convert to Pandas for Plotly
        pandas_df = df.toPandas()
        
        logger.info(f"Loaded {len(pandas_df)} records from Delta Lake")
        return pandas_df, df
    except Exception as e:
        logger.error(f"Error loading Delta Lake data: {e}")
        return pd.DataFrame(), None

# Initialize Dash app
app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "CareerRadar - Job Market Intelligence"

# Load initial data
df_pandas, df_spark = load_delta_data()

# App layout
app.layout = html.Div([
    # Header
    html.Div([
        html.H1("CareerRadar", className="header-title"),
        html.P("Real-time Job Market Intelligence Platform", className="header-subtitle"),
    ], className="header"),
    
    # Refresh button
    html.Div([
        html.Button("🔄 Refresh Data", id="refresh-button", n_clicks=0, className="refresh-btn"),
        html.Span(id="last-update", className="last-update")
    ], className="control-panel"),
    
    # Key Metrics Row
    html.Div([
        html.Div([
            html.Div([
                html.H3(id="total-jobs", children="0"),
                html.P("Total Jobs")
            ], className="metric-card")
        ], className="metric-col"),
        
        html.Div([
            html.Div([
                html.H3(id="total-companies", children="0"),
                html.P("Companies")
            ], className="metric-card")
        ], className="metric-col"),
        
        html.Div([
            html.Div([
                html.H3(id="total-skills", children="0"),
                html.P("Unique Skills")
            ], className="metric-card")
        ], className="metric-col"),
        
        html.Div([
            html.Div([
                html.H3(id="avg-skills", children="0"),
                html.P("Avg Skills/Job")
            ], className="metric-card")
        ], className="metric-col"),
    ], className="metrics-row"),
    
    # Charts Row 1
    html.Div([
        html.Div([
            dcc.Graph(id="top-skills-chart")
        ], className="chart-col-half"),
        
        html.Div([
            dcc.Graph(id="seniority-chart")
        ], className="chart-col-half"),
    ], className="charts-row"),
    
    # Charts Row 2
    html.Div([
        html.Div([
            dcc.Graph(id="location-chart")
        ], className="chart-col-half"),
        
        html.Div([
            dcc.Graph(id="company-chart")
        ], className="chart-col-half"),
    ], className="charts-row"),
    
    # Charts Row 3
    html.Div([
        html.Div([
            dcc.Graph(id="timeline-chart")
        ], className="chart-col-full"),
    ], className="charts-row"),
    
    # Charts Row 4 - Skill Co-occurrence Heatmap
    html.Div([
        html.Div([
            dcc.Graph(id="skill-cooccurrence-chart")
        ], className="chart-col-full"),
    ], className="charts-row"),
    
    # Data Table
    html.Div([
        html.H2("Job Listings", className="section-title"),
        html.Div([
            html.Label("Filter by Seniority:", className="filter-label"),
            dcc.Dropdown(
                id="seniority-filter",
                options=[{"label": "All", "value": "all"}],
                value="all",
                className="filter-dropdown"
            ),
        ], className="filter-row"),
        
        dash_table.DataTable(
            id="jobs-table",
            columns=[],
            data=[],
            page_size=10,
            style_table={'overflowX': 'auto'},
            style_cell={
                'textAlign': 'left',
                'padding': '10px',
                'backgroundColor': '#1e1e1e',
                'color': '#ffffff',
                'border': '1px solid #333'
            },
            style_header={
                'backgroundColor': '#2d2d2d',
                'fontWeight': 'bold',
                'color': '#00d4ff',
                'border': '1px solid #444'
            },
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': '#252525'
                }
            ]
        )
    ], className="table-container"),
    
    # Footer
    html.Div([
        html.P("Built with Apache Kafka, Spark Streaming, Delta Lake & Plotly Dash", className="footer-text")
    ], className="footer"),
    
], className="container")

# Callback to refresh data
@app.callback(
    [
        Output("total-jobs", "children"),
        Output("total-companies", "children"),
        Output("total-skills", "children"),
        Output("avg-skills", "children"),
        Output("top-skills-chart", "figure"),
        Output("seniority-chart", "figure"),
        Output("location-chart", "figure"),
        Output("company-chart", "figure"),
        Output("timeline-chart", "figure"),
        Output("skill-cooccurrence-chart", "figure"),
        Output("jobs-table", "columns"),
        Output("jobs-table", "data"),
        Output("seniority-filter", "options"),
        Output("last-update", "children"),
    ],
    [Input("refresh-button", "n_clicks")]
)
def update_dashboard(n_clicks):
    """Update all dashboard components"""
    global df_pandas, df_spark
    
    # Reload data
    df_pandas, df_spark = load_delta_data()
    
    if df_pandas.empty:
        return ["0", "0", "0", "0", {}, {}, {}, {}, {}, {}, [], [], [{"label": "All", "value": "all"}], "No data available"]
    
    # Calculate metrics
    total_jobs = len(df_pandas)
    total_companies = df_pandas['company'].nunique()
    
    # Get unique skills from skills_extracted column
    all_skills = []
    for skills_list in df_pandas['skills_extracted'].dropna():
        if isinstance(skills_list, list):
            all_skills.extend(skills_list)
    total_skills = len(set(all_skills))
    avg_skills = round(df_pandas['skills_count'].mean(), 1) if 'skills_count' in df_pandas.columns else 0
    
    # Top 10 Skills Bar Chart
    skills_df = pd.DataFrame(all_skills, columns=['skill'])
    top_skills = skills_df['skill'].value_counts().head(10)
    
    fig_skills = go.Figure(data=[
        go.Bar(
            x=top_skills.values,
            y=top_skills.index,
            orientation='h',
            marker=dict(color='#00d4ff', line=dict(color='#0099cc', width=1))
        )
    ])
    fig_skills.update_layout(
        title="Top 10 In-Demand Skills",
        xaxis_title="Number of Jobs",
        yaxis_title="",
        template="plotly_dark",
        height=400,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    # Seniority Distribution Pie Chart
    seniority_counts = df_pandas['seniority'].value_counts()
    
    fig_seniority = go.Figure(data=[
        go.Pie(
            labels=seniority_counts.index,
            values=seniority_counts.values,
            hole=0.4,
            marker=dict(colors=['#00d4ff', '#00ff88', '#ff6b6b'])
        )
    ])
    fig_seniority.update_layout(
        title="Seniority Level Distribution",
        template="plotly_dark",
        height=400,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    # Location Distribution Bar Chart
    location_counts = df_pandas['location'].value_counts().head(8)
    
    fig_location = go.Figure(data=[
        go.Bar(
            x=location_counts.index,
            y=location_counts.values,
            marker=dict(color='#00ff88', line=dict(color='#00cc66', width=1))
        )
    ])
    fig_location.update_layout(
        title="Jobs by Location",
        xaxis_title="",
        yaxis_title="Number of Jobs",
        template="plotly_dark",
        height=400,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    # Top Companies Bar Chart
    company_counts = df_pandas['company'].value_counts().head(10)
    
    fig_company = go.Figure(data=[
        go.Bar(
            x=company_counts.values,
            y=company_counts.index,
            orientation='h',
            marker=dict(color='#ff6b6b', line=dict(color='#cc5555', width=1))
        )
    ])
    fig_company.update_layout(
        title="Top 10 Hiring Companies",
        xaxis_title="Number of Jobs",
        yaxis_title="",
        template="plotly_dark",
        height=400,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    # Timeline Chart (Jobs Posted Over Time)
    if 'posted_date' in df_pandas.columns:
        df_pandas['posted_date'] = pd.to_datetime(df_pandas['posted_date'])
        timeline_data = df_pandas.groupby('posted_date').size().reset_index(name='count')
        
        fig_timeline = go.Figure(data=[
            go.Scatter(
                x=timeline_data['posted_date'],
                y=timeline_data['count'],
                mode='lines+markers',
                line=dict(color='#00d4ff', width=2),
                marker=dict(size=8, color='#00d4ff')
            )
        ])
        fig_timeline.update_layout(
            title="Job Postings Timeline",
            xaxis_title="Date",
            yaxis_title="Number of Jobs",
            template="plotly_dark",
            height=300,
            margin=dict(l=20, r=20, t=40, b=20)
        )
    else:
        fig_timeline = {}
    
    # Skill Co-occurrence Heatmap (Top 15 skills)
    top_15_skills = top_skills.head(15).index.tolist()
    
    # Create co-occurrence matrix
    cooccurrence = pd.DataFrame(0, index=top_15_skills, columns=top_15_skills)
    
    for skills_list in df_pandas['skills_extracted'].dropna():
        if isinstance(skills_list, list):
            common_skills = [s for s in skills_list if s in top_15_skills]
            for i, skill1 in enumerate(common_skills):
                for skill2 in common_skills[i:]:
                    if skill1 != skill2:
                        cooccurrence.loc[skill1, skill2] += 1
                        cooccurrence.loc[skill2, skill1] += 1
    
    fig_heatmap = go.Figure(data=go.Heatmap(
        z=cooccurrence.values,
        x=cooccurrence.columns,
        y=cooccurrence.index,
        colorscale='Viridis',
        showscale=True
    ))
    fig_heatmap.update_layout(
        title="Skill Co-occurrence Analysis (Top 15 Skills)",
        template="plotly_dark",
        height=500,
        margin=dict(l=20, r=20, t=40, b=20),
        xaxis={'side': 'bottom'}
    )
    
    # Prepare table data
    table_columns = [
        {"name": "Job ID", "id": "job_id"},
        {"name": "Title", "id": "title"},
        {"name": "Company", "id": "company"},
        {"name": "Location", "id": "location"},
        {"name": "Experience", "id": "experience"},
        {"name": "Seniority", "id": "seniority"},
        {"name": "Skills Count", "id": "skills_count"},
    ]
    
    table_data = df_pandas[['job_id', 'title', 'company', 'location', 'experience', 'seniority', 'skills_count']].to_dict('records')
    
    # Seniority filter options
    seniority_options = [{"label": "All", "value": "all"}]
    seniority_options.extend([{"label": s, "value": s} for s in df_pandas['seniority'].unique()])
    
    # Last update timestamp
    last_update = f"Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}"
    
    return [
        str(total_jobs),
        str(total_companies),
        str(total_skills),
        str(avg_skills),
        fig_skills,
        fig_seniority,
        fig_location,
        fig_company,
        fig_timeline,
        fig_heatmap,
        table_columns,
        table_data,
        seniority_options,
        last_update
    ]

# Callback to filter table by seniority
@app.callback(
    Output("jobs-table", "data", allow_duplicate=True),
    [Input("seniority-filter", "value")],
    prevent_initial_call=True
)
def filter_table(seniority):
    """Filter jobs table by seniority level"""
    if df_pandas.empty or seniority == "all":
        return df_pandas[['job_id', 'title', 'company', 'location', 'experience', 'seniority', 'skills_count']].to_dict('records')
    
    filtered_df = df_pandas[df_pandas['seniority'] == seniority]
    return filtered_df[['job_id', 'title', 'company', 'location', 'experience', 'seniority', 'skills_count']].to_dict('records')

if __name__ == '__main__':
    logger.info("Starting CareerRadar Dashboard on http://0.0.0.0:8050")
    app.run_server(host='0.0.0.0', port=8050, debug=False)
