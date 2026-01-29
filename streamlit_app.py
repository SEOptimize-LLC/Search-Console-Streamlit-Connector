
import streamlit as st
import pandas as pd
import json
import os
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import base64

# Configure Streamlit page
st.set_page_config(
    page_title="Google Search Console Connector",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constants
SCOPES = [
    'https://www.googleapis.com/auth/webmasters.readonly',
    'https://www.googleapis.com/auth/webmasters'
]

# Custom CSS for better UI
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        text-align: center;
        color: #1f77b4;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .auth-button {
        background-color: #4285f4;
        color: white;
        padding: 0.5rem 1rem;
        border: none;
        border-radius: 0.25rem;
        cursor: pointer;
    }
</style>
""", unsafe_allow_html=True)

def get_auth_url():
    """Generate OAuth authorization URL"""
    try:
        # Check if we have client secrets in Streamlit secrets
        if "google" in st.secrets:
            client_config = {
                "web": {
                    "client_id": st.secrets["google"]["client_id"],
                    "client_secret": st.secrets["google"]["client_secret"],
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": [st.secrets["google"]["redirect_uri"]]
                }
            }
        else:
            st.error("Google OAuth credentials not found in secrets. Please configure your secrets.toml file.")
            return None

        flow = Flow.from_client_config(
            client_config,
            scopes=SCOPES,
            redirect_uri=st.secrets["google"]["redirect_uri"]
        )

        auth_url, _ = flow.authorization_url(
            access_type='offline',
            include_granted_scopes='true',
            prompt='consent'
        )

        return auth_url, flow

    except Exception as e:
        st.error(f"Error generating auth URL: {str(e)}")
        return None

def get_credentials_from_code(auth_code):
    """Exchange authorization code for credentials"""
    try:
        client_config = {
            "web": {
                "client_id": st.secrets["google"]["client_id"],
                "client_secret": st.secrets["google"]["client_secret"],
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [st.secrets["google"]["redirect_uri"]]
            }
        }

        flow = Flow.from_client_config(
            client_config,
            scopes=SCOPES,
            redirect_uri=st.secrets["google"]["redirect_uri"]
        )

        flow.fetch_token(code=auth_code)
        return flow.credentials

    except Exception as e:
        # Don't display error here - let the caller handle it
        # This prevents error messages from persisting across page loads
        return None

def save_credentials(credentials):
    """Save credentials to session state"""
    creds_dict = {
        "token": credentials.token,
        "refresh_token": credentials.refresh_token,
        "token_uri": credentials.token_uri,
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
        "scopes": credentials.scopes
    }
    st.session_state["credentials"] = creds_dict

def load_credentials():
    """Load credentials from session state"""
    if "credentials" in st.session_state:
        creds_dict = st.session_state["credentials"]
        credentials = Credentials(
            token=creds_dict["token"],
            refresh_token=creds_dict["refresh_token"],
            token_uri=creds_dict["token_uri"],
            client_id=creds_dict["client_id"],
            client_secret=creds_dict["client_secret"],
            scopes=creds_dict["scopes"]
        )

        # Refresh if expired
        if credentials.expired and credentials.refresh_token:
            try:
                credentials.refresh(Request())
                save_credentials(credentials)  # Save refreshed credentials
            except Exception as e:
                st.error(f"Error refreshing credentials: {str(e)}")
                return None

        return credentials
    return None

def get_search_console_service(credentials):
    """Build Search Console service"""
    try:
        return build('searchconsole', 'v1', credentials=credentials)
    except Exception as e:
        st.error(f"Error building Search Console service: {str(e)}")
        return None

def get_verified_sites(service):
    """Get list of verified sites"""
    try:
        sites = service.sites().list().execute()
        return [site['siteUrl'] for site in sites.get('siteEntry', [])]
    except HttpError as e:
        st.error(f"HTTP Error getting sites: {e}")
        return []
    except Exception as e:
        st.error(f"Error getting verified sites: {str(e)}")
        return []

def get_search_console_data(service, site_url, start_date, end_date, dimensions=None, filters=None, max_rows=25000):
    """Fetch Search Console data with error handling"""
    try:
        request = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': dimensions or ['query'],
            'rowLimit': min(max_rows, 25000),  # API limit
            'startRow': 0
        }

        if filters:
            request['dimensionFilterGroups'] = [{'filters': filters}]

        # Execute request with error handling
        response = service.searchanalytics().query(
            siteUrl=site_url, 
            body=request
        ).execute()

        # Convert to DataFrame
        if 'rows' in response:
            data = []
            for row in response['rows']:
                row_data = {}
                if dimensions:
                    for i, dimension in enumerate(dimensions):
                        row_data[dimension] = row['keys'][i]

                row_data.update({
                    'clicks': row.get('clicks', 0),
                    'impressions': row.get('impressions', 0),
                    'ctr': row.get('ctr', 0),
                    'position': row.get('position', 0)
                })
                data.append(row_data)

            return pd.DataFrame(data)
        else:
            return pd.DataFrame()

    except HttpError as e:
        error_details = json.loads(e.content.decode())
        error_message = error_details.get('error', {}).get('message', 'Unknown error')
        st.error(f"Google Search Console API Error: {error_message}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching Search Console data: {str(e)}")
        return pd.DataFrame()

def create_metrics_cards(df):
    """Create metric cards from data"""
    if df.empty:
        return

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_clicks = df['clicks'].sum()
        st.metric("Total Clicks", f"{total_clicks:,}")

    with col2:
        total_impressions = df['impressions'].sum()
        st.metric("Total Impressions", f"{total_impressions:,}")

    with col3:
        avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
        st.metric("Average CTR", f"{avg_ctr:.2f}%")

    with col4:
        avg_position = df['position'].mean() if not df.empty else 0
        st.metric("Average Position", f"{avg_position:.1f}")

def create_visualizations(df, dimensions):
    """Create visualizations based on data"""
    if df.empty:
        st.warning("No data available for visualization")
        return

    # Performance over time chart
    if 'date' in dimensions:
        st.subheader("📈 Performance Over Time")
        fig = px.line(df, x='date', y=['clicks', 'impressions'], 
                     title="Clicks and Impressions Over Time")
        st.plotly_chart(fig, use_container_width=True)

    # Top queries/pages chart
    if 'query' in dimensions:
        st.subheader("🔍 Top Queries")
        top_queries = df.nlargest(20, 'clicks')
        fig = px.bar(top_queries, x='clicks', y='query', orientation='h',
                    title="Top 20 Queries by Clicks")
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)

    if 'page' in dimensions:
        st.subheader("📄 Top Pages")
        top_pages = df.nlargest(20, 'clicks')
        fig = px.bar(top_pages, x='clicks', y='page', orientation='h',
                    title="Top 20 Pages by Clicks")
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)

def main():
    """Main application function"""

    # Header
    st.markdown('<div class="main-header">🔍 Google Search Console Connector</div>', 
                unsafe_allow_html=True)

    # Load existing credentials first
    credentials = load_credentials()

    # Handle OAuth callback
    auth_code = st.query_params.get("code")
    if auth_code and not credentials:
        credentials = get_credentials_from_code(auth_code)
        if credentials:
            save_credentials(credentials)
            st.query_params.clear()
            st.rerun()

    # Authentication section
    if not credentials:
        st.markdown("### 🔐 Authentication Required")
        st.info("Please authenticate with Google to access your Search Console data.")

        if st.button("🔑 Sign in with Google", type="primary"):
            auth_result = get_auth_url()
            if auth_result:
                auth_url, _ = auth_result
                st.markdown(f"""
                **Step 1:** Click the link below to authorize the application:

                🔗 [Authorize Access]({auth_url})

                **Step 2:** After authorizing, you'll be redirected back to this app.
                """)

        # Manual code input as fallback
        st.markdown("---")
        st.markdown("### 📝 Manual Authorization Code Input")
        manual_code = st.text_input("If the redirect doesn't work, paste the authorization code here:")
        if manual_code and st.button("Submit Code"):
            credentials = get_credentials_from_code(manual_code)
            if credentials:
                save_credentials(credentials)
                st.success("✅ Successfully authenticated!")
                st.rerun()

        return

    # Build Search Console service
    service = get_search_console_service(credentials)
    if not service:
        return

    # Get verified sites
    sites = get_verified_sites(service)
    if not sites:
        st.error("No verified sites found in your Search Console account.")
        return

    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Configuration")

        # Site selection
        selected_site = st.selectbox("🌐 Select Website", sites)

        # Date range selection
        date_range = st.selectbox(
            "📅 Date Range",
            ["Last 7 days", "Last 30 days", "Last 3 months", "Custom range"]
        )

        if date_range == "Custom range":
            start_date = st.date_input("Start Date", 
                                     value=datetime.now() - timedelta(days=30))
            end_date = st.date_input("End Date", value=datetime.now() - timedelta(days=1))
        else:
            days = {"Last 7 days": 7, "Last 30 days": 30, "Last 3 months": 90}[date_range]
            end_date = datetime.now() - timedelta(days=1)
            start_date = end_date - timedelta(days=days)

        # Dimensions selection
        st.markdown("### 📊 Dimensions")
        dimensions = []

        if st.checkbox("Query", value=True):
            dimensions.append('query')
        if st.checkbox("Page"):
            dimensions.append('page')
        if st.checkbox("Date"):
            dimensions.append('date')
        if st.checkbox("Country"):
            dimensions.append('country')
        if st.checkbox("Device"):
            dimensions.append('device')

        # Row limit
        max_rows = st.slider("Maximum Rows", 1000, 25000, 10000, 1000)

    # Main content
    if st.button("📊 Fetch Data", type="primary"):
        if not dimensions:
            st.error("Please select at least one dimension.")
            return

        with st.spinner("Fetching Search Console data..."):
            df = get_search_console_data(
                service=service,
                site_url=selected_site,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                dimensions=dimensions,
                max_rows=max_rows
            )

            if not df.empty:
                st.success(f"✅ Successfully fetched {len(df)} rows of data!")

                # Store data in session state
                st.session_state["search_data"] = df
                st.session_state["dimensions"] = dimensions
            else:
                st.warning("No data found for the selected criteria.")

    # Display results if data exists
    if "search_data" in st.session_state and not st.session_state["search_data"].empty:
        df = st.session_state["search_data"]
        dimensions = st.session_state.get("dimensions", [])

        st.markdown("---")
        st.header("📊 Results")

        # Metrics cards
        create_metrics_cards(df)

        st.markdown("---")

        # Visualizations
        create_visualizations(df, dimensions)

        st.markdown("---")

        # Data table
        st.subheader("📋 Data Table")
        st.dataframe(df, use_container_width=True)

        # Download data
        csv = df.to_csv(index=False)
        st.download_button(
            label="💾 Download CSV",
            data=csv,
            file_name=f"search_console_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

if __name__ == "__main__":
    main()
