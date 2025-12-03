import streamlit as st
import pickle
import time
from pathlib import Path
from datetime import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Page configuration
st.set_page_config(
    page_title="Crypto Market Dashboard",
    page_icon="TO THE MOON",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Sidebar navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Select Page", ["Market Data", "Sentiment Analysis"])

# Data file paths
market_data_file = Path('market_data.pkl')
sentiment_data_file = Path('sentiment_data.pkl')

# ====================== PAGE 1: MARKET DATA ======================
if page == "Market Data":
    st.title("Real-Time Crypto Market Data")
    st.markdown("---")
    
    placeholder = st.empty()
    
    while True:
        try:
            if market_data_file.exists():
                with open(market_data_file, 'rb') as f:
                    data = pickle.load(f)
                
                with placeholder.container():
                    st.info(f" Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    exchanges = list(data.keys())
                    
                    if len(exchanges) == 0:
                        st.warning(" Waiting for market data...")
                    else:
                        cols = st.columns(len(exchanges))
                        
                        for idx, exchange in enumerate(exchanges):
                            with cols[idx]:
                                st.header(f"{exchange.upper()}")
                                
                                products = data[exchange]
                                
                                for product, info in products.items():
                                    st.subheader(f" {product}")
                                    
                                    col1, col2 = st.columns(2)
                                    
                                    with col1:
                                        st.metric(
                                            label="Best Bid",
                                            value=f"${info['bid']:,.2f}"
                                        )
                                        st.metric(
                                            label="Spread",
                                            value=f"${info['spread']:.2f}",
                                            delta=f"{info['spread_percent']:.3f}%"
                                        )
                                    
                                    with col2:
                                        st.metric(
                                            label="Best Ask",
                                            value=f"${info['ask']:,.2f}"
                                        )
                                        st.metric(
                                            label="24h Volume",
                                            value=f"{info['volume']:,.2f}"
                                        )
                                    
                                    st.caption(f" {info['timestamp']}")
                                    st.markdown("---")
            else:
                st.warning("No data file found. Make sure the WebSocket aggregator is running!")
                st.info("Run the market data aggregator script first to start collecting data.")
        
        except Exception as e:
            st.error(f" Error loading data: {e}")
        
        time.sleep(2)

# ====================== PAGE 2: SENTIMENT ANALYSIS ======================
elif page == "Sentiment Analysis":
    st.title("Market Sentiment Analysis")
    st.markdown("---")
    
    placeholder = st.empty()
    
    while True:
        try:
            if sentiment_data_file.exists():
                with open(sentiment_data_file, 'rb') as f:
                    sentiment_data = pickle.load(f)
                
                with placeholder.container():
                    # Header with overall sentiment
                    col1, col2, col3 = st.columns(3)
                    
                    overall = sentiment_data.get('overall_sentiment', 0)
                    sentiment_label = "Bullish" if overall > 0.1 else "Bearish" if overall < -0.1 else "🟡 Neutral"
                    
                    with col1:
                        st.metric(
                            label="Overall Sentiment",
                            value=sentiment_label,
                            delta=f"{overall:.3f}"
                        )
                    
                    with col2:
                        reddit_count = len(sentiment_data.get('reddit', []))
                        st.metric(
                            label="Reddit Posts Analyzed",
                            value=reddit_count
                        )
                    
                    with col3:
                        news_count = len(sentiment_data.get('news', []))
                        st.metric(
                            label="News Articles Analyzed",
                            value=news_count
                        )
                    
                    st.info(f"Last Updated: {sentiment_data.get('last_update', 'Never')}")
                    st.markdown("---")
                    
                    # Sentiment Distribution Chart
                    all_items = sentiment_data.get('reddit', []) + sentiment_data.get('news', [])
                    
                    if all_items:
                        sentiments = [item['sentiment'] for item in all_items]
                        
                        # Create histogram
                        fig = px.histogram(
                            x=sentiments,
                            nbins=20,
                            title="Sentiment Distribution",
                            labels={'x': 'Sentiment Score', 'y': 'Count'},
                            color_discrete_sequence=['#1f77b4']
                        )
                        fig.add_vline(x=0, line_dash="dash", line_color="red", annotation_text="Neutral")
                        fig.update_layout(height=400)
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Create sentiment gauge
                        fig_gauge = go.Figure(go.Indicator(
                            mode="gauge+number+delta",
                            value=overall,
                            domain={'x': [0, 1], 'y': [0, 1]},
                            title={'text': "Overall Sentiment Score"},
                            delta={'reference': 0},
                            gauge={
                                'axis': {'range': [-1, 1]},
                                'bar': {'color': "darkblue"},
                                'steps': [
                                    {'range': [-1, -0.3], 'color': "lightcoral"},
                                    {'range': [-0.3, 0.3], 'color': "lightyellow"},
                                    {'range': [0.3, 1], 'color': "lightgreen"}
                                ],
                                'threshold': {
                                    'line': {'color': "red", 'width': 4},
                                    'thickness': 0.75,
                                    'value': 0
                                }
                            }
                        ))
                        fig_gauge.update_layout(height=300)
                        st.plotly_chart(fig_gauge, use_container_width=True)
                    
                    st.markdown("---")
                    
                    # Display Reddit and News side by side
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.subheader(" Reddit Sentiment")
                        reddit_items = sentiment_data.get('reddit', [])
                        
                        if reddit_items:
                            for item in reddit_items[-10:]:  # Show last 10
                                sentiment_emoji = "🟢" if item['sentiment'] > 0.1 else "🔴" if item['sentiment'] < -0.1 else "🟡"
                                
                                with st.expander(f"{sentiment_emoji} {item['source']} ({item['sentiment']:.2f})"):
                                    st.write(item['text'])
                                    st.caption(f"Score: {item['score']} | {item['timestamp']}")
                        else:
                            st.info("No Reddit data available yet")
                    
                    with col2:
                        st.subheader("News Sentiment")
                        news_items = sentiment_data.get('news', [])
                        
                        if news_items:
                            for item in news_items[-10:]:  # Show last 10
                                sentiment_emoji = "🟢" if item['sentiment'] > 0.1 else "🔴" if item['sentiment'] < -0.1 else "🟡"
                                
                                with st.expander(f"{sentiment_emoji} {item['source']} ({item['sentiment']:.2f})"):
                                    st.write(item['text'])
                                    if item.get('url'):
                                        st.markdown(f"[Read more]({item['url']})")
                                    st.caption(item['timestamp'])
                        else:
                            st.info("No news data available yet")
                    
                    st.markdown("---")
                    
                    # Market Impact Analysis
                    st.subheader("Sentiment vs Market Data")
                    
                    if market_data_file.exists():
                        with open(market_data_file, 'rb') as f:
                            market_data = pickle.load(f)
                        
                        st.write("**Current Market Prices with Sentiment Context:**")
                        
                        for exchange, products in market_data.items():
                            for product, info in products.items():
                                col_a, col_b, col_c = st.columns([2, 2, 3])
                                
                                with col_a:
                                    st.write(f"**{product}** ({exchange})")
                                
                                with col_b:
                                    st.write(f"${info['bid']:,.2f}")
                                
                                with col_c:
                                    if overall > 0.1:
                                        st.success(f"🟢 Bullish sentiment may support upward price movement")
                                    elif overall < -0.1:
                                        st.error(f"🔴 Bearish sentiment may indicate downward pressure")
                                    else:
                                        st.info(f"🟡 Neutral sentiment - market consolidation likely")
                    else:
                        st.warning("Market data not available. Start the aggregator to see correlation.")
                    
            else:
                st.warning("No sentiment data available yet.")
                st.info("The aggregator will fetch sentiment data every 5 minutes. Please wait...")
        
        except Exception as e:
            st.error(f"Error loading sentiment data: {e}")
        
        time.sleep(3)