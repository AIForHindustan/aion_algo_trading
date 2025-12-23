# shared_core/market_intelligence/signal_dashboard.py
from typing import List, Dict
import streamlit as st
import pandas as pd
from datetime import datetime

def create_signal_dashboard(enhanced_signals: List[Dict]):
    """Create a dashboard for enhanced signals"""
    
    st.title("🎯 Enhanced Scanner Signals with Market Intelligence")
    
    # Market regime overview
    if enhanced_signals:
        first_signal = enhanced_signals[0]
        regime = first_signal.get('market_regime', 'Unknown')
        flow = first_signal.get('institutional_flow', 'Unknown')
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Market Regime", regime)
        with col2:
            st.metric("Institutional Flow", flow)
        with col3:
            st.metric("Total Signals", len(enhanced_signals))
    
    # Signals table
    if enhanced_signals:
        df = pd.DataFrame(enhanced_signals)
        
        # Display key columns
        display_columns = ['symbol', 'signal_type', 'confidence', 'edge_score', 
                         'suppression_reasons', 'market_regime']
        
        st.dataframe(df[display_columns], use_container_width=True)
        
        # Filter controls
        st.sidebar.header("Signal Filters")
        min_confidence = st.sidebar.slider("Minimum Confidence", 0, 100, 60)
        min_edge = st.sidebar.slider("Minimum Edge Score", 0.0, 1.0, 0.55)
        
        filtered_signals = [
            s for s in enhanced_signals 
            if s['confidence'] >= min_confidence and s['edge_score'] >= min_edge
        ]
        
        st.subheader(f"Filtered Signals ({len(filtered_signals)})")
        if filtered_signals:
            filtered_df = pd.DataFrame(filtered_signals)[display_columns]
            st.dataframe(filtered_df, use_container_width=True)