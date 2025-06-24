import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io

# Set page configuration
st.set_page_config(
    page_title="Data Filter & Download",
    page_icon="📊",
    layout="wide"
)

@st.cache_data
def load_data(file_path):
    """Load data from parquet file with caching for performance"""
    try:
        df = pd.read_parquet(file_path)
        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None

def convert_df_to_csv(df):
    """Convert dataframe to CSV for download"""
    output = io.StringIO()
    df.to_csv(output, index=False)
    return output.getvalue()

def main():
    st.title("📊 Data Filter & Download Tool")
    st.markdown("---")
    
    # File upload or path input
    st.subheader("📁 Data Source")
    
    # Option 1: File uploader
    uploaded_file = st.file_uploader(
        "Upload a Parquet file",
        type=['parquet'],
        help="Upload your parquet file to get started"
    )
    
    # Option 2: File path input (for local development)
    st.markdown("**OR**")
    file_path = st.text_input(
        "Enter file path to parquet file:",
        placeholder="e.g., /path/to/your/data.parquet",
        help="Enter the full path to your parquet file"
    )
    
    # Load data
    df = None
    if uploaded_file is not None:
        df = pd.read_parquet(uploaded_file)
        st.success("✅ File uploaded successfully!")
    elif file_path:
        df = load_data(file_path)
        if df is not None:
            st.success("✅ File loaded successfully!")
    
    if df is not None:
        # Display basic info about the dataset
        st.subheader("📋 Dataset Overview")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Rows", len(df))
        with col2:
            st.metric("Total Columns", len(df.columns))
        with col3:
            st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
        
        # Show column information
        st.write("**Columns:**", ", ".join(df.columns.tolist()))
        
        # Identify date columns (columns with 'date' in name or datetime dtype)
        date_columns = []
        for col in df.columns:
            if 'date' in col.lower() or 'time' in col.lower():
                date_columns.append(col)
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                date_columns.append(col)
        
        # Convert string dates to datetime if needed
        for col in date_columns:
            if not pd.api.types.is_datetime64_any_dtype(df[col]):
                try:
                    df[col] = pd.to_datetime(df[col])
                except:
                    st.warning(f"Could not convert column '{col}' to datetime")
                    date_columns.remove(col)
        
        st.markdown("---")
        st.subheader("🔍 Filters")
        
        # Create filter controls
        col1, col2 = st.columns(2)
        
        with col1:
            # Filter column selector
            filter_columns = df.select_dtypes(include=['object', 'category']).columns.tolist()
            if filter_columns:
                selected_filter_col = st.selectbox(
                    "Select column to filter:",
                    options=['None'] + filter_columns,
                    help="Choose a categorical column to filter by"
                )
            else:
                st.info("No categorical columns found for filtering")
                selected_filter_col = 'None'
        
        with col2:
            # Date column selector
            if date_columns:
                selected_date_col = st.selectbox(
                    "Select date column:",
                    options=['None'] + date_columns,
                    help="Choose a date column for date range filtering"
                )
            else:
                st.info("No date columns found")
                selected_date_col = 'None'
        
        # Apply filters
        filtered_df = df.copy()
        
        # Categorical filter
        if selected_filter_col != 'None':
            unique_values = sorted(df[selected_filter_col].dropna().unique())
            selected_values = st.multiselect(
                f"Select values for {selected_filter_col}:",
                options=unique_values,
                default=unique_values[:min(5, len(unique_values))],  # Default to first 5 values
                help="Select one or more values to filter by"
            )
            
            if selected_values:
                filtered_df = filtered_df[filtered_df[selected_filter_col].isin(selected_values)]
        
        # Date range filter
        if selected_date_col != 'None':
            min_date = df[selected_date_col].min().date()
            max_date = df[selected_date_col].max().date()
            
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input(
                    "Start date:",
                    value=min_date,
                    min_value=min_date,
                    max_value=max_date
                )
            with col2:
                end_date = st.date_input(
                    "End date:",
                    value=max_date,
                    min_value=min_date,
                    max_value=max_date
                )
            
            if start_date <= end_date:
                filtered_df = filtered_df[
                    (filtered_df[selected_date_col].dt.date >= start_date) &
                    (filtered_df[selected_date_col].dt.date <= end_date)
                ]
            else:
                st.error("Start date must be before or equal to end date")
        
        st.markdown("---")
        
        # Show filtered results
        st.subheader("📊 Filtered Data")
        st.write(f"Showing {len(filtered_df):,} of {len(df):,} rows")
        
        # Display options
        col1, col2 = st.columns(2)
        with col1:
            show_rows = st.slider("Rows to display:", 5, min(100, len(filtered_df)), 10)
        with col2:
            if st.checkbox("Show all columns", value=False):
                st.dataframe(filtered_df.head(show_rows), use_container_width=True)
            else:
                # Show only first few columns if there are many
                display_cols = filtered_df.columns[:10] if len(filtered_df.columns) > 10 else filtered_df.columns
                st.dataframe(filtered_df[display_cols].head(show_rows), use_container_width=True)
                if len(filtered_df.columns) > 10:
                    st.info(f"Showing first 10 of {len(filtered_df.columns)} columns. Check 'Show all columns' to see all.")
        
        # Download section
        st.markdown("---")
        st.subheader("💾 Download Data")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Download filtered data
            csv_data = convert_df_to_csv(filtered_df)
            st.download_button(
                label="📥 Download Filtered Data as CSV",
                data=csv_data,
                file_name=f"filtered_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                help="Download the currently filtered dataset"
            )
        
        with col2:
            # Download original data
            if len(filtered_df) != len(df):
                csv_original = convert_df_to_csv(df)
                st.download_button(
                    label="📥 Download Original Data as CSV",
                    data=csv_original,
                    file_name=f"original_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    help="Download the complete original dataset"
                )
        
        with col3:
            # Show summary statistics
            if st.button("📈 Show Summary Stats"):
                st.subheader("Summary Statistics")
                numeric_cols = filtered_df.select_dtypes(include=[np.number]).columns
                if len(numeric_cols) > 0:
                    st.dataframe(filtered_df[numeric_cols].describe())
                else:
                    st.info("No numeric columns found for summary statistics")
    
    else:
        st.info("👆 Please upload a parquet file or enter a file path to get started")
        
        # Show example of expected file structure
        st.subheader("📝 Expected File Format")
        st.write("""
        Your parquet file should contain:
        - At least one categorical column for filtering
        - Optionally, a date/datetime column for date range filtering
        - Any additional columns you want to analyze
        
        **Example columns:**
        - `category`, `region`, `product_type` (for categorical filtering)
        - `date`, `timestamp`, `created_at` (for date filtering)
        - `value`, `amount`, `quantity` (data columns)
        """)

if __name__ == "__main__":
    main()