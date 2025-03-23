import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables first
load_dotenv()

# Set page configuration FIRST (before any other Streamlit commands)
st.set_page_config(
    page_title="Product Cost Analysis",
    page_icon="💰",
    layout="wide"
)

# Add title
st.title("Product Cost Analysis Dashboard")
st.markdown("*Programmed with Python and Streamlit by Kevin Somany*")

try:
    # Get database credentials
    db_params = {
        'dbname': st.secrets['db_name'],
        'user': st.secrets['db_username'],
        'password': st.secrets['db_password'],
        'host': st.secrets['db_host'],
        'port': st.secrets['db_port'],
        'options': '-c default_transaction_read_only=on'
    }
    
    # Modified query to get products with categories
    product_query = """
    SELECT DISTINCT 
        pt.id,
        pt.name ->> 'en_US' as product_name,
        split_part(pc.complete_name, ' / ', 1) AS category_l1,
        split_part(pc.complete_name, ' / ', 2) AS category_l2,
        split_part(pc.complete_name, ' / ', 3) AS category_l3,
        split_part(pc.complete_name, ' / ', 4) AS category_l4
    FROM product_template pt
    LEFT JOIN product_category pc ON pt.categ_id = pc.id
    WHERE pt.type = 'product'  -- Only get storable products
    AND pt.active = true      -- Only get active products
    ORDER BY product_name
    """
    
    conn = psycopg2.connect(**db_params)
    products_df = pd.read_sql_query(product_query, conn)
    conn.close()

    # Create filter columns
    filter_col1, filter_col2 = st.columns(2)
    
    with filter_col1:
        # Category Level 1 Filter
        category_l1_options = ['All'] + sorted(products_df['category_l1'].unique().tolist())
        selected_category_l1 = st.selectbox('Product Category Level 1', category_l1_options)
        
        # Category Level 2 Filter (dependent on Level 1)
        if selected_category_l1 != 'All':
            category_l2_df = products_df[products_df['category_l1'] == selected_category_l1]
        else:
            category_l2_df = products_df
        category_l2_options = ['All'] + sorted(category_l2_df['category_l2'].unique().tolist())
        selected_category_l2 = st.selectbox('Product Category Level 2', category_l2_options)

    with filter_col2:
        # Category Level 3 Filter (dependent on Level 2)
        if selected_category_l2 != 'All':
            category_l3_df = category_l2_df[category_l2_df['category_l2'] == selected_category_l2]
        else:
            category_l3_df = category_l2_df
        category_l3_options = ['All'] + sorted(category_l3_df['category_l3'].unique().tolist())
        selected_category_l3 = st.selectbox('Product Category Level 3', category_l3_options)
        
        # Category Level 4 Filter (dependent on Level 3)
        if selected_category_l3 != 'All':
            category_l4_df = category_l3_df[category_l3_df['category_l3'] == selected_category_l3]
        else:
            category_l4_df = category_l3_df
        category_l4_options = ['All'] + sorted(category_l4_df['category_l4'].unique().tolist())
        selected_category_l4 = st.selectbox('Product Category Level 4', category_l4_options)

    # Filter products based on selected categories
    filtered_products = products_df.copy()
    if selected_category_l1 != 'All':
        filtered_products = filtered_products[filtered_products['category_l1'] == selected_category_l1]
    if selected_category_l2 != 'All':
        filtered_products = filtered_products[filtered_products['category_l2'] == selected_category_l2]
    if selected_category_l3 != 'All':
        filtered_products = filtered_products[filtered_products['category_l3'] == selected_category_l3]
    if selected_category_l4 != 'All':
        filtered_products = filtered_products[filtered_products['category_l4'] == selected_category_l4]

    # Create product selector with filtered products
    selected_product = st.selectbox(
        "Select Product",
        options=filtered_products['product_name'].tolist()
    )
    
    # Replace the date range selector section with:
    st.write("Select Cost Analysis Period:")
    period_col1, period_col2, period_col3 = st.columns(3)

    # Initialize dates in session state if not already present
    if 'start_date' not in st.session_state:
        st.session_state.start_date = datetime.now().replace(year=datetime.now().year - 1).strftime('%Y-%m-%d')
    if 'end_date' not in st.session_state:
        st.session_state.end_date = datetime.now().strftime('%Y-%m-%d')

    with period_col1:
        if st.button("Last 12 Months"):
            st.session_state.start_date = (datetime.now() - pd.DateOffset(years=1)).strftime('%Y-%m-%d')
            st.session_state.end_date = datetime.now().strftime('%Y-%m-%d')
    with period_col2:
        if st.button("Last 6 Months"):
            st.session_state.start_date = (datetime.now() - pd.DateOffset(months=6)).strftime('%Y-%m-%d')
            st.session_state.end_date = datetime.now().strftime('%Y-%m-%d')
    with period_col3:
        if st.button("Last 3 Months"):
            st.session_state.start_date = (datetime.now() - pd.DateOffset(months=3)).strftime('%Y-%m-%d')
            st.session_state.end_date = datetime.now().strftime('%Y-%m-%d')

    # Use these session state variables in your SQL query parameters
    start_date = st.session_state.start_date
    end_date = st.session_state.end_date

    # Get BOMs for selected product
    bom_query = """
    SELECT 
        mb.id as bom_id,
        mb.code as bom_code,
        mb.product_qty as quantity,
        uom.name ->> 'en_US' as unit
    FROM mrp_bom mb
    JOIN uom_uom uom ON mb.product_uom_id = uom.id
    WHERE mb.product_tmpl_id = %s
    AND mb.active = true
    """

    if selected_product:
        product_tmpl_id = int(filtered_products[filtered_products['product_name'] == selected_product]['id'].iloc[0])
        conn = psycopg2.connect(**db_params)
        boms_df = pd.read_sql_query(bom_query, conn, params=(product_tmpl_id,))
        conn.close()
        
        if not boms_df.empty:
            # Create BOM selector
            bom_options = [f"{row['bom_code']} ({row['quantity']} {row['unit']})" for _, row in boms_df.iterrows()]
            selected_bom = st.selectbox("Select BOM", options=bom_options)
            # Convert numpy.int64 to Python int
            selected_bom_id = int(boms_df.iloc[bom_options.index(selected_bom)]['bom_id'])

    # Add analyze button
    if st.button("Analyze Cost Breakdown"):
        if not selected_product:
            st.warning("Please select a product first.")
        else:
            product_tmpl_id = int(filtered_products[filtered_products['product_name'] == selected_product]['id'].iloc[0])
            
            # Get BOMs for the selected product
            conn = psycopg2.connect(**db_params)
            boms_df = pd.read_sql_query(bom_query, conn, params=(product_tmpl_id,))
            conn.close()
            
            if boms_df.empty:
                st.warning("No Bill of Materials found for this product.")
            else:
                # Get the selected BOM ID
                bom_options = [f"{row['bom_code']} ({row['quantity']} {row['unit']})" for _, row in boms_df.iterrows()]
                if 'selected_bom' not in locals():
                    st.warning("Please select a BOM first.")
                    st.stop()
                
                # Convert numpy.int64 to Python int
                selected_bom_id = int(boms_df.iloc[bom_options.index(selected_bom)]['bom_id'])
                
                # Connect to database for cost analysis
                conn = psycopg2.connect(**db_params)
                
                # Define the recursive query
                sql = """
                WITH RECURSIVE bom_tree AS (
                    -- Base case: only the lines from the chosen BOM, skipping top-level
                    SELECT
                        pp.id AS component_id,
                        pt.id AS component_tmpl_id,
                        pt.name ->> 'en_US' AS component_name,
                        mb.id AS bom_id,
                        (mbl.product_qty / mb.product_qty::numeric) AS multiplier,
                        COALESCE(
                            (SELECT AVG(price_unit)
                             FROM purchase_order_line
                             WHERE product_id = pp.id
                               AND state in ('purchase', 'done')
                               AND create_date BETWEEN %s AND %s
                            ),
                            (SELECT AVG(unit_cost)
                             FROM stock_valuation_layer
                             WHERE product_id = pp.id
                               AND create_date BETWEEN %s AND %s
                            ),
                            0
                        ) AS unit_cost,
                        1 AS level,
                        ARRAY[pt.name ->> 'en_US'] AS path,
                        (SELECT pt2.name ->> 'en_US' 
                         FROM product_template pt2 
                         WHERE pt2.id = mb.product_tmpl_id
                        ) AS finished_product
                    FROM mrp_bom mb
                    JOIN mrp_bom_line mbl ON mb.id = mbl.bom_id
                    JOIN product_product pp ON mbl.product_id = pp.id
                    JOIN product_template pt ON pp.product_tmpl_id = pt.id
                    WHERE mb.id = %s
                      AND pt.id != mb.product_tmpl_id  -- exclude top-level product

                    UNION ALL

                    SELECT
                        pp.id AS component_id,
                        pt.id AS component_tmpl_id,
                        pt.name ->> 'en_US' AS component_name,
                        mb.id AS bom_id,
                        parent.multiplier * (mbl.product_qty / mb.product_qty::numeric) AS multiplier,
                        COALESCE(
                            (SELECT AVG(price_unit)
                             FROM purchase_order_line
                             WHERE product_id = pp.id
                               AND state in ('purchase', 'done')
                               AND create_date BETWEEN %s AND %s
                            ),
                            (SELECT AVG(unit_cost)
                             FROM stock_valuation_layer
                             WHERE product_id = pp.id
                               AND create_date BETWEEN %s AND %s
                            ),
                            0
                        ) AS unit_cost,
                        parent.level + 1 AS level,
                        parent.path || ARRAY[pt.name ->> 'en_US'] AS path,
                        parent.finished_product
                    FROM bom_tree parent
                    JOIN mrp_bom mb ON mb.product_tmpl_id = parent.component_tmpl_id
                                    AND mb.active = true
                    JOIN mrp_bom_line mbl ON mb.id = mbl.bom_id
                    JOIN product_product pp ON mbl.product_id = pp.id
                    JOIN product_template pt ON pp.product_tmpl_id = pt.id
                    WHERE (pt.name ->> 'en_US') <> ALL(parent.path)  -- prevent cycles
                )
                SELECT DISTINCT
                    finished_product,
                    level,
                    array_to_string(path, ' > ') AS hierarchy_path,
                    component_name,
                    multiplier AS qty_per_parent,
                    multiplier AS total_qty_needed,
                    unit_cost,
                    ROUND(multiplier * unit_cost, 2) AS total_cost
                FROM bom_tree
                WHERE level > 0  -- exclude top level
                ORDER BY level, component_name;
                """
                
                # Execute query with all parameters
                df_cost = pd.read_sql_query(
                    sql, 
                    conn, 
                    params=(
                        start_date, end_date,  # For first COALESCE
                        start_date, end_date,  # For first COALESCE
                        selected_bom_id,       # For WHERE clause
                        start_date, end_date,  # For second COALESCE
                        start_date, end_date   # For second COALESCE
                    )
                )
                
                if df_cost.empty:
                    st.warning("No Bill of Materials found for this product.")
                else:
                    # Calculate total cost
                    total_cost = df_cost['total_cost'].sum()
                    
                    # Display total cost as metric
                    st.metric("Total Product Cost (THB)", f"฿{total_cost:,.2f}")
                    
                    # Display the detailed breakdown
                    st.subheader("Cost Breakdown Details")
                    
                    # Format the dataframe for display
                    display_df = df_cost.copy()
                    display_df['unit_cost'] = display_df['unit_cost'].apply(lambda x: f"฿{x:,.2f}")
                    display_df['total_cost'] = display_df['total_cost'].apply(lambda x: f"฿{x:,.2f}")
                    
                    st.dataframe(
                        display_df,
                        column_config={
                            "finished_product": "Finished Product",
                            "level": "Level",
                            "hierarchy_path": "Component Hierarchy",
                            "component_name": "Component",
                            "qty_per_parent": st.column_config.NumberColumn(
                                "Qty per Parent",
                                format="%.4f"
                            ),
                            "total_qty_needed": st.column_config.NumberColumn(
                                "Total Qty Needed",
                                format="%.4f"
                            ),
                            "unit_cost": "Unit Cost (THB)",
                            "total_cost": "Total Cost (THB)"
                        },
                        hide_index=True
                    )

except Exception as e:
    st.error(f"Error: {str(e)}") 