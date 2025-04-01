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
    # First, let's verify the secrets are loaded
    required_secrets = ['db_name', 'db_username', 'db_password', 'db_host', 'db_port']
    missing_secrets = [secret for secret in required_secrets if secret not in st.secrets]
    
    if missing_secrets:
        st.error(f"Missing required secrets: {', '.join(missing_secrets)}")
        st.stop()
        
    # Get database credentials
    db_params = {
        'dbname': st.secrets['db_name'],
        'user': st.secrets['db_username'],
        'password': st.secrets['db_password'],
        'host': st.secrets['db_host'],
        'port': st.secrets['db_port']
    }
    
    # Test the database connection
    try:
        conn = psycopg2.connect(**db_params)
        st.success("✅ Successfully connected to the database!")
        conn.close()
    except psycopg2.Error as e:
        st.error(f"Failed to connect to the database: {str(e)}")
        st.stop()
        
    # Modified query to get products with new structure
    product_query = """
    WITH base_products AS (
        SELECT 
            pt.id AS tmpl_id,
            pc.complete_name AS product_category,
            regexp_replace(pt.name->>'en_US', '\s*\(.*\)$', '') AS base_product_name,
            u.name->>'en_US' AS uom
        FROM product_template pt
        JOIN product_category pc ON pt.categ_id = pc.id
        JOIN uom_uom u ON u.id = pt.uom_id
        WHERE TRIM(split_part(pc.complete_name, '/', 1)) = 'Finished Goods'
    ),
    variant_data AS (
        SELECT 
            pp.id AS product_id,
            pp.product_tmpl_id,
            pp.default_code,
            CASE 
                WHEN lower(u.name->>'en_US') = 'carton'
                     AND pp.combination_indices IS NOT NULL 
                     AND pp.combination_indices <> ''
                THEN COALESCE(
                   (
                     SELECT string_agg(ptavv.name->>'en_US', ', ')
                     FROM unnest(string_to_array(pp.combination_indices, ',')) AS attr_id
                     JOIN product_template_attribute_value ptav 
                       ON ptav.id = trim(attr_id)::integer
                     JOIN product_attribute_value ptavv 
                       ON ptavv.id = ptav.product_attribute_value_id
                   ), ''
                )
                WHEN lower(u.name->>'en_US') = 'piece'
                THEN COALESCE(
                   substring(pt.name->>'en_US' from '\((.*)\)'),
                   ''
                )
                ELSE ''
            END AS variant_attributes
        FROM product_product pp
        JOIN product_template pt ON pt.id = pp.product_tmpl_id
        JOIN uom_uom u ON u.id = pt.uom_id
    )
    SELECT 
        bp.tmpl_id as id,
        CASE 
            WHEN lower(bp.uom) = 'carton' THEN 'Finished Goods 2 (Carton)'
            WHEN lower(bp.uom) = 'piece' THEN 'Finished Goods 1 (PCS)'
            ELSE 'Finished Goods'
        END AS manufacturing_type,
        bp.product_category,
        bp.base_product_name,
        bp.uom,
        vd.default_code,
        bp.base_product_name ||
            CASE 
                WHEN vd.variant_attributes <> '' 
                THEN ' (' || vd.variant_attributes || ')'
                ELSE ''
            END AS product_name,
        vd.variant_attributes AS variant_suffix,
        CASE 
            WHEN position(',' in vd.variant_attributes) > 0 
            THEN trim(split_part(vd.variant_attributes, ',', 1))
            ELSE vd.variant_attributes
        END AS pack_size,
        CASE 
            WHEN position(',' in vd.variant_attributes) > 0 
            THEN trim(split_part(vd.variant_attributes, ',', 2))
            ELSE ''
        END AS language_code
    FROM base_products bp
    JOIN variant_data vd ON vd.product_tmpl_id = bp.tmpl_id
    ORDER BY bp.base_product_name, vd.default_code;
    """
    
    conn = psycopg2.connect(**db_params)
    products_df = pd.read_sql_query(product_query, conn)
    conn.close()

    # Replace the filter columns section with:
    filter_col1, filter_col2, filter_col3 = st.columns(3)

    with filter_col1:
        # Manufacturing Type Filter
        mfg_type_options = ['All'] + sorted(products_df['manufacturing_type'].unique().tolist())
        selected_mfg_type = st.selectbox('Manufacturing Type', mfg_type_options)

    with filter_col2:
        # Filter products based on manufacturing type
        filtered_by_type = products_df if selected_mfg_type == 'All' else products_df[products_df['manufacturing_type'] == selected_mfg_type]
        # Pack Size Filter (if available)
        pack_size_options = ['All'] + sorted(filtered_by_type['pack_size'].unique().tolist())
        selected_pack_size = st.selectbox('Pack Size', pack_size_options)

    with filter_col3:
        # Language Code Filter (if available)
        filtered_by_pack = filtered_by_type if selected_pack_size == 'All' else filtered_by_type[filtered_by_type['pack_size'] == selected_pack_size]
        language_options = ['All'] + sorted(filtered_by_pack['language_code'].unique().tolist())
        selected_language = st.selectbox('Language', language_options)

    # Apply all filters
    filtered_products = products_df
    if selected_mfg_type != 'All':
        filtered_products = filtered_products[filtered_products['manufacturing_type'] == selected_mfg_type]
    if selected_pack_size != 'All':
        filtered_products = filtered_products[filtered_products['pack_size'] == selected_pack_size]
    if selected_language != 'All':
        filtered_products = filtered_products[filtered_products['language_code'] == selected_language]

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
    AND mb.type = 'normal'  -- Only show normal manufacturing BOMs
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
                
                # Standardize date range to match CSV
                start_date = '2024-01-01'
                end_date = '2024-12-31'
                
                # Add detailed debug information
                st.write("### Debug Information")
                st.write("#### Using CSV Date Range:")
                st.write(f"Start Date: {start_date}")
                st.write(f"End Date: {end_date}")
                st.write(f"BOM ID: {selected_bom_id}")

                # Add component-level cost verification query
                cost_verification_sql = """
                WITH component_costs AS (
                    SELECT 
                        pp.id as product_id,
                        pt.name ->> 'en_US' as component_name,
                        mbl.product_qty::numeric as line_qty,
                        mb.product_qty::numeric as bom_qty,
                        (mbl.product_qty::numeric / NULLIF(mb.product_qty::numeric, 0)) as multiplier,
                        COALESCE(
                            (SELECT AVG(pol.price_unit)::numeric
                             FROM purchase_order_line pol
                             WHERE pol.product_id = pp.id
                               AND pol.state in ('purchase', 'done')
                               AND pol.create_date >= %s::date 
                               AND pol.create_date <= %s::date),
                            (SELECT AVG(svl.unit_cost)::numeric
                             FROM stock_valuation_layer svl
                             WHERE svl.product_id = pp.id
                               AND svl.create_date >= %s::date 
                               AND svl.create_date <= %s::date),
                            0
                        ) as raw_unit_cost,
                        uom.name ->> 'en_US' as component_uom,
                        uom.factor::numeric as conversion_factor
                    FROM mrp_bom mb
                    JOIN mrp_bom_line mbl ON mb.id = mbl.bom_id
                    JOIN product_product pp ON mbl.product_id = pp.id
                    JOIN product_template pt ON pp.product_tmpl_id = pt.id
                    JOIN uom_uom uom ON pt.uom_id = uom.id
                    WHERE mb.id = %s
                )
                SELECT 
                    component_name,
                    line_qty,
                    bom_qty,
                    multiplier,
                    raw_unit_cost,
                    (multiplier * raw_unit_cost)::numeric as computed_total_cost,
                    component_uom,
                    conversion_factor,
                    (SELECT COUNT(*) 
                     FROM purchase_order_line pol 
                     WHERE pol.product_id = cc.product_id 
                       AND pol.state in ('purchase', 'done')
                       AND pol.create_date >= %s::date 
                       AND pol.create_date <= %s::date
                    ) as pol_count,
                    (SELECT COUNT(*) 
                     FROM stock_valuation_layer svl 
                     WHERE svl.product_id = cc.product_id
                       AND svl.create_date >= %s::date 
                       AND svl.create_date <= %s::date
                    ) as svl_count
                FROM component_costs cc
                ORDER BY component_name;
                """

                # Execute cost verification query
                cost_verification_params = (
                    start_date, end_date,  # For POL
                    start_date, end_date,  # For SVL
                    selected_bom_id,       # For BOM
                    start_date, end_date,  # For POL count
                    start_date, end_date   # For SVL count
                )
                
                cost_verification_df = pd.read_sql_query(
                    cost_verification_sql, 
                    conn, 
                    params=cost_verification_params
                )

                if not cost_verification_df.empty:
                    st.write("#### Component-Level Cost Verification:")
                    st.write("""
                    Shows raw costs before recursive calculation. 
                    Note: This table shows direct costs of all components, including intermediate ones. 
                    The final Total Product Cost will be different because it:
                    - Only counts leaf components (those without their own BOMs)
                    - Prevents double-counting of intermediate components
                    - Includes costs from all levels of the BOM hierarchy
                    """)
                    
                    # Format numeric columns for display
                    format_cols = {
                        'line_qty': '%.4f',
                        'bom_qty': '%.4f',
                        'multiplier': '%.4f',
                        'raw_unit_cost': '฿%.2f',
                        'computed_total_cost': '฿%.2f',
                        'conversion_factor': '%.4f'
                    }
                    
                    # Create a copy for calculations before formatting
                    calc_df = cost_verification_df.copy()
                    
                    # Calculate the total before formatting
                    total_computed_cost = calc_df['computed_total_cost'].astype(float).sum()
                    
                    # Format the numbers in the main dataframe
                    for col, fmt in format_cols.items():
                        if col in cost_verification_df.columns:
                            cost_verification_df[col] = cost_verification_df[col].apply(
                                lambda x: fmt % float(x) if pd.notnull(x) else "N/A"
                            )
                    
                    # Create a total row
                    total_row = pd.DataFrame([{
                        'component_name': 'TOTAL',
                        'line_qty': '',
                        'bom_qty': '',
                        'multiplier': '',
                        'raw_unit_cost': '',
                        'computed_total_cost': f'฿{total_computed_cost:.2f}',
                        'component_uom': '',
                        'conversion_factor': '',
                        'pol_count': '',
                        'svl_count': ''
                    }])
                    
                    # Combine the original dataframe with the total row
                    display_df = pd.concat([cost_verification_df, total_row], ignore_index=True)
                    
                    # Display the dataframe with the total row
                    st.dataframe(
                        display_df,
                        hide_index=True
                    )

                # Execute main recursive query with standardized dates
                params = (
                    start_date, end_date,      # Base case: POL
                    start_date, end_date,      # Base case: SVL
                    selected_bom_id,           # Base case: BOM ID
                    start_date, end_date,      # Recursive: POL
                    start_date, end_date       # Recursive: SVL
                )

                # Add query execution logging
                st.write("#### Query Execution Details:")
                st.write("Parameters used:")
                st.json({
                    "start_date": start_date,
                    "end_date": end_date,
                    "bom_id": selected_bom_id,
                    "parameter_count": len(params)
                })

                # Get current search_path
                search_path_query = "SHOW search_path;"
                with conn.cursor() as cur:
                    cur.execute(search_path_query)
                    search_path = cur.fetchone()[0]
                st.write("#### Database Settings:")
                st.write(f"Search Path: {search_path}")
                
                # Update main recursive query to only include leaf nodes
                sql = """
                WITH RECURSIVE bom_tree AS (
                    -- Base case: only the lines from the chosen BOM, skipping top-level
                    SELECT
                        pp.id AS component_id,
                        pt.id AS component_tmpl_id,
                        pt.name ->> 'en_US' AS component_name,
                        mb.id AS bom_id,
                        (mbl.product_qty::numeric / NULLIF(mb.product_qty::numeric, 0)) AS multiplier,
                        uom.name ->> 'en_US' AS component_uom,
                        COALESCE(uom.factor, 1.0)::numeric AS conversion_factor,
                        COALESCE(
                            (SELECT AVG(pol.price_unit)::numeric
                             FROM purchase_order_line pol
                             WHERE pol.product_id = pp.id
                               AND pol.state in ('purchase', 'done')
                               AND pol.create_date >= %s::date 
                               AND pol.create_date <= %s::date),
                            (SELECT AVG(svl.unit_cost)::numeric
                             FROM stock_valuation_layer svl
                             WHERE svl.product_id = pp.id
                               AND svl.create_date >= %s::date 
                               AND svl.create_date <= %s::date),
                            0
                        )::numeric AS unit_cost,
                        1 AS level,
                        ARRAY[pt.name ->> 'en_US'] AS path,
                        (SELECT pt2.name ->> 'en_US' 
                         FROM product_template pt2 
                         WHERE pt2.id = mb.product_tmpl_id) AS finished_product
                    FROM mrp_bom mb
                    JOIN mrp_bom_line mbl ON mb.id = mbl.bom_id
                    JOIN product_product pp ON mbl.product_id = pp.id
                    JOIN product_template pt ON pp.product_tmpl_id = pt.id
                    JOIN uom_uom uom ON pt.uom_id = uom.id
                    WHERE mb.id = %s
                      AND mb.active = true
                      AND mb.type = 'normal'
                      AND pt.id != mb.product_tmpl_id

                    UNION ALL

                    -- Recursive step: find sub-BOMs for each component
                    SELECT
                        pp.id AS component_id,
                        pt.id AS component_tmpl_id,
                        pt.name ->> 'en_US' AS component_name,
                        mb.id AS bom_id,
                        (parent.multiplier * (mbl.product_qty::numeric / NULLIF(mb.product_qty::numeric, 0)))::numeric AS multiplier,
                        uom.name ->> 'en_US' AS component_uom,
                        COALESCE(uom.factor, 1.0)::numeric AS conversion_factor,
                        COALESCE(
                            (SELECT AVG(pol.price_unit)::numeric
                             FROM purchase_order_line pol
                             WHERE pol.product_id = pp.id
                               AND pol.state in ('purchase', 'done')
                               AND pol.create_date >= %s::date 
                               AND pol.create_date <= %s::date),
                            (SELECT AVG(svl.unit_cost)::numeric
                             FROM stock_valuation_layer svl
                             WHERE svl.product_id = pp.id
                               AND svl.create_date >= %s::date 
                               AND svl.create_date <= %s::date),
                            0
                        )::numeric AS unit_cost,
                        parent.level + 1 AS level,
                        parent.path || ARRAY[pt.name ->> 'en_US'] AS path,
                        parent.finished_product
                    FROM bom_tree parent
                    JOIN mrp_bom mb ON mb.product_tmpl_id = parent.component_tmpl_id
                        AND mb.active = true AND mb.type = 'normal'
                    JOIN mrp_bom_line mbl ON mb.id = mbl.bom_id
                    JOIN product_product pp ON mbl.product_id = pp.id
                    JOIN product_template pt ON pp.product_tmpl_id = pt.id
                    JOIN uom_uom uom ON pt.uom_id = uom.id
                    WHERE (pt.name ->> 'en_US') <> ALL(parent.path)
                )
                SELECT DISTINCT
                    finished_product,
                    level,
                    array_to_string(path, ' > ') AS hierarchy_path,
                    component_name,
                    ROUND(multiplier::numeric, 6) AS qty_per_parent,
                    ROUND((multiplier * conversion_factor)::numeric, 6) AS normalized_qty,
                    ROUND(unit_cost::numeric, 2) AS unit_cost,
                    ROUND((multiplier * unit_cost)::numeric, 2) AS total_cost,
                    component_uom
                FROM bom_tree bt
                WHERE level > 0
                  AND NOT EXISTS (
                      SELECT 1
                      FROM mrp_bom mb2
                      WHERE mb2.active = true
                        AND mb2.type = 'normal'
                        AND mb2.product_tmpl_id = bt.component_tmpl_id
                  )
                ORDER BY level, component_name;
                """
                
                # Add explanation of the cost calculation
                st.write("#### Cost Calculation Method:")
                st.write("""
                - Only leaf nodes (components without their own BOMs) are included in the total cost
                - This prevents double-counting of intermediate components
                - Each component's cost is calculated as: quantity × unit cost
                - Unit costs are averaged from purchase orders or stock valuations within the date range
                """)

                # Execute query with properly ordered parameters
                df_cost = pd.read_sql_query(sql, conn, params=params)
                
                if df_cost.empty:
                    st.warning("No Bill of Materials found for this product.")
                else:
                    # Calculate total cost (now only from leaf nodes)
                    total_cost = df_cost['total_cost'].sum()
                    
                    # Display total cost as metric
                    st.metric("Total Product Cost (THB)", f"฿{total_cost:,.2f}")
                    
                    # Display the detailed breakdown by level
                    st.subheader("Cost Breakdown Details by Level")
                    
                    # Format the dataframe
                    display_df = df_cost.copy()
                    display_df['unit_cost'] = display_df['unit_cost'].apply(lambda x: f"฿{x:,.2f}")
                    display_df['total_cost'] = display_df['total_cost'].apply(lambda x: f"฿{x:,.2f}")
                    
                    # Get unique levels
                    levels = sorted(display_df['level'].unique())
                    
                    # Create tabs for different views
                    tab1, tab2 = st.tabs(["Level by Level View", "Complete Breakdown"])
                    
                    with tab1:
                        # Create a dictionary to store parent components for each level
                        parent_components = {}
                        
                        # First pass: identify parent components
                        for level in levels:
                            if level > 1:  # Skip level 1 as it's the first breakdown
                                # Get components from previous level that have sub-components
                                parent_df = df_cost[df_cost['level'] == level]
                                parent_names = parent_df['hierarchy_path'].apply(lambda x: x.split(' > ')[-2]).unique()
                                parent_components[level] = ', '.join(parent_names)
                        
                        # Display table for each level
                        for level in levels:
                            level_df = display_df[display_df['level'] == level]
                            
                            # Calculate subtotal for this level using the original df_cost for accurate calculation
                            level_total = df_cost[df_cost['level'] == level]['total_cost'].sum()
                            
                            # Create header with parent component information
                            if level == 1:
                                header = f"Level {level} Components - Breakdown of {selected_product}"
                            else:
                                header = f"Level {level} Components - Breakdown of: {parent_components[level]}"
                            
                            # Add subtotal to header
                            header += f" (Subtotal: ฿{level_total:,.2f})"
                            
                            # Create a total row for this level
                            total_row = pd.DataFrame([{
                                'finished_product': '',
                                'level': None,
                                'hierarchy_path': '',
                                'component_name': 'TOTAL',
                                'qty_per_parent': None,
                                'component_uom': '',
                                'normalized_qty': None,
                                'unit_cost': '',
                                'total_cost': f'฿{level_total:.2f}'
                            }])
                            
                            # Combine level dataframe with its total row
                            level_display_df = pd.concat([level_df, total_row], ignore_index=True)
                            
                            # Create expander for each level
                            with st.expander(header, expanded=True):
                                st.dataframe(
                                    level_display_df,
                                    column_config={
                                        "finished_product": "Finished Product",
                                        "level": None,  # Hide level column since it's in the header
                                        "hierarchy_path": "Component Hierarchy",
                                        "component_name": "Component",
                                        "qty_per_parent": st.column_config.NumberColumn(
                                            "Qty per Parent",
                                            format="%.4f"
                                        ),
                                        "component_uom": "Unit of Measure",
                                        "normalized_qty": st.column_config.NumberColumn(
                                            "Normalized Qty",
                                            format="%.4f"
                                        ),
                                        "unit_cost": "Unit Cost (THB)",
                                        "total_cost": "Total Cost (THB)"
                                    },
                                    hide_index=True
                                )
                    
                    with tab2:
                        # Display the original complete table
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
                                "component_uom": "Unit of Measure",
                                "normalized_qty": st.column_config.NumberColumn(
                                    "Normalized Qty",
                                    format="%.4f"
                                ),
                                "unit_cost": "Unit Cost (THB)",
                                "total_cost": "Total Cost (THB)"
                            },
                            hide_index=True
                        )

except Exception as e:
    st.error(f"Error: {str(e)}") 