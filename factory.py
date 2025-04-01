import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
from collections import defaultdict
from typing import Tuple, Optional

# Set page configuration to wide mode
st.set_page_config(layout="wide")

# Load environment variables
load_dotenv()

# Database connection function
def get_database_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        port=os.getenv('DB_PORT')
    )

def get_product_data():
    conn = get_database_connection()
    query = """
    WITH base_products AS (
      SELECT 
        pt.id AS tmpl_id,
        pc.complete_name AS product_category,
        regexp_replace(pt.name->>'en_US', '\s*\(.*\)$', '') AS base_product_name,
        u.name->>'en_US' AS uom,
        TRIM(split_part(pc.complete_name, '/', 1)) AS category_level_1,
        TRIM(split_part(pc.complete_name, '/', 2)) AS category_level_2,
        TRIM(split_part(pc.complete_name, '/', 3)) AS category_level_3,
        TRIM(split_part(pc.complete_name, '/', 4)) AS category_level_4
      FROM product_template pt
      JOIN product_category pc ON pt.categ_id = pc.id
      JOIN uom_uom u ON u.id = pt.uom_id
      WHERE TRIM(split_part(pc.complete_name, '/', 1)) = 'Finished Goods'
        AND TRIM(split_part(pc.complete_name, '/', 3)) = 'Factory'
        AND lower(u.name->>'en_US') = 'carton'
    ),
    variant_data AS (
      SELECT 
        pp.id AS product_id,
        pp.product_tmpl_id,
        pp.default_code,
        MAX(
          CASE 
            WHEN pa.name::json->>'en_US' ILIKE '%Pack%' 
            THEN pav.name::json->>'en_US'
            ELSE NULL
          END
        ) AS pack_size,
        MAX(
          CASE 
            WHEN pa.name::json->>'en_US' ILIKE '%Version%' 
            THEN pav.name::json->>'en_US'
            ELSE NULL
          END
        ) AS language_code
      FROM product_product pp
      JOIN product_template pt ON pt.id = pp.product_tmpl_id
      JOIN uom_uom u ON u.id = pt.uom_id
      JOIN unnest(string_to_array(pp.combination_indices, ',')) AS attr_id ON TRUE
      JOIN product_template_attribute_value ptav ON ptav.id = trim(attr_id)::integer
      JOIN product_attribute pa ON pa.id = ptav.attribute_id
      JOIN product_attribute_value pav ON pav.id = ptav.product_attribute_value_id
      WHERE lower(u.name->>'en_US') = 'carton'
        AND pp.active = true
      GROUP BY pp.id, pp.product_tmpl_id, pp.default_code
    )
    SELECT 
        'Finished Goods 2 (Carton)' AS manufacturing_type,
        bp.category_level_1,
        bp.category_level_2,
        bp.category_level_3,
        bp.category_level_4,
        vd.default_code,
        bp.base_product_name,
        vd.language_code,
        vd.pack_size,
        bp.uom
    FROM base_products bp
    JOIN variant_data vd ON vd.product_tmpl_id = bp.tmpl_id
    ORDER BY bp.base_product_name, vd.default_code;
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def get_bom_data(default_code: str) -> Tuple[pd.DataFrame, float]:
    """
    Retrieve BOM data for a specific product.
    
    Args:
        default_code (str): Product code to lookup
        
    Returns:
        Tuple[pd.DataFrame, float]: BOM data and total cost
    """
    conn = get_database_connection()
    query = """
    WITH base_product AS (
        SELECT 
            pp.id AS product_id,
            pt.id AS tmpl_id,
            pt.name->>'en_US' AS base_product_name,
            pp.default_code
        FROM product_product pp
        JOIN product_template pt ON pt.id = pp.product_tmpl_id
        WHERE pp.default_code = %s
    ),
    bom_data AS (
        SELECT 
            b.id AS bom_id,
            b.product_tmpl_id,
            b.product_id AS bom_variant_id,
            bl.product_id AS component_id,
            bl.product_qty AS component_qty,
            u.name->>'en_US' AS component_uom
        FROM mrp_bom b
        JOIN mrp_bom_line bl ON bl.bom_id = b.id
        JOIN uom_uom u ON u.id = bl.product_uom_id
        WHERE b.active = TRUE
          AND (b.product_id IS NULL OR b.product_id = (SELECT product_id FROM base_product))
    )
    SELECT 
        bp.default_code AS finished_product_code,
        bp.base_product_name AS finished_product_name,
        b.component_id AS component_product_id,
        pt.name->>'en_US' AS component_product_name,
        b.component_qty,
        b.component_uom,
        pa.name::json->>'en_US' AS attribute_name,
        pav.name::json->>'en_US' AS attribute_value
    FROM base_product bp
    JOIN bom_data b ON b.product_tmpl_id = bp.tmpl_id
    JOIN product_product pp ON pp.id = b.component_id
    JOIN product_template pt ON pt.id = pp.product_tmpl_id
    LEFT JOIN product_template_attribute_value ptav ON ptav.product_tmpl_id = pp.product_tmpl_id
    LEFT JOIN product_attribute pa ON pa.id = ptav.attribute_id
    LEFT JOIN product_attribute_value pav ON pav.id = ptav.product_attribute_value_id
    ORDER BY component_product_name;
    """
    
    df = pd.read_sql_query(query, conn, params=(default_code,))
    conn.close()
    
    # Add FIFO cost calculation
    df['fifo_cost'] = df['component_product_id'].apply(get_fifo_cost)
    
    # Calculate component costs
    df['component_cost'] = df['fifo_cost'] * df['component_qty']
    
    # Calculate total BOM cost
    total_cost = df['component_cost'].sum()
    
    return df, total_cost

def get_fifo_cost(product_id):
    conn = get_database_connection()
    query = """
    SELECT svl.id, svl.unit_cost, svl.quantity, svl.create_date
    FROM stock_valuation_layer svl
    JOIN product_product pp ON pp.id = svl.product_id
    WHERE pp.id = %s
    ORDER BY svl.create_date ASC;
    """
    
    df = pd.read_sql(query, conn, params=(product_id,))
    conn.close()
    
    # Initialize a list to hold batches with remaining quantities
    remaining_batches = []
    
    # Iterate through each move in ascending order
    for _, row in df.iterrows():
        qty = row['quantity']
        cost = row['unit_cost']
        if qty > 0:
            # Incoming stock: add a new batch
            remaining_batches.append({
                'unit_cost': cost,
                'remaining': qty,
                'create_date': row['create_date']
            })
        else:
            # Outgoing stock: subtract from earliest batches
            consumption = -qty
            while consumption > 0 and remaining_batches:
                batch = remaining_batches[0]
                if batch['remaining'] > consumption:
                    batch['remaining'] -= consumption
                    consumption = 0
                else:
                    consumption -= batch['remaining']
                    remaining_batches.pop(0)
    
    # Return the FIFO cost (unit_cost of the first remaining batch)
    return remaining_batches[0]['unit_cost'] if remaining_batches else 0

def calculate_monthly_bom_cost(bom_df):
    """
    Calculate monthly total BOM cost using a single-pass approach for all components.
    """
    # Set start date
    start_date = pd.Timestamp('2025-01-01')
    
    # Create a list to store all component moves
    all_moves = []
    
    # For each component in the BOM
    for _, row in bom_df.iterrows():
        comp_id = row['component_product_id']
        comp_qty = row['component_qty']
        
        # Get component cost data
        conn = get_database_connection()
        query = """
        SELECT 
            svl.unit_cost,
            svl.quantity,
            svl.create_date,
            %s as bom_qty  -- Include BOM quantity in the query
        FROM stock_valuation_layer svl
        JOIN product_product pp ON pp.id = svl.product_id
        WHERE pp.id = %s
          AND svl.quantity > 0  -- Only consider positive quantities (production events)
        ORDER BY svl.create_date ASC;
        """
        
        df = pd.read_sql(query, conn, params=(comp_qty, int(comp_id)))
        conn.close()
        
        if not df.empty:
            all_moves.append(df)
    
    if not all_moves:
        return pd.DataFrame()
    
    # Combine all component moves into one DataFrame
    combined_moves = pd.concat(all_moves, ignore_index=True)
    
    # Convert create_date to datetime and set as index
    combined_moves['create_date'] = pd.to_datetime(combined_moves['create_date'])
    combined_moves.set_index('create_date', inplace=True)
    
    # Calculate total cost considering BOM quantities
    combined_moves['total_cost'] = combined_moves['unit_cost'] * combined_moves['quantity'] * combined_moves['bom_qty']
    combined_moves['total_qty'] = combined_moves['quantity'] * combined_moves['bom_qty']
    
    # Resample to monthly and compute weighted average
    monthly = combined_moves.resample('M').agg({
        'total_cost': 'sum',
        'total_qty': 'sum'
    })
    
    # Calculate monthly weighted average cost
    monthly['total_bom_cost'] = monthly.apply(
        lambda row: row['total_cost'] / row['total_qty'] if row['total_qty'] != 0 else None,
        axis=1
    )
    
    # Filter data starting from January 1, 2025
    monthly = monthly[monthly.index >= start_date]
    
    # Handle missing months by forward filling (optional)
    # monthly['total_bom_cost'] = monthly['total_bom_cost'].fillna(method='ffill')
    
    # Drop months with no data
    monthly = monthly.dropna(subset=['total_bom_cost'])
    
    # Add rolling average to smooth out spikes (optional)
    monthly['rolling_average'] = monthly['total_bom_cost'].rolling(
        window=3,  # 3-month rolling average
        min_periods=1
    ).mean()
    
    return monthly[['total_bom_cost', 'rolling_average']]

def get_bom_tree(finished_product_code):
    """
    Retrieve the full BOM hierarchy using a recursive query.
    Returns a DataFrame with BOM identifiers, levels, paths, and costs for each component.
    Only includes active BOMs.
    """
    conn = get_database_connection()
    query = """
    WITH RECURSIVE bom_tree AS (
      -- Level 0: The finished product itself as the root
      SELECT
        NULL::integer AS bom_id,
        NULL::text AS bom_name,
        p.id AS parent_product_id,
        p.default_code AS parent_code,
        p.id AS component_product_id,
        p.default_code AS component_code,
        pt.name->>'en_US' AS component_name,
        1.0::numeric AS product_qty,
        (uu.name::json)->>'en_US' AS uom,
        0 AS level,
        p.default_code AS path,
        (
          SELECT svl.unit_cost 
          FROM stock_valuation_layer svl 
          WHERE svl.product_id = p.id 
            AND svl.quantity > 0
          ORDER BY svl.create_date ASC 
          LIMIT 1
        ) AS cost
      FROM product_product p
      JOIN product_template pt ON pt.id = p.product_tmpl_id
      JOIN uom_uom uu ON uu.id = pt.uom_id
      WHERE p.default_code = %s
      
      UNION ALL
      
      -- Recursive part: Get BOM components for each level
      SELECT
        mb.id AS bom_id,
        mb.code AS bom_name,
        p.id AS parent_product_id,
        p.default_code AS parent_code,
        bl.product_id AS component_product_id,
        cp.default_code AS component_code,
        pt.name->>'en_US' AS component_name,
        bl.product_qty,
        (uu.name::json)->>'en_US' AS uom,
        bt.level + 1 AS level,
        bt.path || ' > ' || cp.default_code AS path,
        (
          SELECT svl.unit_cost 
          FROM stock_valuation_layer svl 
          WHERE svl.product_id = cp.id 
            AND svl.quantity > 0
          ORDER BY svl.create_date ASC 
          LIMIT 1
        ) AS cost
      FROM mrp_bom mb
      JOIN product_product p ON p.id = mb.product_id
      JOIN mrp_bom_line bl ON bl.bom_id = mb.id
      JOIN product_product cp ON cp.id = bl.product_id
      JOIN product_template pt ON pt.id = cp.product_tmpl_id
      JOIN uom_uom uu ON uu.id = bl.product_uom_id
      JOIN bom_tree bt ON bt.component_product_id = p.id
      WHERE mb.active = true
    )
    SELECT *
    FROM bom_tree
    ORDER BY level, bom_id, path;
    """
    df = pd.read_sql_query(query, conn, params=(finished_product_code,))
    conn.close()
    return df

def create_sidebar_filters(df):
    filters = {
        'category_4': st.sidebar.selectbox('Category Level 4', ['All'] + sorted(df['category_level_4'].dropna().unique())),
        'product': st.sidebar.selectbox('Base Product Name', ['All'] + sorted(df['base_product_name'].dropna().unique())),
        'language': st.sidebar.selectbox('Language Code', ['All'] + sorted(df['language_code'].dropna().unique())),
        'pack_size': st.sidebar.selectbox('Pack Size', ['All'] + sorted(df['pack_size'].dropna().unique()))
    }
    return filters

def apply_filters(df, filters):
    filtered_df = df.copy()
    for field, value in filters.items():
        if value != 'All':
            filtered_df = filtered_df[filtered_df[field] == value]
    return filtered_df

def main():
    st.title('Bill of Materials Explorer')
    
    # Get the initial data for filters
    df = get_product_data()
    
    # Create filters in the sidebar
    st.sidebar.header('Filters')
    
    # Start with a copy of the original dataframe
    filtered_df = df.copy()
    
    # Category Level 4 filter
    category_4_options = ['All'] + sorted(df['category_level_4'].dropna().unique().tolist())
    selected_category_4 = st.sidebar.selectbox('Category Level 4', category_4_options)
    
    # Apply category filter and update subsequent options
    if selected_category_4 != 'All':
        filtered_df = filtered_df[filtered_df['category_level_4'] == selected_category_4]
    
    # Base Product Name filter
    product_options = ['All'] + sorted(filtered_df['base_product_name'].dropna().unique().tolist())
    selected_product = st.sidebar.selectbox('Base Product Name', product_options)
    
    # Apply product filter
    if selected_product != 'All':
        filtered_df = filtered_df[filtered_df['base_product_name'] == selected_product]
    
    # Language Code filter
    language_options = ['All'] + sorted(filtered_df['language_code'].dropna().unique().tolist())
    selected_language = st.sidebar.selectbox('Language Code', language_options)
    
    # Apply language filter
    if selected_language != 'All':
        filtered_df = filtered_df[filtered_df['language_code'] == selected_language]
    
    # Pack Size filter
    pack_size_options = ['All'] + sorted(filtered_df['pack_size'].dropna().unique().tolist())
    selected_pack_size = st.sidebar.selectbox('Pack Size', pack_size_options)
    
    # Apply pack size filter
    if selected_pack_size != 'All':
        filtered_df = filtered_df[filtered_df['pack_size'] == selected_pack_size]
    
    # Show number of filtered products
    st.sidebar.write(f"Products matching filters: {len(filtered_df)}")
    
    # Add Load BOM button
    if st.sidebar.button('Load BOM'):
        if len(filtered_df) == 1:
            default_code = filtered_df['default_code'].iloc[0]
            bom_df, total_cost = get_bom_data(default_code)
            bom_tree_df = get_bom_tree(default_code)  # Get the hierarchical BOM data
            
            if not bom_df.empty:
                # Display warning message about cost discrepancies
                st.warning("""
                ### Important Note About Cost Discrepancies
                
                In many ERP systems, the "official cost" of the finished product (the top‐level BOM item) may be a standard cost or a frozen cost that doesn't automatically recalculate whenever sub‐component costs change. Meanwhile, if you look at the sub‐BOM's components (lower‐level raw materials, ingredients, packaging, etc.), you might be using current or actual costs for each item. This leads to a situation where adding up all the lower‐level component costs can exceed the "official" cost recorded at the top level.

                **Here's an easy breakdown of why this can happen:**

                1. **Top‐Level Cost is Fixed or Outdated:**
                   - Some ERPs store a standard or frozen cost for the finished product
                   - This cost might have been set a while ago and not updated regularly
                   - As raw material prices, labor costs, or overhead change, the sub‐BOM components may reflect more recent, higher costs

                2. **Different Cost Methods:**
                   - The finished product might be on standard costing, while sub-components use average or FIFO costing
                   - Standard costing locks in a cost, whereas average or FIFO costing lets costs fluctuate

                3. **Overhead or Packaging Discrepancies:**
                   - Top‐level BOM cost might not include certain overhead charges or packaging details
                   - Sub‐BOM components may include more detailed packaging, labor, or overhead items

                4. **Timing Differences:**
                   - Official cost may reflect older, cheaper raw material prices
                   - Current BOM breakdown uses more recent (and potentially higher) cost data

                5. **Data Entry or Maintenance Gaps:**
                   - Recent sub‐BOM changes (new ingredients, different packaging) might not be reflected in top-level cost
                   - Incomplete updates in the ERP system can cause discrepancies
                """)
                
                # Display finished product info
                finished_product_info = {
                    "Product Code": bom_df['finished_product_code'].iloc[0],
                    "Product Name": bom_df['finished_product_name'].iloc[0],
                }
                st.subheader("Finished Product Details")
                st.json(finished_product_info)
                
                # Display flat BOM list
                bom_display = bom_df[[
                    'component_product_name',
                    'component_qty',
                    'component_uom',
                    'attribute_name',
                    'attribute_value',
                    'fifo_cost',
                    'component_cost'
                ]].copy()
                
                # Rename columns to more human-readable names
                bom_display.columns = [
                    'Component Name',
                    'Quantity',
                    'Unit of Measure',
                    'Attribute Type',
                    'Attribute Value',
                    'Current FIFO Cost',
                    'Total Component Cost'
                ]
                
                # Format cost columns with THB symbol and thousands separator
                bom_display['Current FIFO Cost'] = bom_display['Current FIFO Cost'].apply(lambda x: f"฿{x:,.2f}")
                bom_display['Total Component Cost'] = bom_display['Total Component Cost'].apply(lambda x: f"฿{x:,.2f}")
                
                
                # Display hierarchical BOM
                st.subheader("BOM Hierarchy by Level")
                if not bom_tree_df.empty:
                    # Define base annotations for each level
                    level_annotations = {
                        0: "Level 0: Finished Product (Official Cost as recorded by ERP)",
                        1: "Level 1: Direct BOM Components of {}",
                        2: "Level 2: Sub BOM Components of {}",
                        3: "Level 3: Sub-Sub BOM Components of {}"
                    }
                    
                    # Display each level separately
                    levels = sorted(bom_tree_df['level'].unique())
                    for lvl in levels:
                        # Get parent information for levels > 0
                        if lvl > 0:
                            # Get unique parents for this level with their names, quantities, and UOMs
                            level_data = bom_tree_df[bom_tree_df['level'] == lvl]
                            # Get unique parent details by looking up where component_code matches parent_code
                            parent_details = []
                            for parent_code in level_data['parent_code'].unique():
                                parent_row = bom_tree_df[bom_tree_df['component_code'] == parent_code].iloc[0]
                                parent_detail = f"{parent_row['component_name']} ({parent_row['product_qty']} {parent_row['uom']})"
                                parent_details.append(parent_detail)
                            parent_str = ", ".join(parent_details)
                            # Format the annotation with parent information
                            annotation = level_annotations.get(lvl, f"Level {lvl}: Additional Components of {{}}").format(parent_str)
                        else:
                            # Level 0 doesn't need parent information
                            annotation = level_annotations.get(lvl)
                        
                        st.markdown(f"### {annotation}")
                        
                        df_level = bom_tree_df[bom_tree_df['level'] == lvl].copy()
                        
                        # Calculate total cost for each component
                        df_level['total_cost'] = df_level['product_qty'] * df_level['cost']
                        
                        # Calculate level total before formatting
                        level_total = df_level['total_cost'].sum()
                        
                        # Group by parent BOM for levels > 0
                        if lvl > 0:
                            # Get unique parent BOMs
                            parent_boms = df_level['parent_code'].unique()
                            
                            # Create tabs for each parent BOM
                            tab_titles = []
                            for parent_code in parent_boms:
                                # Look up parent details from the full bom_tree_df instead of df_level
                                parent_row = bom_tree_df[bom_tree_df['component_code'] == parent_code]
                                if not parent_row.empty:
                                    parent_row = parent_row.iloc[0]
                                    tab_title = f"{parent_row['component_name']} ({parent_row['product_qty']} {parent_row['uom']})"
                                else:
                                    # Fallback if parent not found
                                    tab_title = f"Parent BOM {parent_code}"
                                tab_titles.append(tab_title)
                            
                            # Create tabs
                            tabs = st.tabs(tab_titles)
                            
                            # Display data for each parent BOM
                            for tab, parent_code in zip(tabs, parent_boms):
                                with tab:
                                    # Filter data for this parent BOM
                                    parent_data = df_level[df_level['parent_code'] == parent_code].copy()
                                    
                                    if not parent_data.empty:
                                        # Calculate parent total before formatting
                                        parent_total = parent_data['total_cost'].sum()
                                        
                                        # Display columns
                                        display_cols = [
                                            'component_code', 
                                            'component_name', 
                                            'product_qty', 
                                            'uom', 
                                            'cost', 
                                            'total_cost', 
                                            'path'
                                        ]
                                        
                                        # Format cost columns for display
                                        display_data = parent_data[display_cols].copy()
                                        display_data['cost'] = display_data['cost'].apply(lambda x: f"฿{x:,.2f}")
                                        display_data['total_cost'] = display_data['total_cost'].apply(lambda x: f"฿{x:,.2f}")
                                        
                                        st.dataframe(
                                            display_data,
                                            use_container_width=True
                                        )
                                        
                                        # Show subtotal for this parent BOM
                                        st.write(f"**Subtotal for this BOM:** ฿{parent_total:,.2f}")
                                    else:
                                        st.warning(f"No components found for parent BOM {parent_code}")
                        else:
                            # For level 0, display all data without grouping
                            display_cols = [
                                'component_code', 
                                'component_name', 
                                'product_qty', 
                                'uom', 
                                'cost', 
                                'total_cost', 
                                'path'
                            ]
                            
                            # Format cost columns for display
                            display_data = df_level[display_cols].copy()
                            display_data['cost'] = display_data['cost'].apply(lambda x: f"฿{x:,.2f}")
                            display_data['total_cost'] = display_data['total_cost'].apply(lambda x: f"฿{x:,.2f}")
                            
                            st.dataframe(
                                display_data,
                                use_container_width=True
                            )
                        
                        # Show level total
                        st.write(f"**Total Cost for Level {lvl}:** ฿{level_total:,.2f}")
                else:
                    st.warning("No BOM hierarchy data available.")
                
            else:
                st.warning("No BOM found for this product.")
        else:
            st.warning("Please apply filters to select exactly one product.")

if __name__ == "__main__":
    main()