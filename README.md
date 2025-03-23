# Product Cost Analysis Dashboard

This Streamlit application provides a comprehensive product cost analysis dashboard for Blue Spice. It allows users to analyze the cost breakdown of products based on their Bill of Materials (BOM) structure.

## Features

- Hierarchical product category filtering (4 levels)
- Product selection with BOM support
- Cost analysis period selection (3, 6, or 12 months)
- Detailed cost breakdown with component hierarchy
- Total cost calculation in THB
- Interactive data tables and visualizations

## Prerequisites

- Python 3.8 or higher
- PostgreSQL database with Blue Spice data
- Access to the database with appropriate credentials

## Setup

1. Clone this repository:
```bash
git clone <repository-url>
cd product-cost-analysis
```

2. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install the required packages:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file in the project root with your database credentials:
```
db_name=your_database_name
db_username=your_username
db_password=your_password
db_host=your_host
db_port=your_port
```

## Running the Application

1. Make sure your virtual environment is activated
2. Run the Streamlit app:
```bash
streamlit run product_cost_analysis.py
```

3. Open your web browser and navigate to the URL shown in the terminal (typically http://localhost:8501)

## Usage

1. Select a product category using the hierarchical filters
2. Choose a specific product from the filtered list
3. Select a BOM for the chosen product
4. Choose the analysis period (3, 6, or 12 months)
5. Click "Analyze Cost Breakdown" to view the detailed cost analysis

## Data Structure

The application analyzes the following data:
- Product hierarchy and categories
- Bill of Materials (BOM) structure
- Purchase order prices
- Stock valuation costs
- Component quantities and relationships

## Error Handling

The application includes comprehensive error handling for:
- Database connection issues
- Missing or invalid data
- Invalid user selections
- Query execution errors

## Contributing

Feel free to submit issues and enhancement requests! 