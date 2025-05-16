from flask import Flask, render_template, jsonify, request, send_from_directory
import sqlite3
import pandas as pd
import os
import plotly
import plotly.express as px
import plotly.graph_objects as go
import json
import threading
import time

app = Flask(__name__, static_folder='static')

# Database paths
DB_PATHS = {
    'inspections': 'inspections.db',
    'category_frequency': 'category_frequency.db',
    'outcome_analysis': 'outcome_analysis.db',
    'seasonal_analysis': 'seasonal_analysis.db',
    'output_analysis': 'output_analysis.db',
    'subcategory_frequency': 'subcategory_frequency.db'
}

# Function to get data from any database
def get_data(query, db_name='inspections'):
    try:
        conn = sqlite3.connect(DB_PATHS[db_name])
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Error executing query on {db_name}: {e}")
        return pd.DataFrame()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/static/<path:path>')
def serve_static(path):
    return send_from_directory('static', path)

# --- Routes for inspections.db ---
@app.route('/api/results_summary')
def results_summary():
    df = get_data("SELECT key, SUM(value) as value FROM results_summary GROUP BY key")
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/daily_inspections')
def daily_inspections():
    df = get_data("SELECT date, SUM(count) as count FROM daily_inspections GROUP BY date ORDER BY date")
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/monthly_trend')
def monthly_trend():
    df = get_data("SELECT month, SUM(count) as count FROM monthly_trend GROUP BY month ORDER BY month")
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/failures_by_location')
def failures_by_location():
    df = get_data("SELECT street_number, result, SUM(count) as count FROM failures_by_location WHERE result != 'PASSED' GROUP BY street_number ORDER BY count DESC ")
    return jsonify(df.to_dict(orient='records'))

# --- Routes for category_frequency.db ---
@app.route('/api/top_inspection_categories')
def top_inspection_categories():
    df = get_data("SELECT category_name, count FROM inspection_categories ORDER BY count DESC ", 'category_frequency')
    return jsonify(df.to_dict(orient='records'))

# --- Routes for outcome_analysis.db ---
@app.route('/api/outcome_categories')
def outcome_categories():
    df = get_data("SELECT c.name, COUNT(i.id) as item_count FROM categories c JOIN subcategories s ON c.id = s.category_id JOIN items i ON s.id = i.subcategory_id GROUP BY c.name ORDER BY item_count DESC", 'outcome_analysis')
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/subcategory_items')
def subcategory_items():
    category_id = request.args.get('category_id', default=1, type=int)
    df = get_data(f"""
        SELECT s.name as subcategory, SUM(i.count) as total_count 
        FROM subcategories s 
        JOIN items i ON s.id = i.subcategory_id 
        WHERE s.category_id = {category_id}
        GROUP BY s.name 
        ORDER BY total_count DESC
        
    """, 'outcome_analysis')
    return jsonify(df.to_dict(orient='records'))

# --- Routes for seasonal_analysis.db ---
@app.route('/api/day_of_week_totals')
def day_of_week_totals():
    df = get_data("SELECT day, total FROM day_of_week_totals ORDER BY total DESC", 'seasonal_analysis')
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/season_results')
def season_results():
    df = get_data("""
        SELECT season, result_type, count 
        FROM season_results 
        WHERE result_type IN (
            SELECT result_type 
            FROM season_results 
            GROUP BY result_type 
            ORDER BY SUM(count) DESC
            
        )
        ORDER BY count DESC
    """, 'seasonal_analysis')
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/month_totals')
def month_totals():
    df = get_data("SELECT month, total FROM month_totals ORDER BY month", 'seasonal_analysis')
    return jsonify(df.to_dict(orient='records'))

# --- Routes for output_analysis.db ---
@app.route('/api/top_categories')
def top_categories():
    df = get_data("""
        SELECT c.name as category, COUNT(i.id) as item_count 
        FROM categories c 
        JOIN subcategories s ON c.id = s.category_id 
        JOIN items i ON s.id = i.subcategory_id 
        GROUP BY c.name 
        ORDER BY item_count DESC
        
    """, 'output_analysis')
    return jsonify(df.to_dict(orient='records'))

# --- Routes for subcategory_frequency.db ---
@app.route('/api/subcategory_frequency')
def subcategory_frequency():
    df = get_data("SELECT category_name, count FROM inspection_categories ORDER BY count DESC LIMIT 15", 'subcategory_frequency')
    return jsonify(df.to_dict(orient='records'))

# --- Routes for day of week analysis ---
@app.route('/api/day_of_week_results')
def day_of_week_results():
    df = get_data("""
        SELECT day, result_type, count 
        FROM day_of_week_results 
        WHERE result_type IN (
            SELECT result_type 
            FROM day_of_week_results 
            GROUP BY result_type 
            ORDER BY SUM(count) DESC
            
        )
        ORDER BY count DESC
        0
    """, 'seasonal_analysis')
    return jsonify(df.to_dict(orient='records'))

# --- Additional visualization APIs for existing chart style ---
@app.route('/api/category_distribution')
def category_distribution():
    df = get_data("SELECT category_name, count FROM inspection_categories ORDER BY count DESC ", 'category_frequency')
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/seasonal_totals')
def seasonal_totals():
    df = get_data("SELECT season, total FROM season_totals ORDER BY total DESC", 'seasonal_analysis')
    return jsonify(df.to_dict(orient='records'))

@app.route('/api/season_top_results')
def season_top_results():
    df = get_data("""
        SELECT season, result_type, count 
        FROM season_results 
        WHERE result_type IN (
            SELECT result_type 
            FROM season_results 
            GROUP BY result_type 
            ORDER BY SUM(count) DESC
        )
        ORDER BY season, count DESC
    """, 'seasonal_analysis')
    return jsonify(df.to_dict(orient='records'))

# --- Generate Plotly charts data ---
@app.route('/api/charts/category_bar')
def category_bar_chart():
    df = get_data("SELECT category_name, count FROM inspection_categories ORDER BY count DESC ", 'category_frequency')
    return jsonify({"x": df['category_name'].tolist(), "y": df['count'].tolist()})

@app.route('/api/charts/season_distribution')
def season_distribution():
    df = get_data("SELECT season, total FROM season_totals ORDER BY season", 'seasonal_analysis')
    return jsonify({"labels": df['season'].tolist(), "values": df['total'].tolist()})

@app.route('/api/charts/day_distribution')
def day_distribution():
    df = get_data("SELECT day, total FROM day_of_week_totals ORDER BY total DESC", 'seasonal_analysis')
    return jsonify({"x": df['day'].tolist(), "y": df['total'].tolist()})

@app.route('/api/charts/subcategory_top')
def subcategory_top():
    df = get_data("SELECT category_name, count FROM inspection_categories ORDER BY count DESC ", 'subcategory_frequency')
    return jsonify({"labels": df['category_name'].tolist(), "values": df['count'].tolist()})

# --- Template routes for HTML ---
@app.route('/templates/index.html')
def serve_index_template():
    return render_template('index.html')

# Run the Flask server in a thread
def run_flask():
    app.run(debug=True, use_reloader=False, host='0.0.0.0', port=5000)

if __name__ == "__main__":
    # Check if all DBs exist and print availability
    print("Checking database availability:")
    for name, path in DB_PATHS.items():
        if os.path.exists(path):
            print(f"✓ {name}: {path}")
        else:
            print(f"✗ {name}: {path} (not found)")
    
    # Create templates directory if it doesn't exist
    if not os.path.exists('templates'):
        os.makedirs('templates')
    
    # Create static directory if it doesn't exist
    if not os.path.exists('static'):
        os.makedirs('static')
    
    # Write the index.html template
    with open('templates/index.html', 'w') as f:
        f.write(open('index.html').read() if os.path.exists('index.html') else "")
    
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.start()
    
    print("Dashboard running at http://localhost:5000")