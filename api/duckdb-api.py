from flask import Flask, jsonify
import duckdb
from datetime import datetime
import pandas as pd
app = Flask(__name__)

@app.route('/metrics')
def metrics():
    try:
        con = duckdb.connect('/data/market_data.duckdb')
        df = con.execute("""
            SELECT 
                symbol, 
                COUNT(*) as tick_count, 
                ROUND(AVG(price), 2) as avg_price,
                ROUND(MAX(price), 2) as max_price, 
                ROUND(MIN(price), 2) as min_price
            FROM market_ticks GROUP BY symbol
        """).df()
        con.close()
        # ADD TIME IN PYTHON (Grafana loves it)
        df['time'] = pd.Timestamp.now().isoformat()
        return jsonify(df.to_dict('records'))
    except Exception as e:
        return jsonify([{"error": str(e)}])
    
@app.route('/live')
def live():
    try:
        con = duckdb.connect('/data/market_data.duckdb')
        
        df = con.execute("""
            WITH recent_data AS (
                SELECT ts, symbol, price, volume
                FROM market_ticks 
                WHERE ts IN (
                    SELECT DISTINCT ts 
                    FROM market_ticks
                    GROUP BY ts
                    HAVING COUNT(DISTINCT symbol) = 3
                    ORDER BY ts DESC
                    LIMIT 50
                )
            )
            SELECT 
                ts as time,
                -- Price columns
                MAX(CASE WHEN symbol = 'SP500' THEN price END) as SP500,
                MAX(CASE WHEN symbol = 'STOXX600' THEN price END) as STOXX600,
                MAX(CASE WHEN symbol = 'NIKKEI225' THEN price END) as NIKKEI225,
                -- Volume columns
                MAX(CASE WHEN symbol = 'SP500' THEN volume END) as SP500_volume,
                MAX(CASE WHEN symbol = 'STOXX600' THEN volume END) as STOXX600_volume,
                MAX(CASE WHEN symbol = 'NIKKEI225' THEN volume END) as NIKKEI225_volume
            FROM recent_data
            GROUP BY ts
            ORDER BY ts DESC
        """).df()
        
        con.close()
        
        # Format timestamp
        df['time'] = pd.to_datetime(df['time']).dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        return jsonify(df.to_dict('records'))
    except Exception as e:
        return jsonify([{"error": str(e)}])

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
