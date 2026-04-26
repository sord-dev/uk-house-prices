import csv
import os
import psycopg
from pyproj import Transformer

transformer = Transformer.from_crs("EPSG:27700", "EPSG:4326")

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'house_prices',
    'user': 'prices',
    'password': os.getenv('POSTGRES_PASSWORD'),
}

CSV_DIR = './postcode_csv/CSV'

def ingest():
    conn = psycopg.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    files = sorted(os.listdir(CSV_DIR))
    total = 0
    
    for filename in files:
        if not filename.endswith('.csv'):
            continue
            
        filepath = os.path.join(CSV_DIR, filename)
        batch = []
        
        with open(filepath, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 4:
                    continue
                postcode = row[0].strip().upper()
                try:
                    easting = float(row[2])
                    northing = float(row[3])
                    lat, lng = transformer.transform(easting, northing)
                    batch.append((postcode, lat, lng))
                except (ValueError, IndexError):
                    continue
        
        if batch:
            cur.executemany("""
                INSERT INTO postcodes (postcode, lat, lng)
                VALUES (%s, %s, %s)
                ON CONFLICT (postcode) DO UPDATE SET
                    lat = EXCLUDED.lat,
                    lng = EXCLUDED.lng
            """, batch)
            conn.commit()
            total += len(batch)
            print(f"{filename}: {len(batch)} postcodes inserted ({total} total)")
    
    cur.close()
    conn.close()
    print(f"Done. {total} postcodes total.")

if __name__ == '__main__':
    ingest()