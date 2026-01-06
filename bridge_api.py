from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from confluent_kafka import Producer
from databricks import sql
import os
import json
import random

app = FastAPI()

# --- CONFLUENT KAFKA CONFIGURATION ---
# Now reads from Environment Variables
conf = {
    'bootstrap.servers': os.environ.get('CONFLUENT_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.environ.get('CONFLUENT_API_KEY'),
    'sasl.password': os.environ.get('CONFLUENT_API_SECRET'),
    'client.id': 'ios-bridge-api'
}

# --- DATABRICKS CONFIGURATION ---
DB_SERVER_HOSTNAME = os.environ.get('DATABRICKS_SERVER_HOSTNAME')
DB_HTTP_PATH = os.environ.get('DATABRICKS_HTTP_PATH')
DB_ACCESS_TOKEN = os.environ.get('DATABRICKS_ACCESS_TOKEN')

# Initialize Producer
# We check if configuration is present to avoid immediate crash on boot if env vars are missing
try:
    if conf['bootstrap.servers'] and conf['sasl.username']:
        producer = Producer(conf)
    else:
        print("WARNING: Confluent Env Vars missing. Producer disabled.")
        producer = None
except Exception as e:
    print(f"Kafka Producer Init Error: {e}")
    producer = None

class CrimeReport(BaseModel):
    report_id: str
    crime_type: str
    latitude: float
    longitude: float
    user_id: str

@app.get("/")
def home():
    return {"status": "CrimeWatch API is running"}

@app.post("/report-crime")
async def report_crime(report: CrimeReport):
    print(f"Received Report: {report}")
    
    value = json.dumps({
        "report_id": report.report_id,
        "user_id": report.user_id,
        "crime_type": report.crime_type,
        "latitude": report.latitude,
        "longitude": report.longitude
    })
    
    try:
        if producer:
            producer.produce('crime-reports-topic', key=report.user_id, value=value)
            producer.flush()
            return {"status": "success", "message": "Crime reported to data pipeline"}
        else:
            # If producer is None (missing env vars), we log it but don't crash
            print("Simulation: Report received but not sent (Producer not configured).")
            return {"status": "simulated", "message": "Report received (Producer disabled)"}
    except Exception as e:
        print(f"Error sending to Kafka: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/danger-zones")
async def get_danger_zones():
    # If credentials are missing, skip straight to fallback
    if not (DB_SERVER_HOSTNAME and DB_HTTP_PATH and DB_ACCESS_TOKEN):
        print("Databricks Env Vars missing. Returning Mock Data.")
        return {"zones": _get_mock_zones()}

    try:
        print("Connecting to Databricks...")
        connection = sql.connect(
            server_hostname=DB_SERVER_HOSTNAME,
            http_path=DB_HTTP_PATH,
            access_token=DB_ACCESS_TOKEN
        )
        cursor = connection.cursor()
        
        cursor.execute("SELECT zone_id, center_lat, center_lon, radius_meters FROM gold_danger_zones")
        result = cursor.fetchall()
        
        zones = []
        for row in result:
             zones.append({
                "zone_id": str(row.zone_id),
                "center_lat": float(row.center_lat),
                "center_lon": float(row.center_lon),
                "radius_meters": float(row.radius_meters)
             })
        
        cursor.close()
        connection.close()
        return {"zones": zones}

    except Exception as e:
        print(f"Databricks/Connection Error: {e}")
        return {"zones": _get_mock_zones()}

def _get_mock_zones():
    return [
        {"zone_id": "zone_macroplaza", "center_lat": 25.6691, "center_lon": -100.3129, "radius_meters": 500},
        {"zone_id": "zone_tec", "center_lat": 25.6514, "center_lon": -100.2905, "radius_meters": 400}
    ]

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)