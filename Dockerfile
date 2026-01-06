FROM python:3.9-slim

WORKDIR /app

# Install system dependencies (required for confluent-kafka)
RUN apt-get update && apt-get install -y gcc librdkafka-dev && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app code
COPY bridge_api.py .

# Run the python script directly (it handles the PORT env var internally)
CMD ["python", "bridge_api.py"]