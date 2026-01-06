# Use an official lightweight Python image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies required for confluent-kafka (librdkafka)
# git and gcc are often needed for compiling certain python extensions
RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file (if you have one) or install directly
# For simplicity, we install directly here based on your previous needs
RUN pip install --no-cache-dir fastapi uvicorn confluent-kafka pydantic

# Copy the bridge_api code into the container
COPY bridge_api.py .

# Expose the port the app runs on
EXPOSE 8000

# Command to run the application
CMD ["uvicorn", "bridge_api:app", "--host", "0.0.0.0", "--port", "8000"]