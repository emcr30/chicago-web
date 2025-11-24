# Dockerfile for CHICAGO Streamlit App
FROM python:3.11-slim

# Set workdir
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y build-essential gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt ./requirements.txt

# Install Python dependencies
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy app code
COPY . /app

# Expose Streamlit port
EXPOSE 8501

# Entrypoint
CMD ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]
