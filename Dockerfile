FROM python:3.9

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p logs

# Copy application code
COPY . .

# Set Python to run in unbuffered mode
ENV PYTHONUNBUFFERED=1

# Run the ETL pipeline when the container starts
CMD ["python", "main.py"]