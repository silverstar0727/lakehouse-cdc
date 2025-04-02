FROM bitnami/spark:3.5.2

WORKDIR /app

# Copy application code
COPY . .

RUN pip install --no-cache-dir confluent-kafka, py4j==0.10.9

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Command to run the processor
CMD ["python", "processor.py"]
