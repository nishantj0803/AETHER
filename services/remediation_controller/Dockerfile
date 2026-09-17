FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY services/demo_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy full project for absolute imports
COPY services/ services/
COPY config/ config/

# Create non-root user for security
RUN useradd --no-create-home --system aether
USER aether

EXPOSE 8000

CMD ["uvicorn", "services.demo_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
