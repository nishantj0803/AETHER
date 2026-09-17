FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY services/demo_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy full project for absolute imports (services.demo_service.*)
COPY services/ services/

# Create non-root user for security (M11)
RUN useradd --no-create-home --system aether
USER aether

EXPOSE 8000

CMD ["uvicorn", "services.demo_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
