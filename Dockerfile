# Use a lightweight Python image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
# We copy it first to leverage Docker cache if requirements don't change
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY run_pipeline.py .


# Copy the rest of your application code into the container
COPY src/ ./src/



# Create directories for input and output data
# These will be used with volumes
RUN mkdir -p data/input data/output

# Command to run the application - this can be overridden by docker-compose
# We won't set a specific default command here, as you'll run scripts manually
# CMD ["python", "src/main.py"] # Example if you had a main script

# Expose a port if your application were a web service (not applicable here)
# EXPOSE 8000