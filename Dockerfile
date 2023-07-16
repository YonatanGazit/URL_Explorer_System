# Use the official Python base image with version 3.9
FROM python:3.9

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install the required dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Expose port 8000 for the FastAPI server
EXPOSE 8000

# Start the FastAPI server with uvicorn when the container is run
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]