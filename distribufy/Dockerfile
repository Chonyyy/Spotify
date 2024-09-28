# Use a base Python image
FROM python:latest

# Set the working directory
WORKDIR /app

# Copy common files and install dependencies
COPY requirements.txt app.py server.py common/ /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy databases and logs directories
COPY databases/ /app/databases/

# Set default command to bash
CMD ["bash"]
