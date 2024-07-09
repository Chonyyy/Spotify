# Use the latest Python image as the base
FROM python:latest

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 8000 available outside the container
EXPOSE 8000

# Run app.py when the container launches
CMD ["python", "app.py"]
