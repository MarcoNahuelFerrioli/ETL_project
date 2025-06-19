# Create image based on Python 3.11.9
FROM python:3.11.9

# Create working directory called 'app'
WORKDIR /app

# Copy the requirements file for the project
COPY requirements.txt .

# Install dependencies without caching to reduce image size
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project into the container
COPY . .


CMD ["tail", "-f", "/dev/null"]