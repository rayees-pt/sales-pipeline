FROM openjdk:8-jdk

RUN update-alternatives --list java | sed 's|/bin/java||' && echo $JAVA_HOME

# Install Python and Pip
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*

# Create a symbolic link for Python3
RUN ln -s /usr/bin/python3 /usr/bin/python

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy necessary files into the Docker image
COPY data/sales_data.csv .

COPY config.ini .

COPY jars/postgresql-42.7.2.jar .

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Run sales_data_pipeline.py when the container launches
CMD ["python", "./sales_data_pipeline.py"]

