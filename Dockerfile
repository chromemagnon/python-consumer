
FROM python:3.9-alpine

# Install Python 3 and pip
# RUN apk add --no-cache python3 py3-pip

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the project files into the working directory
COPY . .

# Install the required Python libraries
RUN pip install pika==1.3.2 pydantic==2.5.3 mysql-connector-python==8.2.0 sqlalchemy==2.0.23

# Command to run on container start
CMD [ "python3", "./main.py" ]
#CMD /bin/sh -c "sleep 10; python3 ./main.py"
