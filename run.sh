#!/bin/bash

# Step 1: Clean and install the project (compile + package)
echo "Cleaning and building the project..."
mvn clean install

# Step 2: Run the Java application (you can replace this with your specific .jar file)
echo "Running the application..."
java -jar target/backend-clipper-1.0-SNAPSHOT-jar-with-dependencies.jar
