# Duedil

This repo is part of a duedil data engineer assignment, whose problem statement can be found in the repo itself.
 
 
## Prerequisites
Following software should be installed on the system and their locations should be inlcuded in the system PATH - 
1. Oracle Java 1.8 
2. Apache Spark
3. Apache Maven

## Build instructions
Goto the directory containing pom.xml and run the following command-

`mvn clean install`

 This will create a new directory target that will contain the built jar.
 
## Run instructions
Run the following command to start program execution-

`spark-submit --class "app.Driver" --master local[4] path/to/jar path/to/input path/to/output1 path/to/output2 path/to/output3`

Test data is present in the resource directory under src/main. Run the following command to run unit tests - 
 
 `spark-submit --class "testing.TestDriver" --master local[4] path/to/jar path/to/test_input path/to/expected_output1 path/to/expected_output2 path/to/expected_output3`
 
## Authors
 Harshit Jain

## Acknowledgement
- Apache Spark Official Documentation (https://spark.apache.org/docs/latest/)

- StackOverflow

## End