
# Scalable Log Analysis using MapReduce and Spark

## Overview

This project focuses on developing scalable cloud computing solutions to efficiently analyze large log files generated by supercomputers, specifically the BlueGene/L dataset from Lawrence Livermore National Laboratory. The aim is to process, analyze, and extract meaningful insights from logs using frameworks like Hadoop MapReduce, Spark RDD, and Spark SQL.

The project emphasizes scalability, performance, and efficient use of parallel programming design patterns while addressing specific questions related to log file analysis.

## Table of Contents
1. [Introduction](#introduction)
2. [Features](#features)
3. [Installation](#installation)
4. [Usage](#usage)
5. [Technologies and Dependencies](#technologies-and-dependencies)
6. [Key Results](#key-results)
7. [Contributors](#contributors)
8. [License](#license)

## Introduction

Scalable computing solutions have transformed the processing of large datasets, enabling parallel and distributed processing for increased performance. This project addresses log file analysis tasks like identifying errors, calculating frequencies, and determining trends in a scalable manner.

The objectives include:
- Leveraging Hadoop MapReduce, Spark RDD, and Spark SQL to solve complex log analysis tasks.
- Ensuring performance through parallel processing.
- Demonstrating scalability by utilizing multiple nodes and design patterns.

## Features

1. **Parallel Processing**: Efficiently process large datasets using Hadoop MapReduce and Apache Spark.
2. **Scalable Design**: Utilize distributed computing to handle growing datasets without performance degradation.
3. **Log Analysis Tasks**:
   - Count specific fatal error occurrences.
   - Calculate averages and group data by time intervals.
   - Identify top occurrences, specific events, and error trends.
4. **Performance Comparison**: Compare Spark and Hadoop MapReduce in terms of latency and throughput.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/ashwinsabu/Scalable-Programming.git
   ```
2. Set up a Hadoop and Spark environment:
   - Install Hadoop (Version 3.3.1 or later) on your system.
   - Install Apache Spark (Version 3.2.1 or later).
3. Place the log file in the Hadoop Distributed File System (HDFS):
   ```bash
   hdfs dfs -mkdir /logs
   hdfs dfs -put /path/to/logfile /logs
   ```
4. Execute MapReduce or Spark jobs based on the task requirements.

## Usage

Run the respective Python scripts for each task. For example:

1. **MapReduce Task**:
   ```bash
   hadoop jar log_analysis.jar Mapper Reducer /logs/input /logs/output
   ```

2. **Spark Task**:
   ```bash
   spark-submit task_script.py
   ```

## Technologies and Dependencies

- **Programming Language**: Python
- **Frameworks**: Hadoop MapReduce, Apache Spark (RDD and SQL)
- **Development Environment**: Ubuntu 22.04, VirtualBox
- **Dataset**: BlueGene/L supercomputer logs

## Key Results

1. **Top Dates by Occurrences**:
   - Example: `2005.07.09: 381,827 occurrences`
2. **Error Trends by Hour**:
   - Example: `Hour 08: 57,420,982 seconds (average duration)`
3. **Earliest Fatal Error**:
   - `2005-08-03-15.35.34.555500`
4. **Top Nodes by Events**:
   - Example: `Node R00-M0-NB: 192 events`

## Contributors

- **Ashwin Sabu**
  - **Email**: [x23196505@student.ncirl.ie](mailto:x23196505@student.ncirl.ie)
  - **Institution**: National College of Ireland

## License

This project is for academic purposes and does not currently include a specific license.