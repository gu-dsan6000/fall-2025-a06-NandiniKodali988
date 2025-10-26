---
title: Analysis Report
author: Nandini Kodali
---

# Problem 1: Log Level Distribution

## Apporach

I used PySpark to analyse the log files stores in the S3 bucket and determine the distribution of different log levels.<br>

- The python script creates a Spark session connected to the cluster master and read all the log files recurssively.<br>
- Using regex, I extracted standard log levels (INFO, WARN, ERROR, DEBUG) from each line. <br> 
- Filtered out the lines with the requried log level, adn aggregated the counts for each category. <br>
- Wrote a random sample of ten log lines to review message types. <br>
- Generated summary statistics contianing info on total lines processes, lines containing required levels, and distribution of each level. <br>

## Key findings

| Log Level | Count     |
|------------|-----------:|
| INFO | 27,389,482 |
| WARN | 9,595 |
| ERROR | 11,259 |
| DEBUG | 0 |
 
The log analysis revealed that nearly all cluster log entries were informational, with INFO messages accounting for about 99.9% of all lines containing identifiable log levels. These messages primarily document normal Spark operations such as task execution, data storage in memory, and file output commits to HDFS, indicating smooth job progression. A very small portion of logs were classified as WARN (0.04%) or ERROR (0.04%), suggesting that the cluster experienced only minor or transient issues during execution.

## Spark Web UI

![](images/problem1_8080.png)

## Applixation UI

![](images/problem1_4040.png)
![](images/problem1_4040_executors.png)

