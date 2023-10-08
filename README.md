# Top 10 Mutual Friends

## Overview
This Hadoop MapReduce program is designed to identify the top ten pairs of friends who share the highest number of mutual friends among them.

## Usage
To use this program, follow these steps:

1. **Data Input**: Ensure you have the input data containing information about friendships and mutual connections.

2. **Configuration**: Make necessary configurations in the MapReduce job, such as specifying the input and output paths.

3. **Execution**: Run the Hadoop MapReduce job to analyze the data and find the top ten pairs of friends with the most mutual connections.

4. **Results**: The results will be stored in the specified output path, showing the top ten pairs of friends with the highest mutual friend counts.

## Example
Here's a sample command for running the program:

```shell
hadoop jar Top10MutualFriends.jar inputPath outputPath

