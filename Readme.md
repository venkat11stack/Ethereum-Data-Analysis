# Ethereum Data Analysis with Apache Spark

This repository contains a suite of applications developed to analyze Ethereum blockchain data using Apache Spark. The analysis covers several aspects including transaction patterns, smart contract activity, miner data, scam identification, gas usage, and data storage overhead.

## Project Structure

- `transaction.py` & `transaction.ipynb`: Monthly transaction count analysis.
- `average.py` & `average.ipynb`: Monthly average transaction value computation.
- `top10services.py` & `top10services.ipynb`: Identification of top 10 smart contracts by total Ether received.
- `top10miners.py` & `top10miners.ipynb`: Analysis of top 10 miners by block size.
- `scams.py`, `scams_output.ipynb`, `visualization.ipynb`: Scam analysis and categorization.
- `gasguzzlers.py`, `gas_visualization.ipynb`: Gas price and usage trends over time.
- `overhead.py`: Analysis of potential data storage savings by removing certain block table columns.

Each Python script performs data processing using Spark, while the Jupyter Notebooks (`*.ipynb`) are used for further analysis and visualization of the results.

## How to Run

1. Set up your Spark environment with access to the Ethereum dataset.
2. Execute the following command to run a script (replace `script_name.py` with the actual script name): ccc create spark script_name.py -s -e [number of executors]
3. Use the `ccc method bucket cp` command to copy output files from the S3 bucket to local storage for further analysis.

## Results

Results of the analysis are saved in text files, with some visualizations provided in Jupyter Notebooks. Here is a brief overview of the findings:

- **Time Analysis**: The monthly transaction count and average transaction value provide insights into the Ethereum network's usage over time.
- **Top Ten Services**: Smart contracts with the highest Ether receipts reveal the most popular services on Ethereum.
- **Top Ten Most Active Miners**: The analysis identifies miners that have contributed most to the network in terms of block size.
- **Scam Analysis**: A detailed investigation into scams, categorizing them and tracking Ether flow over time.
- **Gas Analysis**: Examination of gas price and usage trends, offering an understanding of network congestion and smart contract complexity.
- **Data Overhead**: Estimates the potential savings in storage space by removing certain columns from the block data.

## Visualizations

Visualizations are included in the respective Jupyter Notebooks and provide graphical representations of the data and trends identified in the analysis.

## Requirements

- Apache Spark 3.0.1
- Python 3.7 or above
- Jupyter Notebook for visualizations
- Access to an S3 bucket with Ethereum dataset

## Note

The Ethereum dataset was available for the duration of the coursework and may not be accessible currently. Users looking to replicate this analysis should ensure they have access to a similar dataset.
