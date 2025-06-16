# Sales Data Processing Pipeline

This project contains a Python script designed to process and analyze sales data files. The script reads compressed sales datasets, combines them with product information, filters relevant data, performs aggregation, and calculates sales indices for reporting purposes.

## Features

- Reads multiple compressed (.gz) sales data files from a specified directory.
- Extracts year and month information from file names.
- Filters sales data by specific product types.
- Merges sales data with product metadata.
- Groups and aggregates sales by sales point ID, product type, year, and month.
- Calculates unit and revenue-based sales indices.
- Filters incomplete data and computes median indices to fill missing values.
- Prepares summarized datasets for further analysis.

## Requirements

- Python 3.7 or higher
- pandas
- numpy

## Installation

You can install the required Python packages using pip:

```bash
pip install pandas numpy
