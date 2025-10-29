# Azure-DLT-Pipeline-for-Flight-Data


This repository contains the Python scripts for a Delta Live Tables (DLT) pipeline built on the Azure platform. The project is designed to process flight data, ingesting it from Azure Data Lake Storage (ADLS) and transforming it into a clean, analytics-ready Gold-layer dimensional model using Azure Databricks.

The pipeline is structured as a series of notebooks, with a main dlt_pipeline.py file acting as the central definition that orchestrates the other scripts.

Key Components & Project Recall (File Breakdown)
This is what each file in the project does, organized by the logical flow of data:

1.Setup.py

Purpose: This is the configuration notebook. It's the first step in the process, responsible for setting up all necessary variables, such as:

Storage paths in ADLS for raw, bronze, and gold data.

Database and table names.

Schema definitions for the raw data.

2.Bronze NoteBook.py

Purpose (Extract/Load): This notebook defines the Bronze layer. It reads the raw flight data (e.g., CSVs or JSONs) from the Azure Storage path defined in the setup script. It then ingests this data into the initial Bronze DLT table with minimal to no transformations.

3.Gold_Dims.py

Purpose (Transform/Load): This notebook creates the final Gold layer. It reads directly from the Bronze table and performs all the necessary transformations in one step:

Cleaning: Handling null values, renaming columns, and correcting data types.

Transforming: Applying business logic, possibly joining data, or creating new features.

Loading: Building the final Dimension tables (e.g., dim_flights, dim_airports, dim_passengers) and saving them as Gold DLT tables, ready for BI reporting. (Note: This project appears to be a Bronze-to-Gold pipeline, where the Silver-layer transformations are included in this script.)

dlt_pipeline.py

Purpose (Orchestration): This is the main DLT pipeline definition file. It doesn't contain the transformation logic itself. Instead, it likely uses %run commands or Python import statements to include the 1.Setup.py, 2.Bronze NoteBook.py, and 3.Gold_Dims.py notebooks, linking them together to create the full, end-to-end DLT graph.
