# GEO Dataset Processing Pipeline

This Luigi pipeline downloads and processes datasets from NCBI GEO (Gene Expression Omnibus).

## Features

- Downloads datasets from NCBI GEO
- Automatically extracts archives (including nested archives)
- Processes text files into pandas DataFrames
- Special handling for Probes data with full and short versions
- Automatic cleanup of intermediate files

## Installation

1. Clone this repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Run the pipeline with a GEO dataset accession number:

```bash
python -m luigi --module pipeline GEOPipeline --dataset-name GSE123456 --local-scheduler
```

Replace `GSE123456` with your desired dataset accession number.

## Pipeline Steps

1. `DownloadDataset`: Downloads the dataset and verifies it's an archive
2. `ExtractArchive`: Extracts all archives recursively
3. `ProcessTextFiles`: Processes text files into DataFrames
4. `GEOPipeline`: Wrapper task that orchestrates the entire pipeline

## Output

The pipeline creates the following directory structure:

```
.
├── {dataset_name}.archive              # Downloaded archive
├── dataset_{dataset_name}/            # Extracted files
└── processed_{dataset_name}/          # Processed CSV files
    ├── {filename}_Probes_full.csv     # Full Probes data
    ├── {filename}_Probes_short.csv    # Short Probes data (selected columns)
    └── {filename}_{section}.csv       # Other sections
```

## Error Handling

- The pipeline validates that downloaded files are archives
- Failed text file processing is logged but doesn't stop the pipeline
- Intermediate files are cleaned up after successful processing
