# Market Activity Index Replication Package

This repository contains the replication package for the submission *Using satellite imagery to map rural marketplaces
and monitor their activity at high frequency* (Carnap et al.).

## Overview

This package provides code and intermediate data products for detecting periodic market activity using high-frequency satellite imagery. The methodology combines Google Earth Engine processing with python processing to identify market locations and quantify activity patterns from Planet Labs satellite data.

## Getting Started

**For a complete walkthrough of the methodology, see the main demonstration notebook:**

**[`code/master_data_derivation.ipynb`](code/master_data_derivation.ipynb)**

This notebook provides a step-by-step example of the complete processing pipeline for a single candidate location in Ethiopia, from satellite imagery acquisition to final market activity analysis as shown in Figure 3C of the paper.

## Repository Structure

- `code/` - Analysis notebooks and data processing scripts
- `datasets/` - Intermediate data products and shapefiles  
- `graphs/` - Output figures
- `temp/` - Temporary analysis files

## Requirements
See the master data derivation notebook for a detailed setup guide.

- Python 3.8+ with packages listed in `requirements.txt`
- Node.js 18+ for Google Earth Engine processing
- Google Earth Engine account (for running the full pipeline)
- Optional: Planet Labs API access (for imagery download)

## Note

Due to licensing restrictions, the original Planet Labs satellite imagery cannot be redistributed. However, all intermediate processing outputs and final analysis data are provided to enable replication of the results.