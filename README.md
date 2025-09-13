# Market Activity Index Replication Package

This repository contains the replication package for *Using satellite imagery to map rural marketplaces
and monitor their activity at high frequency* (https://arxiv.org/abs/2407.12953).

## Getting Started

**For a walkthrough of the methodology underlying market detection and activity tracking, see the main demonstration notebook:**

**[`code/master_data_derivation.ipynb`](code/master_data_derivation.ipynb)**

This notebook provides a step-by-step example of the complete processing pipeline for a single candidate location in Ethiopia, from satellite imagery acquisition to final market activity analysis as shown in Figure 3C of the paper. The data underlying the paper is derived from applying this methodology to thousands of similar candidate locations. 

The figures and tables derived from this data are documented in **[`code/figures/figures_1_2_3_t1_t2_s1_s2_s3_s5.ipynb`](code/figures/figures_1_2_3_t1_t2_s1_s2_s3_s5.ipynb)** and **[`code/figures/00_master.do`](code/figures/00_master.do)**. The latter requires Stata to run. 

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
