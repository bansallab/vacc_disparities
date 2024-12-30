# Geographic Heterogeneity in Racial and Ethnic Disparities in COVID-19 Vaccination in the US

## Overview

This repository contains the manuscript and associated materials for the study: **"Geographic heterogeneity in racial and ethnic disparities in COVID-19 vaccination in the US"**. The work explores disparities in COVID-19 vaccination coverage across racial and ethnic groups, with a focus on urban-rural differences and the role of CDC funding. This analysis provides insights into how targeted interventions and granular data collection can improve health equity.

## Key Highlights

- **Focus:** Analysis of racial and ethnic inequities in COVID-19 vaccination across 13 U.S. states.
- **Timeframe:** Summer 2021 data.
- **Key Variables:** Urban-rural disparities, county-scale data sufficiency, and CDC funding alignment.
- **Findings:** 
  - Greater disparities in rural regions due to limited healthcare access.
  - Insufficient county-scale data poses challenges for disparity analysis.
  - CDC funding often misaligned with regions of greatest need.
- **Implications:** Importance of targeted interventions and granular data collection to address vaccination inequities.

## Coding Files

The repository includes Jupyter notebooks for the analyses conducted in the study:

- `HD_Funding Plots.ipynb`: Analyzes the impact of CDC funding on disparities affecting Hispanic and Black populations.
- `HD_Scatterplots.ipynb`: Examines the relationship between Black and Hispanic disparities and the number of individuals vaccinated within these groups.
- `T-Test Analysis.ipynb`: Performs T-tests to determine the statistical significance of observed disparities.
- `HD_maps1.ipynb`: Creates maps to visually demonstrate Black and Hispanic disparities in the US.

## Data Folders

- **`demo_vacc/`**: Contains all the demographic data used in `Compile_and_clean_state_data.py`. This includes state-level vaccination data segmented by race, ethnicity, and other demographics.
- **`other_data/`**: Includes all additional datasets required for `Compile_and_clean_state_data.py`, such as population data and FIPS codes.

## How to Use This Repository

1. **Analysis:** Explore the Jupyter notebooks in the `code/` folder for detailed analysis workflows.
2. **Data Preparation:** The `demo_vacc/` and `other_data/` folders provide the necessary input data for `Compile_and_clean_state_data.py`.
3. **Data Insights:** If additional data is included, it will be located in the respective folders for replication or further exploration.
on.

## Contact

For questions or collaborations, please reach out to:

Shweta Bansal, Ph.D  
[Institution Name]  
[Email Address]
