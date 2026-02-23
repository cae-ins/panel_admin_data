library(arrow)
library(dplyr)

# Ouvrir le dataset
ds <- open_dataset("03_data_output/base_finale/base_selectionnee_2015_2025.parquet")


names(ds)
