library(data.table)
library(finutils)
library(roll)


# Set up
PATH_DATASET = "D:/strategies/season"

# Import daily data
prices = qc_daily(
  file_path = "F:/lean/data/stocks_daily.csv",
  min_obs = 3 * 252,
  price_threshold = 1e-008,
  add_dv_rank = TRUE
)

# define target variables
setorder(prices, symbol, date)
prices[, return_day := shift(close, 1, type = "lead") / close - 1, by = symbol]
prices[, return_day2 := shift(close, 2, type = "lead") / close - 1, by = symbol]
prices[, return_day3 := shift(close, 3, type = "lead") / close - 1, by = symbol]
prices[, return_week := shift(close, 5, type = "lead") / close - 1, by = symbol]
prices[, return_week2 := shift(close, 10, type = "lead") / close - 1, by = symbol]

# define frequency unit
prices[, month := data.table::yearmon(date)]
setorder(prices, symbol, date)
prices[, day_of_month := 1:.N, by = .(symbol, month)]
prices[, day_of_month := as.factor(day_of_month)]

# Remove missing values and select columns we need
prices = na.omit(prices, cols = c("symbol", "return_day3", "return_week2", "day_of_month"))

# Structure of dates
prices[, .N, by = day_of_month][order(day_of_month)]
prices[day_of_month == 23, day_of_month := 22] # 23 day to 22 day
prices[day_of_month == 22, day_of_month := 21] # not sure about this but lets fo with it

# Save every symbol separately
prices_dir = file.path(PATH_DATASET, "prices")
if (!dir.exists(prices_dir)) {
  dir.create(prices_dir)
}
for (s in prices[, unique(symbol)]) {
  # s = "aapl"
  if (s %in% c("prn", "prn.1")) next
  prices_ = prices[symbol == s]
  if (nrow(prices_) == 0) next
  file_name = file.path(prices_dir, paste0(s, ".csv"))
  fwrite(prices_, file_name)
}

# Create sh file for predictors
cont = sprintf(
  "#!/bin/bash

#PBS -N Season
#PBS -l ncpus=1
#PBS -l mem=4GB
#PBS -J 1-%d
#PBS -o logs
#PBS -j oe

cd ${PBS_O_WORKDIR}

apptainer run image_season.sif padobran_estimate.R",
  length(list.files(prices_dir)))
writeLines(cont, "padobran_estimate.sh")

# Add to padobran
# scp -r /home/sn/data/strategies/pead/prices padobran:/home/jmaric/peadml/prices
