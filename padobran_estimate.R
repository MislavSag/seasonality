suppressMessages(library(data.table))
suppressMessages(library(runner))
suppressMessages(library(quantreg))


# paths
PATH_SAVE = file.path("./median_reg_results")

# Create directory if it doesnt exists
if (!dir.exists(PATH_SAVE)) dir.create(PATH_SAVE)

# Get index
i = as.integer(Sys.getenv('PBS_ARRAY_INDEX'))
# i = 1

# Get symbol
if (interactive()) {
  symbols = gsub("\\.csv", "", list.files(file.path("D:/strategies/season", "prices")))
} else {
  symbols = gsub("\\.csv", "", list.files("prices"))
}
symbol_i = symbols[i]

# Get data
if (interactive()) {
  prices = fread(file.path("D:/strategies/season", "prices", paste0(symbol_i, ".csv")))
} else {
  prices = fread(file.path("prices", paste0(symbol_i, ".csv")))
}

# Prepare data for median regression
prices[, day_of_month := as.factor(day_of_month)]

# Create future returns
prices[, lead_1 := shift(close, 1, type = "lead") / close - 1, by = symbol]
prices[, lead_2 := shift(close, 2, type = "lead") / shift(close, 1, type = "lead") - 1, by = symbol]
prices[, lead_3 := shift(close, 3, type = "lead") / shift(close, 2, type = "lead") - 1, by = symbol]
prices[, lead_4 := shift(close, 3, type = "lead") / shift(close, 2, type = "lead") - 1, by = symbol]
prices[, lead_5 := shift(close, 3, type = "lead") / shift(close, 2, type = "lead") - 1, by = symbol]

# Function to calcualte median regression
get_coeffs = function(df, y = "return_week") {
  # df = prices[1:300]
  res = rq(as.formula(paste0(y, " ~ day_of_month")), data = as.data.frame(df))
  summary_fit = summary.rq(res, se = 'nid')
  res_q = as.data.table(summary_fit$coefficients, keep.rownames = TRUE)
  setnames(res_q, c("rn", "estimate", "std_error", "t_value", "p"))
  res_q = cbind(model = "quantile", res_q)
  res_lm = lm(as.formula(paste0(y, " ~ day_of_month")), data = as.data.frame(df))
  summary_fit_lm = summary(res_lm)
  res_lm = as.data.table(summary_fit_lm$coefficients, keep.rownames = TRUE)
  setnames(res_lm, c("rn", "estimate", "std_error", "t_value", "p"))
  res_lm = cbind(model = "lm", res_lm)
  res = rbind(res_lm, res_q)
  res = res[p < 0.1]
  if (nrow(res) == 0) {
    return(NULL)
  } else {
    res = cbind(date = data.table::last(df[, date]),
                symbol = data.table::last(df[, symbol]),
                day_of_month = data.table::last(df[, day_of_month]),
                target = data.table::last(df[, get(y)]),
                lead_1 = data.table::last(df[, lead_1]),
                lead_2 = data.table::last(df[, lead_2]),
                lead_3 = data.table::last(df[, lead_3]),
                lead_4 = data.table::last(df[, lead_4]),
                lead_5 = data.table::last(df[, lead_5]),
                res)
    return(res)
  }
}

# Calculate median regression by day_of_month
res = runner(
  x = prices[1:300],
  f = function(x) {
    if (nrow(x) < (252)) return(NULL)
    tryCatch(get_coeffs(x), error = function(e) NULL)
  },
  lag = 0L,
  na_pad = TRUE
)
res = rbindlist(res)

# Save
file_name = file.path(PATH_SAVE, paste0(symbol_i, ".csv"))
fwrite(res, file_name)

