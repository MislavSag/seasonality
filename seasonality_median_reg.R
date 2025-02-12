library(data.table)
library(quantreg)
library(qlcal)
library(lubridate)
library(AzureStor)
library(runner)
library(glue)
library(roll)
library(PerformanceAnalytics)
library(finutils)


# SET UP ------------------------------------------------------------------
# global vars
PATH = "F:/data/equity/us"

# Set calendar
calendars
setCalendar("UnitedStates/NYSE")


# PRICE DATA --------------------------------------------------------------
# Import daily data
prices = qc_daily(
  file_path = "F:/lean/data/stocks_daily.csv",
  min_obs = 2 * 252,
  price_threshold = 1e-008,
  add_dv_rank = TRUE
)
key(prices)

# save SPY for later and keep only events symbols
spy = prices["spy"]

# free memory
gc()


# PREPARE DATA FOR SEASONALITY ANALYSIS -----------------------------------
# Remove outliers
nrow(prices[returns > 1]) / nrow(prices)
prices = prices[returns < 1] # TODO:: better outlier detection mechanism. For now, remove daily returns above 100%

# define target variables
setorder(prices, symbol, date)
prices[, return_day := shift(close, 1, type = "lead") / close - 1, by = symbol]
prices[, return_day2 := shift(close, 2, type = "lead") / close - 1, by = symbol]
prices[, return_day3 := shift(close, 3, type = "lead") / close - 1, by = symbol]
prices[, return_week := shift(close, 5, type = "lead") / close - 1, by = symbol]
prices[, return_week2 := shift(close, 10, type = "lead") / close - 1, by = symbol]
prices[, std_roll := roll_sd(returns, 22*6), by = symbol]
# TODO: standardized returns
prices[, return_day_std := return_day / std_roll]
prices[, return_day2_std := return_day2 / std_roll]
prices[, return_day3_std := return_day3 / std_roll]
prices[, return_week_std := return_week / std_roll]
prices[, return_week2_std := return_week2 / std_roll]

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

# Remove symbols with less than 750 observations (3 years of data)
symbols_keep = prices[, .N, by = symbol]
symbols_keep = symbols_keep[N >= 750, symbol]
prices = prices[symbol %in% symbols_keep]


# SEASONALITY COARSE ------------------------------------------------------
# To decrease sample size I will exclude observations where winning rate is
# lower than 60% for both long and short
targets = c("return_day", "return_day2", "return_day3", "return_week", "return_week2")
cols_wr = paste0(targets, "_wr")
prices[, (cols_wr) := lapply(targets, function(x) {
  runner(
    x = get(x),
    f = function(x) {
      if (length(x) < 12*10) {
        return(NA)
      } else{
        y_ = sum(x > 0) / length(x)
        if (y_ > 0.7 && mean(x) > 0 && median(x) > 0) {
          return(1L)
        } else if (y_ < 0.3 && mean(x) < 0 && median(x) < 0) {
          return(-1L)
        } else {
          return(0L)
        }
      }
    },
    # k = nrow(.BY),
    na_pad = TRUE
  )
}),
by = .(symbol, day_of_month)]
round(nrow(prices[return_day_wr %in% c(-1L, 1L)]) / nrow(prices) * 10, 3)
round(nrow(prices[return_day2_wr %in% c(-1L, 1L)]) / nrow(prices) * 10, 3)
round(nrow(prices[return_day3_wr %in% c(-1L, 1L)]) / nrow(prices) * 10, 3)
round(nrow(prices[return_week_wr %in% c(-1L, 1L)]) / nrow(prices) * 10, 3)
round(nrow(prices[return_week2_wr %in% c(-1L, 1L)]) / nrow(prices) * 10, 3)

# Recalculate winning rates
cols_wr_signal = paste0(cols_wr, "_signal")
prices[, (cols_wr_signal) := 0]
prices[, (cols_wr_signal) := lapply(cols_wr, function(x) {
  fifelse(shift(get(x)) == 1, 1, 0)
}), by = .(symbol, day_of_month)]
prices[return_day_wr_signal == 1]
prices[return_week_wr_signal == 1]; prices[return_week_wr == 1]
cols_wr_signal_short = paste0(cols_wr, "_signal_short")
prices[, (cols_wr_signal_short) := 0]
prices[, (cols_wr_signal_short) := lapply(cols_wr, function(x) {
  fifelse(shift(get(x)) == -1, -1, 0)
}), by = .(symbol, day_of_month)]
prices[, return_day_wr_signal := ifelse(return_day_wr_signal == 0 & return_day_wr_signal_short == -1, -1, return_day_wr_signal)]
prices[, return_day2_wr_signal := ifelse(return_day2_wr_signal == 0 & return_day2_wr_signal_short == -1, -1, return_day2_wr_signal)]
prices[, return_day3_wr_signal := ifelse(return_day3_wr_signal == 0 & return_day3_wr_signal_short == -1, -1, return_day3_wr_signal)]
prices[, return_week_wr_signal := ifelse(return_week_wr_signal == 0 & return_week_wr_signal_short == -1, -1, return_week_wr_signal)]
prices[, return_week2_wr_signal := ifelse(return_week2_wr_signal == 0 & return_week2_wr_signal_short == -1, -1, return_week2_wr_signal)]
prices[return_day_wr_signal == 1]; prices[return_day_wr == 1]
prices[return_day_wr_signal == -1]; prices[return_day_wr == -1]
prices[, (cols_wr_signal_short) := NULL]
setorder(prices, symbol, date)

# Add to QC
signal_ = "return_day2_wr"
qc_data = prices[close_raw > 5 & x %in% c(1, -1), .(date, symbol, day_of_month, signal = x), env = list(x = signal_)]
setorder(qc_data, date)
qc_data = na.omit(qc_data)
qc_data = qc_data[, .(
  symbol = paste0(symbol, collapse = "|"),
  signal = paste0(signal, collapse = "|")
), by = date]
qc_data[, date := as.character(date)]
qc_data[, date := paste0(date, " 09:31:00")]
blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
cont = storage_container(BLOBENDPOINT, "qc-backtest")
storage_write_csv(qc_data, cont, "seasonality_simple_week.csv")

# Compare QC and local
qc_data[ date %between% c("2023-01-01", "2023-01-03")]

# Set signals for backtest
setorder(prices, symbol, date)
prices[shift(return_day_wr_signal) == 1, return_day_signal := 1, by = symbol]
prices[shift(return_day_wr_signal) == -1, return_day_wr_signal := -1, by = symbol]
prices[shift(return_day2_wr_signal) == 1 | shift(return_day2_wr_signal, 2) == 1,
       return_day2_wr_signal := 1, by = symbol]
prices[shift(return_day2_wr_signal) == -1 | shift(return_day2_wr_signal, 2) == -1,
       return_day2_wr_signal := -1, by = symbol]
prices[shift(return_day3_wr_signal) == 1 | shift(return_day3_wr_signal, 2) == 1 | shift(return_day3_wr_signal, 3) == 1,
       return_day3_wr_signal := 1, by = symbol]
prices[shift(return_day3_wr_signal) == -1 | shift(return_day3_wr_signal, 2) == -1 | shift(return_day3_wr_signal, 3) == -1,
       return_day3_wr_signal := -1, by = symbol]
prices[shift(return_week_wr_signal) == 1 | shift(return_week_wr_signal, 2) == 1 |
         shift(return_week_wr_signal, 3) == 1 | shift(return_week_wr_signal, 4) == 1 |
         shift(return_week_wr_signal, 5) == 1,
       return_week_wr_signal := 1, by = symbol]
prices[shift(return_week_wr_signal) == -1 | shift(return_week_wr_signal, 2) == -1 |
         shift(return_week_wr_signal, 3) == -1 | shift(return_week_wr_signal, 4) == -1 |
         shift(return_week_wr_signal, 5) == -1,
       return_week_wr_signal := -1, by = symbol]
prices[shift(return_week2_wr_signal) == 1 | shift(return_week2_wr_signal, 2) == 1 |
         shift(return_week2_wr_signal, 3) == 1 | shift(return_week2_wr_signal, 4) == 1 |
         shift(return_week2_wr_signal, 5) == 1 | shift(return_week2_wr_signal, 6) == 1 |
         shift(return_week2_wr_signal, 7) == 1 | shift(return_week2_wr_signal, 8) == 1 |
         shift(return_week2_wr_signal, 9) == 1 | shift(return_week2_wr_signal, 10) == 1,
       return_week2_wr_signal := 1, by = symbol]
prices[shift(return_week2_wr_signal) == -1 | shift(return_week2_wr_signal, 2) == -1 |
         shift(return_week2_wr_signal, 3) == -1 | shift(return_week2_wr_signal, 4) == -1 |
         shift(return_week2_wr_signal, 5) == -1 | shift(return_week2_wr_signal, 6) == -1 |
         shift(return_week2_wr_signal, 7) == -1 | shift(return_week2_wr_signal, 8) == -1 |
         shift(return_week2_wr_signal, 9) == -1 | shift(return_week2_wr_signal, 10) == -1,
       return_week2_wr_signal := -1, by = symbol]
tail(prices[return_day_wr_signal == 1, .(symbol, date, return_day_wr_signal)], 10)
tail(prices[return_day2_wr_signal == 1, .(symbol, date, return_day_wr_signal)], 20)
tail(prices[return_day3_wr_signal == 1, .(symbol, date, return_day3_wr_signal)], 20)
tail(prices[return_week_wr_signal == 1, .(symbol, date, return_week_wr_signal)], 20)

# Backtests
backtest = function(signal, performance = TRUE, cost = 0.001) {
  # signal = "return_week_wr_signal"
  back = prices[close_raw > 1 &
                  dollar_vol_rank < 2000 &
                  x %in% c(1, -1), .(date, x, return_day),
                env = list(x = signal)]
  back[, weight := 1 / .N, by = date]
  setorder(back, date)
  back[, ret := x * return_day, env = list(x = signal)]
  portfolio = back[, .(portfolio_ret = sum(ret  * weight) - cost), by = date]
  portfolio = portfolio[portfolio_ret > -1]
  portfolio = portfolio[date > as.Date("2007-01-01")]
  if (performance) {
    return(SharpeRatio.annualized(as.xts.data.table(portfolio))[1, ])
  }
  return(as.xts.data.table(portfolio))
}
back_day   = backtest("return_day_wr_signal")
back_day2  = backtest("return_day2_wr_signal")
back_day3  = backtest("return_day3_wr_signal")
back_week  = backtest("return_week_wr_signal")
back_week2 = backtest("return_week2_wr_signal")
data.table(
  back_day = back_day,
  back_day2 = back_day2,
  back_day3 = back_day3,
  back_week = back_week,
  back_week2 = back_week2
)
charts.PerformanceSummary(backtest("return_week_wr_signal", FALSE))
charts.PerformanceSummary(backtest("return_day2_wr_signal", FALSE))
charts.PerformanceSummary(backtest("return_week_wr_signal", FALSE)["2023"])

# Compare QC and local
qc_data[date %between% c(as.Date("2023-01-01"), as.Date("2023-01-03"))]
cols_wr_signal = paste0(cols_wr, "_signal")
prices[, (cols_wr_signal) := 0]
prices[, (cols_wr_signal) := lapply(cols_wr, function(x) {
  fifelse(shift(get(x)) > 0.7, 1, 0)
}), by = .(symbol, day_of_month)]
prices[return_day_wr_signal == 1]
prices[return_week_wr_signal == 1]
cols_wr_signal_short = paste0(cols_wr, "_signal_short")
prices[, (cols_wr_signal_short) := 0]
prices[, (cols_wr_signal_short) := lapply(cols_wr, function(x) {
  fifelse(shift(get(x)) < 0.3, -1, 0)
}), by = .(symbol, day_of_month)]
prices[, return_day_wr_signal := ifelse(return_day_wr_signal == 0 & return_day_wr_signal_short == -1, -1, return_day_wr_signal)]
prices[, return_day2_wr_signal := ifelse(return_day2_wr_signal == 0 & return_day2_wr_signal_short == -1, -1, return_day2_wr_signal)]
prices[, return_day3_wr_signal := ifelse(return_day3_wr_signal == 0 & return_day3_wr_signal_short == -1, -1, return_day3_wr_signal)]
prices[, return_week_wr_signal := ifelse(return_week_wr_signal == 0 & return_week_wr_signal_short == -1, -1, return_week_wr_signal)]
prices[, return_week2_wr_signal := ifelse(return_week2_wr_signal == 0 & return_week2_wr_signal_short == -1, -1, return_week2_wr_signal)]
prices[return_day_wr_signal == 1]
prices[return_day_wr_signal == -1]
prices[, (cols_wr_signal_short) := NULL]
signal_ = "return_week_wr_signal"
back = prices[x %in% c(1, -1), .(date, symbol, x, return_day),
              env = list(x = signal_)]
back[, weight := 1 / .N, by = date]
setorder(back, date)
back[date > as.Date("2023-01-01") & date < as.Date("2023-01-04")]


# SEASONALITY MINING ------------------------------------------------------
# Recalculate winning rates
cols_wr_signal = paste0(cols_wr, "_signal")
prices[, (cols_wr_signal) := 0]
prices[, (cols_wr_signal) := lapply(cols_wr, function(x) {
  fifelse(shift(get(x)) == 1, 1, 0)
}), by = .(symbol, day_of_month)]
prices[return_day_wr_signal == 1]
prices[return_week_wr_signal == 1]; prices[return_week_wr == 1]
cols_wr_signal_short = paste0(cols_wr, "_signal_short")
prices[, (cols_wr_signal_short) := 0]
prices[, (cols_wr_signal_short) := lapply(cols_wr, function(x) {
  fifelse(shift(get(x)) == -1, -1, 0)
}), by = .(symbol, day_of_month)]
prices[, return_day_wr_signal := ifelse(return_day_wr_signal == 0 & return_day_wr_signal_short == -1, -1, return_day_wr_signal)]
prices[, return_day2_wr_signal := ifelse(return_day2_wr_signal == 0 & return_day2_wr_signal_short == -1, -1, return_day2_wr_signal)]
prices[, return_day3_wr_signal := ifelse(return_day3_wr_signal == 0 & return_day3_wr_signal_short == -1, -1, return_day3_wr_signal)]
prices[, return_week_wr_signal := ifelse(return_week_wr_signal == 0 & return_week_wr_signal_short == -1, -1, return_week_wr_signal)]
prices[, return_week2_wr_signal := ifelse(return_week2_wr_signal == 0 & return_week2_wr_signal_short == -1, -1, return_week2_wr_signal)]
prices[return_day_wr_signal == 1]; prices[return_day_wr == 1]
prices[return_day_wr_signal == -1]; prices[return_day_wr == -1]
prices[, (cols_wr_signal_short) := NULL]
setorder(prices, symbol, date)

# Get coeffs from summary of quantile regression
get_coeffs = function(df, y = "return_week") {
  res = rq(as.formula(paste0(y, " ~ day_of_month")), data = as.data.frame(df))
  summary_fit = summary.rq(res, se = 'nid')
  as.data.table(summary_fit$coefficients, keep.rownames = TRUE)
}

# I we ignore above and choose best from above analysis
ind = prices[, which(!is.na(return_week_wr_signal) & return_week_wr_signal != 0)]
length(ind)
prices[ind][date %between% c(as.Date("2023-01-01"), as.Date("2023-01-03"))]
res = runner(
  x = prices,
  f = function(x) {
    x = x[symbol == x[, last(symbol)]]
    if (nrow(x) < (22*12)) return(NA)
    x = last(x, 2520)
    res_ = tryCatch(get_coeffs(x), error = function(e) NULL)
    return(res_)
  },
  lag = 0L,
  k = as.integer((Sys.Date() - as.Date("1998-01-01"))) + 1,
  at = ind, # ind_sample for p
  na_pad = TRUE
)

# Inspect
length(ind)
length(res)
sum(!is.na(res))
sum(!is.na(res)) / length(res)
head(res)
tail(res)

# Merge results and dt table
results = copy(prices)
results
results[, rq_res := list(as.list(NA))]
results[ind, rq_res := res]
head(results)
tail(results)
cols_keep = c("symbol", "date", "month", "day_of_month", "rq_res",
              "dollar_vol_rank", "close_raw", "return_week_wr_signal")
results = results[!is.na(rq_res), ..cols_keep]

# Unnest results
results = results[, rbindlist(rq_res), by = setdiff(cols_keep, "rq_res")]

# Inspect
results[date %between% c(as.Date("2023-01-01"), as.Date("2023-01-03"))]
prices[ind][date %between% c(as.Date("2023-01-01"), as.Date("2023-01-03"))]
results[symbol == "aar"]

# fwrite
fwrite(results, "F:/strategies/seasonality/seasonality_return_week.csv")


# WR + REG ----------------------------------------------------------------
# Keep only rows where rn is equal to day_of_month
results[rn == "(Intercept)", rnn := 1]
results[is.na(rnn), rnn := stringr::str_extract(rn, "[0-9]+")]
results[, rnn := as.integer(rnn)]

# Set signals
signals = results[as.integer(day_of_month) == rnn]
signals = signals[sign(Value) == return_week_wr_signal]
setnames(signals, "Pr(>|t|)", "p")
signals = signals[p < 0.001]

# Merge prices and signals
prices = signals[, .(symbol, date, signal_reg = return_week_wr_signal)][
  prices, on = c("symbol", "date")]

# Set signals for backtest
setorder(prices, symbol, date)
prices[shift(signal_reg) == 1 | shift(signal_reg, 2) == 1 |
         shift(signal_reg, 3) == 1 | shift(signal_reg, 4) == 1,
       signal_reg := 1, by = symbol]
prices[shift(signal_reg) == -1 | shift(signal_reg, 2) == -1 |
         shift(signal_reg, 3) == -1 | shift(signal_reg, 4) == -1,
       signal_reg := -1, by = symbol]
tail(prices[signal_reg == 1, .(symbol, date, signal_reg)], 20)
tail(prices[signal_reg == -1, .(symbol, date, signal_reg)], 20)

# Backtest
# close_raw > 1 &
#   dollar_vol_rank < 2000 &
# back = prices[signal_reg %in% c(1, -1), .(date, signal_reg, return_day)]
back = prices[dollar_vol_rank < 2000 & signal_reg %in% c(1, -1), .(date, signal_reg, return_day)]
back[, weight := 1 / .N, by = date]
setorder(back, date)
back[, ret := signal_reg * return_day]
portfolio = back[, .(portfolio_ret = sum(ret * weight) - 0.0005), by = date]
portfolio = portfolio[portfolio_ret > -1]
portfolio = portfolio[date > as.Date("2007-01-01")]
portfolio = as.xts.data.table(portfolio)
SharpeRatio.annualized(portfolio)[1, ]
charts.PerformanceSummary(portfolio)


# REG ---------------------------------------------------------------------


