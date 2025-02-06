library(data.table)
library(glue)
library(lubridate)
library(finutils)
library(qlcal)
library(AzureStor)
library(PerformanceAnalytics)


# Set calendar
calendars
setCalendar("UnitedStates/NYSE")

# Import meadin regression results
rq_dt = fread("F:/strategies/seasonality/seasonality_return_week.csv")

# Portfolio 1:
# 1) p has to be lower than 0.02
# 2) keep min Pr for every symbol
# 3) keep top x for every month
portfolio1 = rq_dt[`Pr(>|t|)` < 0.02]
portfolio1 = portfolio1[rn != "(Intercept)"]
if  ("yearmonthid" %in% colnames(portfolio1)) {
  setnames(portfolio1, "yearmonthid", "month")
}
portfolio1[, minp := min(`Pr(>|t|)`) == `Pr(>|t|)`, by = .(month, symbol)]
portfolio1 = portfolio1[minp == TRUE]
setorderv(portfolio1, c("month", "Pr(>|t|)"))
portfolio1 = portfolio1[, head(.SD, 5), by = month]
# portfolio1 = portfolio1[, head(.SD, 50), by = month]
portfolio1 = unique(portfolio1, by = c("month", "symbol")) # remove duplicated symbols in month
# TODO: move this code above
portfolio1[round(month, 2) == 2024.83]

# Portfolio 2:
# 1) p has to be greater than 0.02
# 2)
portfolio2 = rq_dt[`Pr(>|t|)` < 0.02]
portfolio2 = portfolio2[rn != "(Intercept)"]
portfolio2[, rnn := gsub("day_of_month", "", rn)]
setorder(portfolio2, symbol, rnn, date)
portfolio2[, pr_roll := frollapply(`Pr(>|t|)`, 4, function(x) all(x < 0.0001)), by = symbol]
portfolio2 = na.omit(portfolio2)
portfolio2 = portfolio2[pr_roll == 1]
if  ("yearmonthid" %in% colnames(portfolio2)) {
  setnames(portfolio2, "yearmonthid", "month")
}
portfolio2 = unique(portfolio2, by = c("month", "symbol")) # remove duplicated symbols in month
portfolio2[month == 2024.5]

# Clean portfolios
portfolio_prepare = function(portfolio) {
  # portfolio = copy(portfolio1)
  portfolio[, rn := gsub("day_of_month", "", rn)]
  date_ = portfolio[, ceiling_date(date, unit = "month")]
  seq_dates = lapply(date_, function(x) getBusinessDays(x, x %m+% months(1) - 1))
  dates = mapply(function(x, y) x[y], x = seq_dates, y = portfolio[, as.integer(rn)])
  portfolio[, dates_trading := as.Date(dates, origin = "1970-01-01")]

  # date_ = portfolio[, as.Date(paste0(gsub("month", "", date), "01"), format = "%y%m%d")]
  # seq_dates = lapply(date_, function(x) getBusinessDays(x, x %m+% months(1) - 1))
  # dates = mapply(function(x, y) x[y], x = seq_dates, y = portfolio[, as.integer(rn)])
  # portfolio[, dates_trading := as.Date(dates, origin = "1970-01-01")]
  # portfolio
}
portfolio1 = portfolio_prepare(portfolio1)
portfolio2 = portfolio_prepare(portfolio2)

# save to Azure for backtesting
save_qc = function(portfolio, file_name) {
  portfoliosqc = portfolio[, .(dates_trading, symbol, rn, Value)]
  setorder(portfoliosqc, dates_trading)
  portfoliosqc = na.omit(portfoliosqc)
  setnames(portfoliosqc, "dates_trading", "date")
  portfoliosqc = portfoliosqc[, .(symbol = paste0(symbol, collapse = "|"),
                                  rn     = paste0(rn, collapse = "|"),
                                  value  = paste0(Value, collapse = "|")),
                              by = date]
  portfoliosqc[, date := as.character(date)]
  portfoliosqc = na.omit(portfoliosqc)
  blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
  endpoint = "https://snpmarketdata.blob.core.windows.net/"
  BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
  cont = storage_container(BLOBENDPOINT, "qc-backtest")
  storage_write_csv(portfoliosqc, cont, file_name)
}
save_qc(portfolio1, "seasons-portfolio1.csv")
save_qc(portfolio2, "seasons-portfolio2.csv")


# BACKTET LOCALLY ---------------------------------------------------------
# Choose portfolio
portfolio_ = copy(portfolio1)

# Merge price data to
ohlcv = finutils::qc_daily(
  file_path = "F:/lean/data/stocks_daily.csv",
  symbols = portfolio_[, unique(symbol)],
  min_obs = 252*3,
  price_threshold = 0.00001
)
ohlcv[, month := data.table::yearmon(date)]
setorder(ohlcv, symbol, date)
ohlcv[, day_of_month := 1:.N, by = .(symbol, month)]
ohlcv[, day_of_month := as.factor(day_of_month)]
ohlcv[, returns := close / shift(close) - 1, by = symbol]
ohlcv[, c("high", "low", "month") := NULL]

# Merge price data to portfolio
back = merge(ohlcv, portfolio_,
             by.x = c("symbol", "date"),
             by.y = c("symbol", "dates_trading"),
             all.x = TRUE, all.y = FALSE)
back[!is.na(rn)]
back[!is.na(rn), Value]

# Generate signals
setorder(back, symbol, date)
back[, signal := 0]
indecies_long = back[, which(!is.na(Value) & Value >= 0)]
back[sort(c(outer(indecies_long, 1:3, FUN = "+"))), signal := 1]
indecies_short = back[, which(!is.na(Value) & Value < 0)]
back[sort(c(outer(indecies_short, 1:3, FUN = "+"))), signal := -1]
back[sort(c(indecies_long, indecies_short,
            sort(c(outer(indecies_long, 1:3, FUN = "+"))),
            sort(c(outer(indecies_short, 1:3, FUN = "+")))
            ))]
back[signal == 1]
back[signal == -1]

# Backtest
back = back[signal %in% c(1, -1), .(date, signal, returns)]
back[, weight := 1 / .N, by = date]
setorder(back, date)
back[, ret := signal * returns]
portfolio = back[, .(portfolio_ret = sum(ret * weight)), by = date]
setorder(portfolio, date)
portfolio[, portfolio_ret := portfolio_ret - 0.0001]
charts.PerformanceSummary(as.xts.data.table(portfolio))
charts.PerformanceSummary(as.xts.data.table(portfolio)["2020-01-01/2024-01-01"])
Return.cumulative(as.xts.data.table(portfolio))
Return.cumulative(as.xts.data.table(portfolio)["2020-01-01/"])

# Inspect difference between local and QC
back[symbol == "phk"]
back[sort(c(indecies_short, outer(indecies_short, 1:3, FUN = "+")))][symbol == "phk"]
back[sort(c(indecies_short, outer(indecies_short, 1:3, FUN = "+")))] |>
  _[symbol == "phk"] |>
  _[date > as.Date("2020-01-01")]
portfolio1[symbol == "phk"][date > as.Date("2019-12-01")]



ohlcv = finutils::qc_daily(
  file_path = "F:/lean/data/stocks_daily.csv",
  symbols = portfolio_[, unique(symbol)],
  min_obs = 252*3,
  price_threshold = 0.00001
)



file_path = "F:/lean/data/stocks_daily.csv"
symbols = portfolio_[, unique(symbol)]
min_obs = 253
price_threshold = 1e-8
market_symbol = NULL

# Load data
prices = fread(file_path)
setnames(prices, gsub(" ", "_", tolower(colnames(prices))))

# Set keys
setkey(prices, "symbol")
setorder(prices, symbol, date)

# Filter symbols
if (!is.null(symbols)) {
  prices = prices[.(symbols), nomatch = NULL]
  setkey(prices, "symbol")
}

# Remove duplicates
prices = unique(prices, by = c("symbol", "date"))

# Handle duplicate symbols (e.g., PHUN and PHUN.1)
dups = prices[, .(symbol, n = .N),
              by = .(date, open, high, low, close, volume, adj_close, symbol_first = substr(symbol, 1, 1))]
dups = dups[n > 1]
dups[, symbol_short := gsub("\\.\\d$", "", symbol)]
symbols_remove = dups[, .(symbol, n = .N),
                      by = .(date, open, high, low, close, volume, adj_close, symbol_short)]
symbols_remove = symbols_remove[n >= 2, unique(symbol)]
symbols_remove = symbols_remove[grepl("\\.", symbols_remove)]
if (length(symbols_remove) > 0) {
  prices = prices[!.(symbols_remove)]
}

# Adjust prices for splits/dividends
prices[, adj_rate := adj_close / close]
prices[, `:=`(
  open = open * adj_rate,
  high = high * adj_rate,
  low = low * adj_rate
)]
setnames(prices, "close", "close_raw")
setnames(prices, "adj_close", "close")
prices[, adj_rate := NULL]
setcolorder(prices, c("symbol", "date", "open", "high", "low", "close", "volume"))

# Optionally remove rows with low prices
if (!is.null(price_threshold)) {
  prices = prices[open > price_threshold & high > price_threshold &
                    low > price_threshold & close > price_threshold]
}

# Calculate returns
prices[, returns := close / shift(close, 1) - 1, by = symbol]

# Remove missing values
prices = na.omit(prices)

# Set market returns
if (!is.null(market_symbol)) {
  spy_ret = na.omit(prices[symbol == market_symbol, .(date, market_ret = returns)])
  prices = spy_ret[prices, on = "date"]
  setkey(prices, symbol)
  setorder(prices, symbol, date)
}

# Remove symbols with insufficient data
remove_symbols = prices[, .N, by = symbol][N < min_obs, symbol]
if (length(remove_symbols) > 0) {
  prices = prices[!.(remove_symbols)]
}

key(prices)
