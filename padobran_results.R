library(fastverse)
library(finutils)
library(PerformanceAnalytics)
library(lubridate)


# DATA --------------------------------------------------------------------
# Import prices
prices = qc_daily(
  file_path = "F:/lean/data/stocks_daily.csv",
  min_obs = 252,
  price_threshold = 0.00001,
  add_dv_rank = TRUE
)
prices[, month := data.table::yearmon(date)]
setorder(prices, symbol, date)
prices[, day_of_month := 1:.N, by = .(symbol, month)]

# Create future returns
prices[, lead_1 := shift(close, 1, type = "lead") / close - 1, by = symbol]
prices[, lead_2 := shift(close, 2, type = "lead") / shift(close, 1, type = "lead") - 1, by = symbol]
prices[, lead_3 := shift(close, 3, type = "lead") / shift(close, 2, type = "lead") - 1, by = symbol]

# Import models results from padobran
files = list.files("D:/strategies/season/results", full.names = TRUE)
head(files)
results = lapply(files[1:500], fread)
results = rbindlist(results)
results[rn == "(Intercept)", day_of_month := 1L]
results[is.na(day_of_month), day_of_month := as.integer(gsub("day_of_month", "", rn))]
results[, rn := NULL]
results[, month := data.table::yearmon(date)]
results = dcast(results, symbol + date + month + day_of_month ~ model,
                value.var = c("p", "estimate"))
months_days = unique(prices[, .(month, day_of_month, date)], by = c("month", "day_of_month", "date"))
setorder(months_days, month, day_of_month)
results[, months_to_trade := fifelse(month != as.integer(month), round(1 / 12 + month, 3), month)]
# results[, date_trade := ]

# Test individual symbols
s = "fsci"
r = results[symbol == s]
r = dcast(r, date + month + day_of_month ~ model, value.var = c("p", "estimate"))
p = prices[symbol == s]
o = r[p, on = c("date", "month", "day_of_month")]
o[!is.na(lm) | !is.na(quantile)]

# Portfolio 1
paste0(round(nrow(results[p_quantile < 0.01 & p_lm < 0.01]) / nrow(results) * 100, 2), "%")
portfolio1 = results[p_quantile < 0.01 & p_lm < 0.01] |>
  _[, .SD[which.min(p_lm + p_quantile)], by = .(month, day_of_month)] |>
  _[month >= 2000] |>
  _[prices, on = c("symbol", "month", "day_of_month"), nomatch = 0] |>
  _[order(month, day_of_month)]
portfolio1[month %between% c(2000, 2000.1)]
back = portfolio1[, .(symbol = shift(symbol), ), by = .(day_of_month)]
back[month %between% c(2000, 2000.1)]

# [
#   prices, on = c("symbol", "date", "month", "day_of_month")] |>
#   _[!is.na(p_lm) & !is.na(quantile)]

# portfolio1 = results[p_quantile < 0.01 & p_lm < 0.01][
#   prices, on = c("symbol", "date", "month", "day_of_month")] |>
#   _[!is.na(p_lm) & !is.na(quantile)]



# Portfolio 1
portfolio1 = results[, .(p = sum(p, na.rm = TRUE), e = sum(sign(estimate))), by = .(symbol, month, day_of_month)]


results[symbol == "fsci"][month == 2013.500]

# Portfolio 1
# Select best for every date
portfolio1 = results[, .(p = sum(p, na.rm = TRUE), e = sum(sign(estimate))), by = .(data.table::month(date)), n)]
portfolio1 = portfolio1[, rank := frank(p), by = date]
portfolio1 = portfolio1[rank == 1]
# portfolio1 = portfolio1[p < 0.01 & e > 0.05]

setorder(portfolio1, date)
back = prices[portfolio1, on = .(symbol, date), nomatch = 0]
# back = back[dollar_vol_rank < 2000]
charts.PerformanceSummary(as.xts.data.table(back[, .(date, fifelse(e > 0, lead_1, -lead_1))]))
charts.PerformanceSummary(as.xts.data.table(back[, .(date, fifelse(e > 0, lead_2, -lead_2))]))
charts.PerformanceSummary(as.xts.data.table(back[, .(date, fifelse(e > 0, lead_3, -lead_3))]))


portfolio2 = results[model == "quantile", .SD[which.min(p)], by = .(date)]
setorder(portfolio2, date)
back = prices[portfolio2, on = .(symbol, date), nomatch = 0]
# back = back[dollar_vol_rank < 2000]
charts.PerformanceSummary(as.xts.data.table(back[, .(date, lead_1)]))
charts.PerformanceSummary(as.xts.data.table(back[, .(date, lead_2)]))
charts.PerformanceSummary(as.xts.data.table(back[, .(date, lead_3)]))

portfolio3 = results
