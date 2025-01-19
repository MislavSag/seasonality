library(data.table)
library(glue)
library(lubridate)


# Set calendar
calendars
setCalendar("UnitedStates/NYSE")

# Import meadin regression results
rq_dt = fread(glue("F:/strategies/seasonality/seasonality_return_week.csv"))

# Portfolio 1:
# 1) p has to be greater than 0.02
# 2) keep min Pr for every symbol
# 3) keep top x for every month
portfolio1 = rq_dt[`Pr(>|t|)` < 0.02]
portfolio1 = portfolio1[rn != "(Intercept)"]
portfolio1[, minp := min(`Pr(>|t|)`) == `Pr(>|t|)`, by = .(yearmonthid, symbol)]
portfolio1 = portfolio1[minp == TRUE]
setorderv(portfolio1, c("yearmonthid", "Pr(>|t|)"))
portfolio1 = portfolio1[, head(.SD, 5), by = yearmonthid]
portfolio1 = unique(portfolio1, by = c("yearmonthid", "symbol")) # remove duplicated symbols in month
# TODO: move this code above
portfolio1[yearmonthid == 2024.5]

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
portfolio2 = unique(portfolio2, by = c("yearmonthid", "symbol")) # remove duplicated symbols in month
portfolio2[yearmonthid == 2024.5]

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
