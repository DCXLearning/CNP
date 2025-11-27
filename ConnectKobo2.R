# ---- CSV-backed clean_all_kobo() ----
library(readr)
library(dplyr)
library(tibble)

clean_all_kobo <- function(
    data_dir = "Data",
    strict = TRUE   # if FALSE, missing files return empty tibbles instead of stop()
) {
  csv_path <- function(name) file.path(data_dir, paste0(name, ".csv"))
  read_tbl <- function(name) {
    path <- csv_path(name)
    if (!file.exists(path)) {
      msg <- paste("File not found:", path)
      if (strict) stop(msg) else { message(msg); return(tibble()) }
    }
    readr::read_csv(
      path,
      locale = readr::locale(encoding = "UTF-8"),
      na = c("", "NA", "NaN", "null", "NULL"),
      guess_max = 200000,
      show_col_types = FALSE
    )
  }
  
  # One table = one file
  master7       <- read_tbl("ncdd_form7newmaster")
  form7sub1        <- read_tbl("ncdd_form7newsub1")
  form7sub2        <- read_tbl("ncdd_form7newsub2")
  form7sub3 <- read_tbl("ncdd_form7newsub3")
  form3sub3_1_2_3_4     <- read_tbl("ncdd_form7newsub3_1_2_3_4")
  location <- read_tbl("tbl_location")
  village     <- read_tbl("tbl_locationvillage")
  
  message(
    sprintf("Loaded rows → master7:%s form7sub1:%s form7sub2:%s form7sub3:%s form3sub3_1_2_3_4:%s location:%s village:%s",
            nrow(master7), nrow(form7sub1), nrow(form7sub2), nrow(form7sub3), nrow(form3sub3_1_2_3_4),nrow(location), nrow(village))
  )
  
  # Return in the same shape you already use
  list(
    master7            = master7,
    form7sub1          = form7sub1,
    form7sub2          = form7sub2,
    form7sub3          = form7sub3,
    form3sub3_1_2_3_4  = form3sub3_1_2_3_4,
    location           = location,
    village            = village
  )
}
# ---- end CSV-backed clean_all_kobo() ----
