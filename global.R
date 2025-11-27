
library(shiny)
library(DBI)
library(RMariaDB)
library(dplyr)
library(lubridate)
library(ggplot2)
library(DT)
library(tidyr)
library(scales)
library(openxlsx)
library(tibble)
library(officer)
library(forcats)
library(cowplot)
library(flextable)


source("R/Server.R")
con <- tryCatch(
  dbConnect(
    RMariaDB::MariaDB(),
    user = "root",
    password = "Mymariadb123",
    host = "104.248.155.82",
    port = 3306,
    dbname = "NCDD1"
  ),
  error = function(e) {
    message("Failed to connect to the database: ", e$message)
    NULL
  }
)

`%||%` <- function(a, b) if (is.null(a) || is.na(a) || identical(a, "")) b else a
fetch_user_by_identifier <- function(identifier) {
  
  sql <- "
    SELECT u.*, p.title AS permission_title
    FROM users u
    LEFT JOIN users_permission p ON p.permission_id = u.permission_id
    WHERE (u.name = ? OR u.email = ?)
      AND (u.userstatus IS NULL OR u.userstatus <> 'no')
      AND (u.trash      IS NULL OR u.trash      <> 'yes')
      AND (u.group_id   IN (1,3))
    LIMIT 1
  "
  
  res <- pool::dbGetQuery(con, sql, params = list(identifier, identifier))
  tibble::as_tibble(res)
}


# =========================================================================
# 1. Password verification (Laravel bcrypt)
# =========================================================================

verify_password <- function(plain, stored) {
  
  if (is.null(stored) || stored == "" || is.na(stored)) {
    message("Stored password empty")
    return(FALSE)
  }
  
  stored_fixed <- gsub("^\\$2y\\$", "\\$2a\\$", stored)
  
  message("Stored (fixed) password: ", stored_fixed)
  
  ok <- tryCatch(
    bcrypt::checkpw(plain, stored_fixed),
    error = function(e) {
      message("bcrypt error: ", e$message)
      FALSE
    }
  )
  
  if (!isTRUE(ok)) message("Password mismatch")
  
  return(isTRUE(ok))
}

# =========================================================================
# 2. Credential check (shinymanager)
# =========================================================================

my_check <- function(user, password) {

  rec <- fetch_user_by_identifier(user)
  message("Fetched user record:")
  print(rec)

  if (nrow(rec) != 1) {
    return(list(result = FALSE))
  }

  if (!verify_password(password, rec$password[1])) {
    return(list(result = FALSE))
  }

  # --- MODIFICATION START ---

  user_type_scope <- rec$type[1] %||% "guest"

  # Print the type to the terminal/console
  message(sprintf("Login successful. User type (scope) detected: %s", user_type_scope))

  list(
    result = TRUE,
    # Return the user type as the 'user' identifier for shinymanager
    user   = user_type_scope,
    admin  = FALSE,
    expire = Sys.time() + 8 * 3600
  )
  # --- MODIFICATION END ---
}



location <- dbGetQuery(con, "SELECT * FROM tbl_location")
village <- dbGetQuery(con, "SELECT * FROM tbl_locationvillage")


# province lookup
province_lookup <- location %>%
  select(location_id, title_en, title_kh) %>%
  rename(
    province_id = location_id,
    province_kh = title_kh,
    province_en = title_en

  )

# district lookup
district_lookup <- location %>%
  select(location_id, title_en, title_kh) %>%
  rename(
    district_id = location_id,
    district_kh = title_kh,
    district_en = title_en

  )

# commune lookup
commune_lookup <- location %>%
  select(location_id, title_en, title_kh) %>%
  rename(
    commune_id = location_id,
    commune_kh = title_kh,
    commune_en = title_en

  )

# village lookup
village_lookup <- village %>%
  select(location_id, title_en, title_kh) %>%
  rename(
    village_id = location_id,
    village_kh = title_kh,
    village_en = title_en

  )



# now join everything
master7 <- dbGetQuery(con, "SELECT * FROM ncdd_form7newmaster")
form7sub1 <- dbGetQuery(con, "SELECT * FROM ncdd_form7newsub1") 
form7sub2 <-  dbGetQuery(con, "SELECT * FROM ncdd_form7newsub2") 
form7sub3 <- dbGetQuery(con, "SELECT * FROM ncdd_form7newsub3")
form7sub3_1_2_3_4 <- dbGetQuery(con, "SELECT * FROM ncdd_form7newsub3_1_2_3_4")
master7_with_kh <- master7 %>%
  left_join(province_lookup, by = c("province_id" = "province_id")) %>%
  left_join(district_lookup, by = c("district_id" = "district_id")) %>%
  left_join(commune_lookup,  by = c("commune_id"  = "commune_id")) %>%
  left_join(village_lookup,  by = c("village_id"  = "village_id")) %>%
  mutate(
    start_date = as.Date(start_date),
    end_date   = as.Date(end_date),
    year = lubridate::year(end_date),
    month = lubridate::month(end_date),
    semester   = ifelse(lubridate::month(end_date) <= 6, 1, 2)
  ) %>%
  distinct(province_id, district_id, commune_id, village_id, start_date, end_date, .keep_all = TRUE)

keep_cols <- c(
  "id","parent_id","formkey","indicator_id",
  "total","total_indigenous",
  "unit_id","indicator_title",
  "pregnant","after_pregnant","mother_guardian","children_under2","other",
  "number","total_girl",
  "satisfied","dissatisfied","remark",
  "extra_fix","extra"
)

# 2️⃣ Helper function: standardize columns
std <- function(df) {
  miss <- setdiff(keep_cols, names(df))
  if (length(miss)) df[miss] <- NA                # add missing columns as NA
  dplyr::select(df, dplyr::all_of(keep_cols))     # order columns consistently
}

# 3️⃣ Apply to each dataset and tag source
sub1_long  <- std(form7sub1)         %>% mutate(source = "form7sub1")
sub2_long  <- std(form7sub2)         %>% mutate(source = "form7sub2")
sub3_long  <- std(form7sub3)         %>% mutate(source = "form7sub3")
f3314_long <- std(form7sub3_1_2_3_4) %>% mutate(source = "form3sub3_1_2_3_4")

# 4️⃣ Helper: convert numeric-like columns safely
num_cols <- c(
  "total","total_indigenous","pregnant","after_pregnant","mother_guardian",
  "children_under2","other","number","total_girl","satisfied","dissatisfied"
)

to_num <- function(df) {
  for (nm in intersect(num_cols, names(df))) {
    df[[nm]] <- suppressWarnings(as.numeric(df[[nm]]))
  }
  df
}

# Apply numeric conversion
sub1_long  <- to_num(sub1_long)
sub2_long  <- to_num(sub2_long)
sub3_long  <- to_num(sub3_long)
f3314_long <- to_num(f3314_long)

# 4) Final stacked table (row-bind). Row count = sum of all inputs.
form7_all_long <- bind_rows(sub1_long, sub2_long, sub3_long, f3314_long) %>%
  arrange(parent_id, formkey, indicator_id, source)

# ----------------------- OPTIONAL: add date & location columns -----------------------
#Requires master7 and *_lookup data frames to be created earlier in global.R.
form7_all_with_loc <- form7_all_long %>%
  left_join(
    master7 %>%
      select(id, start_date, end_date, province_id, district_id, commune_id, village_id,trash),
    by = c("parent_id" = "id"),
    relationship = "many-to-many"
  ) %>%
  left_join(province_lookup, by = "province_id") %>%
  left_join(district_lookup, by = "district_id") %>%
  left_join(commune_lookup,  by = "commune_id") %>%
  left_join(village_lookup,  by = "village_id") %>%
  mutate(
    start_date = as.Date(start_date),
    end_date   = as.Date(end_date),
    year       = lubridate::year(end_date),
    month      = lubridate::month(end_date),
    semester   = ifelse(lubridate::month(end_date) <= 6, 1, 2)
  )





form8newmaster <- dbGetQuery(con, "SELECT * FROM ncdd_form8newmaster")


# --- form8newsub1 ---
form8_sub1 <- dbGetQuery(con, "SELECT * FROM ncdd_form8newsub1") %>%
  select(id, formkey, indicator_id, unit_id, indicator_title,
         total, pregnant, after_pregnant, mother_guardian,
         children_under2, other, parent_id, extra_fix, extra) %>%
  mutate(
    source = "form8newsub1",
    unit_id = as.character(unit_id)
  )

# --- form8newsub2 ---
form8_sub2 <- dbGetQuery(con, "SELECT * FROM ncdd_form8newsub2") %>%
  select(id, formkey, indicator_id, unit_id, indicator_subject,
         total, pregnant, after_pregnant, mother_guardian,
         children_under2, relatetotal, relatewoman, other,
         parent_id, extra_fix, extra) %>%
  mutate(
    source = "form8newsub2",
    unit_id = as.character(unit_id)
  )

# --- form8newsub3 ---
form8_sub3 <- dbGetQuery(con, "SELECT * FROM ncdd_form8newsub3") %>%
  select(id, formkey, parent_id,
         feedback, commenttype, servicetype, subservice,
         comment_date, citizen_type, citizen_gender, comment_throught,
         howtofix, fixed_date, transferto, transfer_date, responetime) %>%
  mutate(
    source = "form8newsub3"
    # no unit_id column here (that's fine)
  )

# --- form8newsub4 ---
form8_sub4 <- dbGetQuery(con, "SELECT * FROM ncdd_form8newsub4") %>%
  select(id, formkey, parent_id, extra_fix, extra,
         supporterhelthvillage_id, namekh, namelatan, supporterhelthgender,
         education, age, nationality, contract_date, onboard_date,
         resigned_date, supporterhelthphone, training_date, trainingtopic,
         trainingby, supporterhelthstatus, supporterhelthother) %>%
  mutate(source = "form8newsub4")

# --- form8newsub5 ---
form8_sub5 <- dbGetQuery(con, "SELECT * FROM ncdd_form8newsub5") %>%
  select(id, formkey, indicator_id, indicator_title, participant,
         participantfemale, meetingdate, venue, parent_id) %>%
  mutate(source = "form8newsub5")

# --- form8newsub6 ---
form8_sub6 <- dbGetQuery(con, "SELECT * FROM ncdd_form8newsub6") %>%
  select(id, formkey, indicator_id, indicator_title, total, unit_id,
         participant, participantfemale, other, parent_id, extra_fix, extra) %>%
  mutate(
    source = "form8newsub6",
    unit_id = as.character(unit_id)
  )

# --- form8newsub7 ---
form8_sub7 <- dbGetQuery(con, "SELECT * FROM ncdd_form8newsub7") %>%
  select(id, formkey, indicator_id, indicator_title,
         semesterplan, previous, current, carry, balance,
         parent_id) %>%
  mutate(source = "form8newsub7")

# Bind
form8_all <- bind_rows(
  form8_sub7,
  form8_sub1, form8_sub2, form8_sub3, form8_sub4,
  form8_sub5, form8_sub6
) %>%
  arrange(source, parent_id, formkey)

location <- dbGetQuery(con, "SELECT * FROM tbl_location")
village <- dbGetQuery(con, "SELECT * FROM tbl_locationvillage")


province_lookup <- location %>%
  select(location_id, title_en, title_kh) %>%
  rename(
    province_id = location_id,
    province_kh = title_kh,
    province_en = title_en

  )

# district lookup
district_lookup <- location %>%
  select(location_id, title_en, title_kh) %>%
  rename(
    district_id = location_id,
    district_kh = title_kh,
    district_en = title_en

  )

# commune lookup
commune_lookup <- location %>%
  select(location_id, title_en, title_kh) %>%
  rename(
    commune_id = location_id,
    commune_kh = title_kh,
    commune_en = title_en

  )

# village lookup
village_lookup <- village %>%
  select(location_id, title_en, title_kh) %>%
  rename(
    village_id = location_id,
    village_kh = title_kh,
  )

form8_Final_clean <- form8_all %>%
  mutate(parent_id = as.numeric(parent_id)) %>%
  left_join(
    form8newmaster %>%
      select(id, province_id, district_id, commune_id, village_id, start_date, end_date,trash),
    by = c("parent_id" = "id"),
    relationship = "many-to-many"
  ) %>%
  left_join(province_lookup, by = "province_id") %>%
  left_join(district_lookup, by = "district_id") %>%
  left_join(commune_lookup, by = "commune_id") %>%
  left_join(village_lookup, by = "village_id") %>%
  mutate(
    start_date = as.Date(start_date),
    end_date   = as.Date(end_date),
    year = lubridate::year(end_date),
    month = lubridate::month(end_date),
    semester   = ifelse(lubridate::month(end_date) <= 6, 1, 2)

  )

keep_cols <- c(
  "commune_id", "province_id", "district_id", "village_id", "end_date","semesterplan",
  "supporterhelthstatus","supporterhelthgender","participant","participantfemale",
  "start_date", "province_kh", "district_kh", "commune_kh", "village_kh",
  "year", "month", "formkey", "indicator_id","current",
  "pregnant", "after_pregnant", "parent_id", "id", "children_under2",
  "mother_guardian", "source", "total","howtofix","training_date","meetingdate",
  "supporterhelthstatus", "commenttype", "female","relatetotal","relatewoman","previous",
  "servicetype","subservice","citizen_type","citizen_gender","supporterhelthvillage_id","semester","trash"
)


form8_Final <- form8_Final_clean %>%
  select(any_of(keep_cols))

form8_Final <- form8_Final %>%
  mutate(
    satisfied = if_else(commenttype == "satisfy", 1, 0, missing = 0),
    dissatisfied = if_else(commenttype == "unsatisfied", 1, 0, missing = 0)
  )

form8_Final <- form8_Final %>%
  mutate(
    howtofix = tolower(trimws(as.character(howtofix))),  # normalize input

    solve = case_when(
      howtofix == "close" ~ 1,
      howtofix == "investigate" ~ 1,
      howtofix == "penalty" ~ 1,
      TRUE ~ 0  # unknown or unmatched values
    ),

    tranfer = if_else(howtofix == "tranfer", 1, 0, missing = 0)
  )

form8_Final <- form8_Final %>%
  mutate(
    servicetype = tolower(trimws(as.character(servicetype))),  # normalize input

    total_servicetype = case_when(
      servicetype %in% c("formnew7sub3_1", "formnew7sub3_2", "formnew7sub3_3", "formnew7sub3_4") ~ 1,
      is.na(servicetype) | servicetype == "" ~ 0,  # handle blank and NA explicitly
      TRUE ~ 0  # all other unmatched values
    )
  )
# form8_Final <- form8_Final %>%
#   mutate(training = ifelse(is.na(training_date), 0, 1))

form8_Final <- form8_Final %>%
  mutate(
    training_date = as.character(training_date),  # make sure it's text first
    training = case_when(
      is.na(training_date) ~ 0L,
      grepl("^-0*1-11-30", training_date) ~ 0L,  # catch -1-11-30 or -001-11-30 etc.
      grepl("^000", training_date) ~ 0L,         # catch any 0000-year patterns
      training_date == "" ~ 0L,
      TRUE ~ 1L
    )
  )



# 0) Helpers to align columns and types
id_cols  <- c("id","parent_id","formkey","indicator_id","unit_id",
              "province_id","district_id","commune_id","village_id","trash")
num_cols <- c("total","total_indigenous","pregnant","after_pregnant","total_servicetype","previous",
              "mother_guardian","children_under2","other","number","participant","participantfemale","training",
              "total_girl","satisfied","dissatisfied","solve","tranfer","year","month","semesterplan","current","semester")



coerce_types <- function(df) {
  df %>%
    dplyr::mutate(
      dplyr::across(dplyr::any_of(id_cols), as.character),
      dplyr::across(dplyr::any_of(num_cols), ~ suppressWarnings(as.numeric(.x))),
      start_date = as.Date(start_date),
      end_date   = as.Date(end_date)
    )
}

add_missing_cols <- function(df, all_cols) {
  miss <- setdiff(all_cols, names(df))
  if (length(miss)) {
    for (m in miss) df[[m]] <- NA
  }
  # reorder to the same column order
  df[, all_cols, drop = FALSE]
}

# 1) Decide the full set of columns to keep (union, not just shared)
union_cols <- union(names(form7_all_with_loc), names(form8_Final))

# 2) Fix types first
form7_fix <- coerce_types(form7_all_with_loc)
form8_fix <- coerce_types(form8_Final)

# 3) Add any missing columns so both have the same schema
form7_full <- add_missing_cols(form7_fix, union_cols)
form8_full <- add_missing_cols(form8_fix, union_cols)

# 4) Bind rows (keeps ALL columns) and tag source
form7_8_all <- dplyr::bind_rows(
  dplyr::mutate(form7_full, dataset = "form7"),
  dplyr::mutate(form8_full, dataset = "form8")
)

form7_all_with_loc <- form7_all_with_loc %>%
  dplyr::mutate(
    dplyr::across(
      dplyr::any_of(id_cols),
      ~ tidyr::replace_na(as.character(.x), "0")
    )
  )

form7_8_all <- form7_8_all %>%
  dplyr::mutate(
    dplyr::across(
      dplyr::any_of(id_cols),
      ~ tidyr::replace_na(as.character(.x), "0")
    )
  )

master7_with_kh <- master7_with_kh %>%
  dplyr::mutate(
    dplyr::across(
      dplyr::any_of(id_cols),
      ~ tidyr::replace_na(as.character(.x), "0")
    )
  )


##############################FORM9#############################################
# 
master9 <- dbGetQuery(con, "SELECT * FROM ncdd_form9newmaster")

Form9_f10 <- dbGetQuery(con, "SELECT * FROM ncdd_form9newform10") %>%
  dplyr::select(id, formkey, form10_nextplan, parent_id, extra_fix, extra) %>%
  dplyr::mutate(source = "form9newform10")

Form9_f9 <- dbGetQuery(con, "SELECT * FROM ncdd_form9newform9") %>%
  dplyr::select(id, formkey, form9_challenge, form9_solution, parent_id, extra_fix, extra) %>%
  dplyr::mutate(source = "form9newform9")

form9_all <- dplyr::bind_rows(Form9_f10, Form9_f9) %>%
  dplyr::arrange(source, parent_id, formkey)

form9_labeled <- form9_all %>%
  mutate(parent_id = as.numeric(parent_id)) %>%
  left_join(
    master9 %>%
      select(id, province_id, district_id,commune_id, start_date, end_date,trash),
    by = c("parent_id" = "id")
  ) %>%
  left_join(province_lookup, by = "province_id") %>%
  left_join(district_lookup, by = "district_id") %>%
  left_join(commune_lookup,  by = "commune_id") %>%
  mutate(
    start_date = as.Date(start_date),
    end_date   = as.Date(end_date),
    year = lubridate::year(end_date),
    month = lubridate::month(end_date),
    semester   = ifelse(lubridate::month(end_date) <= 6, 1, 2)
  )  %>%
  distinct(province_id, district_id, commune_id, start_date, end_date, .keep_all = TRUE)

form9_labeled <- form9_labeled %>%
  mutate(total = ifelse(is.na(form9_challenge), 0, 1))

form9_labeled   <- coerce_types(form9_labeled)

form7_8_filtered <- semi_join(
  form7_8_all,
  form9_labeled,
  by = c("province_id", "district_id", "commune_id", "year")
)

form7_8_9_all <- dplyr::bind_rows(form7_8_filtered, form9_labeled)


##############################FORM10#############################################

master10 <- dbGetQuery(con, "SELECT * FROM ncdd_form10newmaster")

Form10_sub1 <- dbGetQuery(con, "SELECT * FROM ncdd_form10newsub1") %>%
  select(id, formkey, feedback, commenttype, servicetype, subservice, comment_date, citizen_type, citizen_gender, comment_throught, howtofix, fixed_date, transferto, transfer_date, responetime, parent_id, extra_fix, extra, from_commune, from_district, from_province, communetotop, districttotop, provincetotop) %>%
  mutate(source = "form10newsub1")


Form10_sub2 <- dbGetQuery(con, "SELECT * FROM ncdd_form10newsub2") %>%
  select(id, formkey, indicator_id, indicator_title, semesterplan, previous, carry, current, balance, parent_id, extra_fix, extra) %>%
  mutate(source = "form10newsub2")


Form10_tab2 <- dbGetQuery(con, "SELECT * FROM ncdd_form10newtab2") %>%
  select(id, formkey, indicator_id, indicator_title, unit_id, total, participant, participantfemale, other, parent_id, extra_fix, extra) %>%
  mutate(source = "form10newtab2")



form10_all <- dplyr::bind_rows(Form10_sub1, Form10_sub2,Form10_tab2) %>%
  dplyr::arrange(source, parent_id, formkey)

form10_labeled <- form10_all %>%
  mutate(parent_id = as.numeric(parent_id)) %>%
  left_join(
    master10 %>%
      select(id, province_id, district_id, start_date, end_date,trash),
    by = c("parent_id" = "id")
  ) %>%
  left_join(province_lookup, by = "province_id") %>%
  left_join(district_lookup, by = "district_id") %>%
  mutate(
    start_date = as.Date(start_date),
    end_date   = as.Date(end_date),
    year = lubridate::year(end_date),
    month = lubridate::month(end_date),
    semester   = ifelse(lubridate::month(end_date) <= 6, 1, 2)
  )

form10_labeled <- form10_labeled %>%
  mutate(
    commenttype = as.character(commenttype),
    howtofix = as.character(howtofix)
  ) %>%
  ungroup() %>%
  mutate(
    satisfied = if_else(commenttype == "satisfy", 1, 0, missing = 0),
    dissatisfied = if_else(commenttype == "unsatisfied", 1, 0, missing = 0),
    howtofix = tolower(trimws(howtofix)),
    solve = case_when(
      howtofix %in% c("close", "investigate", "penalty") ~ 1,
      TRUE ~ 0
    ),
    tranfer = if_else(howtofix == "tranfer", 1, 0, missing = 0)
  )


#
form10_labeled   <- coerce_types(form10_labeled)

form10_filtered <- semi_join(
  form7_8_all,
  form10_labeled,
  by = c("province_id", "district_id", "year","month")
)

form7_8_10_all <- dplyr::bind_rows(form10_filtered, form10_labeled)

##############################FORM11#############################################

master11 <- dbGetQuery(con, "SELECT * FROM ncdd_form11newmaster")

Form11_form9 <- dbGetQuery(con, "SELECT * FROM ncdd_form11newform9") %>%
  select(id, formkey,form9_challenge, form9_solution, parent_id) %>%
  mutate(source = "form11newform9")


Form11_form10 <- dbGetQuery(con, "SELECT * FROM ncdd_form11newform10") %>%
  select(id, formkey, form10_nextplan, parent_id) %>%
  mutate(source = "form11newform10")

form11_all <- dplyr::bind_rows(Form11_form9, Form11_form10) %>%
  dplyr::arrange(source, parent_id, formkey)

form11_labeled <- form11_all %>%
  mutate(parent_id = as.numeric(parent_id)) %>%
  left_join(
    master11 %>%
      select(id, province_id, district_id, start_date, end_date,year,semester),
      # select(id, province_id, district_id, start_date, end_date),
    by = c("parent_id" = "id")
  ) %>%
  left_join(province_lookup, by = "province_id") %>%
  left_join(district_lookup, by = "district_id") %>%
  mutate(
    start_date = as.Date(start_date),
    end_date   = as.Date(end_date),
    # year = lubridate::year(end_date),
    # month = lubridate::month(end_date),
    # semester   = ifelse(lubridate::month(end_date) <= 6, 1, 2)
    # semester=1
  )

form11_labeled   <- coerce_types(form11_labeled)

form11_filtered <- semi_join(
  form7_8_10_all,
  form11_labeled,
  by = c("province_id", "district_id", "year","semester")
)

form7_8_10_11_all <- dplyr::bind_rows(form11_filtered, form11_labeled)

##############################FORM12#############################################

master12 <- dbGetQuery(con, "SELECT * FROM ncdd_form12newmaster")

Form12_form9 <- dbGetQuery(con, "SELECT * FROM ncdd_form12newform9") %>%
  select(id, formkey,form9_challenge, form9_solution, parent_id) %>%
  mutate(source = "form12newform9")


Form12_form10 <- dbGetQuery(con, "SELECT * FROM ncdd_form12newform10") %>%
  select(id, formkey, form10_nextplan, parent_id) %>%
  mutate(source = "form12newform10")

form12_all <- dplyr::bind_rows(Form12_form9, Form12_form10) %>%
  dplyr::arrange(source, parent_id, formkey)

form12_labeled <- form12_all %>%
  mutate(parent_id = as.numeric(parent_id)) %>%
  left_join(
    master12 %>%
      select(id, province_id, district_id, start_date, end_date,year,semester),
    by = c("parent_id" = "id")
  ) %>%
  left_join(province_lookup, by = "province_id") %>%
  left_join(district_lookup, by = "district_id") %>%
  mutate(
    start_date = as.Date(start_date),
    end_date   = as.Date(end_date),
    # year = lubridate::year(end_date),
    # month = lubridate::month(end_date),
    # semester = 2
    semester   = ifelse(lubridate::month(end_date) <= 6, 1, 2)
  )

form12_labeled   <- coerce_types(form12_labeled)

form12_filtered <- semi_join(
  form7_8_10_all,
  form12_labeled,
  by = c("province_id", "district_id", "year","semester")
)

form7_8_10_11_12_all <- dplyr::bind_rows(form12_filtered, form12_labeled)
form7_8_10_11_12_final <- dplyr::bind_rows(form7_8_10_11_12_all, form7_8_10_11_all)

###################################### Form13 #####################################


master13 <- dbGetQuery(con, "SELECT * FROM ncdd_form13newmaster")

form13_sub1 <- dbGetQuery(con, "SELECT * FROM ncdd_form13newsub1") %>%
  select(id, formkey, parent_id,
         feedback, commenttype, servicetype, subservice,
         comment_date, citizen_type, citizen_gender, comment_throught,
         howtofix, fixed_date, transferto, transfer_date, responetime) %>%
  mutate(
    source = "form13newsub1",
  )

# --- form8newsub2 ---
form13_sub2 <- dbGetQuery(con, "SELECT * FROM ncdd_form13newsub2") %>%
  select(id, formkey, indicator_id, indicator_title,total, participant,
         participantfemale,parent_id) %>%
  mutate(
    source = "form13newsub2"
  )
# --- form8newsub3 ---
form13_sub3 <- dbGetQuery(con, "SELECT * FROM ncdd_form13newsub3") %>%
  select(id, formkey, indicator_id, indicator_title,
         semesterplan, previous, current, carry, balance,
         parent_id) %>%
  mutate(
    source = "form13newsub3"
    # no unit_id column here (that's fine)
  )

Form13_form9 <- dbGetQuery(con, "SELECT * FROM ncdd_form13newform9") %>%
  select(id, formkey,form9_challenge, form9_solution, parent_id) %>%
  mutate(source = "form13newform9")


Form13_form10 <- dbGetQuery(con, "SELECT * FROM ncdd_form13newform10") %>%
  select(id, formkey, form10_nextplan, parent_id) %>%
  mutate(source = "form13newform10")

form13_all <- dplyr::bind_rows(Form13_form9, Form13_form10,form13_sub1,form13_sub2,form13_sub3) %>%
  dplyr::arrange(source, parent_id, formkey)

form13_labeled <- form13_all %>%
  mutate(parent_id = as.numeric(parent_id)) %>%
  left_join(
    master13 %>%
      select(id, province_id, start_date, end_date),
    # select(id, province_id, district_id, start_date, end_date),
    by = c("parent_id" = "id")
  ) %>%
  left_join(province_lookup, by = "province_id") %>%
  mutate(
    start_date = as.Date(start_date),
    end_date   = as.Date(end_date),
    year = lubridate::year(end_date),
    month = lubridate::month(end_date),
    semester   = ifelse(lubridate::month(end_date) <= 6, 1, 2)
  )

form13_labeled   <- coerce_types(form13_labeled)

form13_filtered <- semi_join(
  form7_8_10_all,
  form13_labeled,
  by = c("province_id", "year","month")
)

form7_8_10_13_all <- dplyr::bind_rows(form13_filtered, form13_labeled)

#################################FORM14###########################################
master14 <- dbGetQuery(con, "SELECT * FROM ncdd_form14newmaster")

Form14_form9 <- dbGetQuery(con, "SELECT * FROM ncdd_form14newform9") %>%
  select(id, formkey,form9_challenge, form9_solution, parent_id) %>%
  mutate(source = "form14newform9")


Form14_form10 <- dbGetQuery(con, "SELECT * FROM ncdd_form14newform10") %>%
  select(id, formkey, form10_nextplan, parent_id) %>%
  mutate(source = "form14newform10")

form14_all <- dplyr::bind_rows(Form14_form9, Form14_form10) %>%
  dplyr::arrange(source, parent_id, formkey)

form14_labeled <- form14_all %>%
  mutate(parent_id = as.numeric(parent_id)) %>%
  left_join(
    master14 %>%
      select(id, province_id, start_date, end_date,year,semester),
    # select(id, province_id, district_id, start_date, end_date),
    by = c("parent_id" = "id")
  ) %>%
  left_join(province_lookup, by = "province_id") %>%
  mutate(
    start_date = as.Date(start_date),
    end_date   = as.Date(end_date),
    # year = lubridate::year(end_date),
    # month = lubridate::month(end_date),
    # semester   = ifelse(lubridate::month(end_date) <= 6, 1, 2)
  )

form14_labeled   <- coerce_types(form14_labeled)

form14_filtered <- semi_join(
  form7_8_10_13_all,
  form14_labeled,
  by = c("province_id", "year","semester")
)

form7_8_10_14_all <- dplyr::bind_rows(form14_filtered, form14_labeled)

################################################################################
master15 <- dbGetQuery(con, "SELECT * FROM ncdd_form15newmaster")

Form15_form9 <- dbGetQuery(con, "SELECT * FROM ncdd_form15newform9") %>%
  select(id, formkey,form9_challenge, form9_solution, parent_id) %>%
  mutate(source = "form15newform9")


Form15_form10 <- dbGetQuery(con, "SELECT * FROM ncdd_form15newform10") %>%
  select(id, formkey, form10_nextplan, parent_id) %>%
  mutate(source = "form15newform10")

form15_all <- dplyr::bind_rows(Form15_form9, Form15_form10) %>%
  dplyr::arrange(source, parent_id, formkey)

form15_labeled <- form15_all %>%
  mutate(parent_id = as.numeric(parent_id)) %>%
  left_join(
    master15 %>%
      select(id, province_id, district_id, start_date, end_date,year),
    by = c("parent_id" = "id")
  ) %>%
  left_join(province_lookup, by = "province_id") %>%
  mutate(
    start_date = as.Date(start_date),
    end_date   = as.Date(end_date),
    # year = lubridate::year(end_date),
    # month = lubridate::month(end_date),
    semester=2,
  )

form15_labeled   <- coerce_types(form15_labeled)

form15_filtered <- semi_join(
  form7_8_10_14_all,
  form15_labeled,
  by = c("province_id", "year")
)

filter15_semester2<- semi_join(
  form7_8_10_13_all,
  form15_labeled,
  by = c("province_id", "year","semester")
)

form7_8_10_11_15_all <- dplyr::bind_rows(form15_filtered, form15_labeled)
form7_8_10_11_15_final <- dplyr::bind_rows(form7_8_10_11_15_all, filter15_semester2)

#################################FORM14#########################################
# Province choices
province_choices <- master7_with_kh %>%
  select(province_id, province_kh, province_en) %>%
  distinct() %>%
  arrange(province_en) %>%
  transmute(name = province_en, value = province_kh)

province_choices <- c("All" = "All", setNames(province_choices$value, province_choices$name))


# District choices
district_choices <- master7_with_kh %>%
  select(district_id, district_kh, district_en) %>%
  distinct() %>%
  arrange(district_kh) %>%
  transmute(name = district_en, value = district_kh)

district_choices <- c("All" = "All", setNames(district_choices$value, district_choices$name))


# Commune choices
commune_choices <- master7_with_kh %>%
  select(commune_id, commune_kh, commune_en) %>%
  distinct() %>%
  arrange(commune_kh) %>%
  transmute(name = commune_en, value = commune_kh)

commune_choices <- c("All" = "All", setNames(commune_choices$value, commune_choices$name))


# Village choices
village_choices <- master7_with_kh %>%
  select(village_id, village_kh, village_en) %>%
  distinct() %>%
  arrange(village_kh) %>%
  transmute(name = village_en, value = village_kh)

village_choices <- c("All" = "All", setNames(village_choices$value, village_choices$name))


# Year, Month, Semester (Safest Version)
year_choices     <- c("All" = "All", sort(unique(master7_with_kh$year)))
month_choices    <- c("All" = "All", sort(unique(master7_with_kh$month)))
semester_choices <- c("All" = "All", sort(unique(master7_with_kh$semester)))
