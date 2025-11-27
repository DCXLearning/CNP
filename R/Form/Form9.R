# source("global.R")
# source("R/Server.R")
from9_summary <- reactive({
  validate(need(exists("form7_8_9_all"),
                "form7_8_9_all not found (build it in global.R)"))
  
  df <- form7_8_9_all %>%
    dplyr::mutate(
      start_date = as.Date(start_date),
      end_date   = as.Date(end_date),
      year  = if (!"year"  %in% names(.))  lubridate::year(end_date)  else year,
      month = if (!"month" %in% names(.)) lubridate::month(end_date) else month
    )
  # df <- apply_location_scope(df)
  
  # ---- apply filters ----
  if (!is.null(input$flt_province) && input$flt_province != "All")
    df <- df %>% dplyr::filter(province_kh == input$flt_province)
  if (!is.null(input$flt_district) && input$flt_district != "All")
    df <- df %>% dplyr::filter(district_kh == input$flt_district)
  if (!is.null(input$flt_commune) && input$flt_commune != "All")
    df <- df %>% dplyr::filter(commune_kh == input$flt_commune)
  if (!is.null(input$flt_year) && input$flt_year != "All")
    df <- df %>% dplyr::filter(year == as.integer(input$flt_year))
  
  keys <- c("province_id","province_kh",
            "district_id","district_kh",
            "commune_id","commune_kh",
 
            "year"
  )
  
  # ===== Sub1 (existing) =====
  base_f <- df %>%
    dplyr::filter(
      formkey == "formnew7sub1",
      indicator_id %in% c("f7_tbl1_1","f7_tbl1_2","f7_tbl1_3","f7_tbl1_4","f7_tbl1_5")
    ) %>%
    dplyr::mutate(ind_kh = dplyr::recode(indicator_id,
                                         "f7_tbl1_1" = "គ្រួសារ",
                                         "f7_tbl1_2" = "ស.ផ",
                                         "f7_tbl1_3" = "ស.ក",
                                         "f7_tbl1_4" = "កុ<២ឆ្នាំ",
                                         "f7_tbl1_5" = "ទា.សំ.ណ"
    ))
  
  pivot_by <- function(dat, value_col) {
    dat %>%
      dplyr::group_by(dplyr::across(dplyr::all_of(keys)), ind_kh) %>%
      dplyr::summarise(value = sum(.data[[value_col]], na.rm = TRUE), .groups = "drop") %>%
      tidyr::pivot_wider(names_from = ind_kh, values_from = value, values_fill = 0)
  }
  
  wide_total <- pivot_by(base_f, "total")
  wide_indig <- pivot_by(base_f, "total_indigenous") %>%
    dplyr::rename_with(~ paste0(.x, "_ដើមភាគតិច"),
                       setdiff(names(.), keys))
  
  sub1_wide <- wide_total %>%
    dplyr::left_join(wide_indig, by = keys)
  
  # --- helper once (you already have it, keeping here for clarity) ---
  is_female <- function(x) {
    xl <- tolower(trimws(as.character(x)))
    as.integer(xl %in% c("female","f","ស្រី"))
  }
  
  # --- map codes for the 3 type columns (same as before) ---
  type_map <- c(
    "f7_tbl3_2" = "ពិការភាព",
    "f7_tbl3_3" = "អ្នកក្រីក្រ",
    "f7_tbl3_4" = "ជនជាតិ.តិច"
  )
  valid_codes <- names(type_map)
  
  # ================== F7 (formnew7sub3) ==================
  # NOTE: totals ONLY from indicator_id == f7_tbl3_1
  f7_totals <- df %>%
    dplyr::filter(formkey == "formnew7sub3", indicator_id == "f7_tbl3_1") %>%
    dplyr::mutate(
      total      = readr::parse_number(as.character(total)),
      total_girl = readr::parse_number(as.character(total_girl))
    ) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `សរុប_f7`  = sum(total, na.rm = TRUE),
      `ស្ត្រី_f7` = sum(total_girl, na.rm = TRUE),
      .groups = "drop"
    )
  
  # F7 type columns (same as your current approach; uses `number` if you prefer)
  f7_types <- df %>%
    dplyr::filter(formkey == "formnew7sub3", indicator_id %in% c("f7_tbl3_2","f7_tbl3_3","f7_tbl3_4")) %>%
    dplyr::mutate(
      indicator_id = trimws(as.character(indicator_id)),
      ind_kh = dplyr::recode(indicator_id, !!!type_map, .default = NA_character_),
      number = readr::parse_number(as.character(dplyr::coalesce(number, total)))  # fallback if `number` missing
    ) %>%
    dplyr::filter(!is.na(ind_kh)) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys)), ind_kh) %>%
    dplyr::summarise(val = sum(number, na.rm = TRUE), .groups = "drop") %>%
    tidyr::pivot_wider(
      names_from  = ind_kh,
      values_from = val,
      values_fill = 0
    ) %>%
    # suffix with _f7 so we can sum with _f8 safely
    dplyr::rename(
      `ពិការភាព_f7`   = `ពិការភាព`,
      `អ្នកក្រីក្រ_f7`  = `អ្នកក្រីក្រ`,
      `ជនជាតិ.តិច_f7` = `ជនជាតិ.តិច`
    )
  
  # ================== F8 (source/dataset == form8) ==================
  f8_counts <- df %>%
    dplyr::filter((!"dataset" %in% names(.)) | dataset == "form8" | ( "source" %in% names(.) & source == "form8")) %>%
    dplyr::mutate(
      svc          = if ("citizen_type"    %in% names(.)) trimws(as.character(citizen_type))       else NA_character_,
      citizen_sex  = if ("citizen_gender" %in% names(.)) citizen_gender                          else NA_character_,
      female       = is_female(citizen_sex),
      code         = if ("citizen_type"   %in% names(.)) trimws(as.character(citizen_type))      else NA_character_
    ) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      # totals: count rows with non-empty servicetype
      `សរុប_f8`        = sum(!is.na(svc) & svc != "", na.rm = TRUE),
      # girls: count rows where citizen_gender is female (no servicetype filter here per your spec)
      `ស្ត្រី_f8`       = sum(female == 1, na.rm = TRUE),
      # 3 per-type columns from citizen_type
      `ពិការភាព_f8`   = sum(code == "f7_tbl3_2", na.rm = TRUE),
      `អ្នកក្រីក្រ_f8`  = sum(code == "f7_tbl3_3", na.rm = TRUE),
      `ជនជាតិ.តិច_f8` = sum(code == "f7_tbl3_4", na.rm = TRUE),
      .groups = "drop"
    )
  
  # ================== join F7 pieces, add F8, then SUM ==================
  # Start from whatever table you want these merged into — here I assume `sub1_wide`.
  citizen_joined <- sub1_wide %>%
    dplyr::left_join(f7_totals, by = keys) %>%
    dplyr::left_join(f7_types,  by = keys) %>%
    dplyr::left_join(f8_counts, by = keys) %>%
    # fill any missing suffix columns so rowSums works
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of(c(
          "សរុប_f7","ស្ត្រី_f7","ពិការភាព_f7","អ្នកក្រីក្រ_f7","ជនជាតិ.តិច_f7",
          "សរុប_f8","ស្ត្រី_f8","ពិការភាព_f8","អ្នកក្រីក្រ_f8","ជនជាតិ.តិច_f8"
        )),
        ~ tidyr::replace_na(.x, 0)
      ),
      # final summed columns
      `សរុប`        = rowSums(dplyr::pick(dplyr::any_of(c("សរុប_f7","សរុប_f8"))), na.rm = TRUE),
      `ស្ត្រី`       = rowSums(dplyr::pick(dplyr::any_of(c("ស្ត្រី_f7","ស្ត្រី_f8"))), na.rm = TRUE),
      `ពិការភាព`   = rowSums(dplyr::pick(dplyr::any_of(c("ពិការភាព_f7","ពិការភាព_f8"))), na.rm = TRUE),
      `អ្នកក្រីក្រ`  = rowSums(dplyr::pick(dplyr::any_of(c("អ្នកក្រីក្រ_f7","អ្នកក្រីក្រ_f8"))), na.rm = TRUE),
      `ជនជាតិ.តិច` = rowSums(dplyr::pick(dplyr::any_of(c("ជនជាតិ.តិច_f7","ជនជាតិ.តិច_f8"))), na.rm = TRUE)
    ) %>%
    # (optional) drop the helper suffix columns now that finals are computed
    dplyr::select(-dplyr::ends_with("_f7"), -dplyr::ends_with("_f8"))

  
  # ================== F9 (formnew9form9) ==================
  # f9_counts <- df %>%
  #   dplyr::filter(formkey == "formnew9form9") %>%
  #   dplyr::mutate(
  #     total = readr::parse_number(as.character(total))
  #   ) %>%
  #   dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
  #   dplyr::summarise(
  #     `សរុប_f9` = sum(total, na.rm = TRUE),
  #     .groups = "drop"
  #   )
  # cat("\n================= F9 COUNTS =================\n")
  # print(f9_counts, n = Inf)
  # 
  # cat("\n================= F7 TOTALS =================\n")
  # print(f7_totals, n = Inf)
  # 
  # cat("\n================= F8 COUNTS =================\n")
  # print(f8_counts, n = Inf)
  # 
  # # ================== join F7, F8, and F9, then SUM ==================
  # citizen_joined <- sub1_wide %>%
  #   dplyr::left_join(f7_totals, by = keys) %>%
  #   dplyr::left_join(f7_types,  by = keys) %>%
  #   dplyr::left_join(f8_counts, by = keys) %>%
  #   dplyr::left_join(f9_counts, by = keys) %>%
  #   dplyr::mutate(
  #     dplyr::across(
  #       dplyr::any_of(c(
  #         "សរុប_f7","ស្ត្រី_f7","ពិការភាព_f7","អ្នកក្រីក្រ_f7","ជនជាតិ.តិច_f7",
  #         "សរុប_f8","ស្ត្រី_f8","ពិការភាព_f8","អ្នកក្រីក្រ_f8","ជនជាតិ.តិច_f8",
  #         "សរុប_f9"
  #       )),
  #       ~ tidyr::replace_na(.x, 0)
  #     ),
  #     # final summed columns
  #     `សរុប`        = rowSums(dplyr::pick(dplyr::any_of(c("សរុប_f7","សរុប_f8","សរុប_f9"))), na.rm = TRUE),
  #     `ស្ត្រី`       = rowSums(dplyr::pick(dplyr::any_of(c("ស្ត្រី_f7","ស្ត្រី_f8"))), na.rm = TRUE),
  #     `ពិការភាព`   = rowSums(dplyr::pick(dplyr::any_of(c("ពិការភាព_f7","ពិការភាព_f8"))), na.rm = TRUE),
  #     `អ្នកក្រីក្រ`  = rowSums(dplyr::pick(dplyr::any_of(c("អ្នកក្រីក្រ_f7","អ្នកក្រីក្រ_f8"))), na.rm = TRUE),
  #     `ជនជាតិ.តិច` = rowSums(dplyr::pick(dplyr::any_of(c("ជនជាតិ.តិច_f7","ជនជាតិ.តិច_f8"))), na.rm = TRUE)
  #   ) %>%
  #   dplyr::select(-dplyr::ends_with("_f7"), -dplyr::ends_with("_f8"), -dplyr::ends_with("_f9"))
  # 
  # 
  # ===== Sub2 additions =====
  # កញ្ចប់ទី១: formnew7sub2_1 → (លទ្ធផល=total, ស.ផ=pregnant)
  k1 <- df %>%
    dplyr::filter(formkey == "formnew7sub2_1") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `កញ្ចប់ទី១_លទ្ធផល` = sum(total, na.rm = TRUE),
      `កញ្ចប់ទី១_ស.ផ`     = sum(pregnant, na.rm = TRUE),
      .groups = "drop"
    )
  
  # កញ្ចប់ទី២: formnew7sub2_2 → (លទ្ធផល=total, ស.ក=after_pregnant)
  k2 <- df %>%
    dplyr::filter(formkey == "formnew7sub2_2") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `កញ្ចប់ទី២_លទ្ធផល` = sum(total, na.rm = TRUE),
      `កញ្ចប់ទី២_ស.ក`     = sum(after_pregnant, na.rm = TRUE),
      .groups = "drop"
    )
  
  k3 <- df %>%
    dplyr::filter(formkey == "formnew7sub2_3") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `កញ្ចប់ទី៣_លទ្ធផល` = sum(total, na.rm = TRUE),
      `កញ្ចប់ទី៣_កុ<២ឆ្នាំ`     = sum(children_under2, na.rm = TRUE),
      .groups = "drop"
    )
  k4 <- df %>%
    dplyr::filter(formkey == "formnew7sub2_4") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `កញ្ចប់ទី៤_លទ្ធផល` = sum(total, na.rm = TRUE),
      `កញ្ចប់ទី៤_កុ<២ឆ្នាំ`     = sum(children_under2, na.rm = TRUE),
      .groups = "drop"
    )
  k5 <- df %>%
    dplyr::filter(formkey == "formnew7sub2_5") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `កញ្ចប់ទី៥_លទ្ធផល` = sum(total, na.rm = TRUE),
      `កញ្ចប់ទី៥_ស.ផ`     = sum(pregnant, na.rm = TRUE),
      `កញ្ចប់ទី៥_ស.ក` = sum(after_pregnant, na.rm = TRUE),
      `កញ្ចប់ទី៥_មា/អា`     = sum(mother_guardian, na.rm = TRUE),
      `កញ្ចប់ទី៥_កុ<២ឆ្នាំ`     = sum(children_under2, na.rm = TRUE),
      .groups = "drop"
    )
  
  k6 <- df %>%
    dplyr::filter(formkey == "formnew7sub2_6") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `កញ្ចប់ទី៦_លទ្ធផល` = sum(total, na.rm = TRUE),
      `កញ្ចប់ទី៦_ស.ក` = sum(after_pregnant, na.rm = TRUE),
      `កញ្ចប់ទី៦_មា/អា`     = sum(mother_guardian, na.rm = TRUE),
      `កញ្ចប់ទី៦_កុ<២ឆ្នាំ`     = sum(children_under2, na.rm = TRUE),
      .groups = "drop"
    )
  
  
  k7 <- df %>%
    dplyr::filter(formkey == "formnew7sub2_7") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `កញ្ចប់ទី៧_លទ្ធផល` = sum(total, na.rm = TRUE),
      `កញ្ចប់ទី៧_ស.ក` = sum(after_pregnant, na.rm = TRUE),
      `កញ្ចប់ទី៧_មា/អា`     = sum(mother_guardian, na.rm = TRUE),
      .groups = "drop"
    )
  
  k8 <- df %>%
    dplyr::filter(formkey == "formnew7sub2_8") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `កញ្ចប់ទី៨_លទ្ធផល` = sum(total, na.rm = TRUE),
      `កញ្ចប់ទី៨_ស.ផ`     = sum(pregnant, na.rm = TRUE),
      `កញ្ចប់ទី៨_ស.ក` = sum(after_pregnant, na.rm = TRUE),
      `កញ្ចប់ទី៨_មា/អា`     = sum(mother_guardian, na.rm = TRUE),
      .groups = "drop"
    )
  
  k9 <- df %>%
    dplyr::filter(formkey == "formnew7sub2_9") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `កញ្ចប់ទី៩_លទ្ធផល` = sum(total, na.rm = TRUE),
      .groups = "drop"
    )
  k10 <- df %>%
    dplyr::filter(dataset == "form8") %>%
    dplyr::filter(formkey == "formnew8tab1_1") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `ខកញ្ចប់ទី១_លទ្ធផល` = sum(total, na.rm = TRUE),
      `ខកញ្ចប់ទី១_ស.ផ`     = sum(pregnant, na.rm = TRUE),
      .groups = "drop"
    )
  
  # ខកញ្ចប់ទី២: formnew7sub2_2 → (លទ្ធផល=total, ស.ក=after_pregnant)
  k11 <- df %>%
    dplyr::filter(formkey == "formnew8tab1_2") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `ខកញ្ចប់ទី២_លទ្ធផល` = sum(total, na.rm = TRUE),
      `ខកញ្ចប់ទី២_ស.ក`     = sum(after_pregnant, na.rm = TRUE),
      `ខកញ្ចប់ទី២_មា/អា`     = sum(mother_guardian, na.rm = TRUE),
      .groups = "drop"
    )
  
  k12 <- df %>%
    dplyr::filter(formkey == "formnew8tab1_3") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `ខកញ្ចប់ទី៣_លទ្ធផល` = sum(total, na.rm = TRUE),
      `ខកញ្ចប់ទី៣_កុ<២ឆ្នាំ`     = sum(children_under2, na.rm = TRUE),
      .groups = "drop"
    )
  k13 <- df %>%
    dplyr::filter(formkey == "formnew8tab1_4") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `ខកញ្ចប់ទី៤_លទ្ធផល` = sum(total, na.rm = TRUE),
      `ខកញ្ចប់ទី៤_កុ<២ឆ្នាំ`     = sum(children_under2, na.rm = TRUE),
      .groups = "drop"
    )
  k14 <- df %>%
    dplyr::filter(formkey == "formnew8tab1_5") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `ខកញ្ចប់ទី៥_លទ្ធផល` = sum(total, na.rm = TRUE),
      `ខកញ្ចប់ទី៥_ស.ផ`     = sum(pregnant, na.rm = TRUE),
      `ខកញ្ចប់ទី៥_ស.ក` = sum(after_pregnant, na.rm = TRUE),
      `ខកញ្ចប់ទី៥_មា/អា`     = sum(mother_guardian, na.rm = TRUE),
      `ខកញ្ចប់ទី៥_កុ<២ឆ្នាំ`     = sum(children_under2, na.rm = TRUE),
      .groups = "drop"
    )
  
  k15 <- df %>%
    dplyr::filter(formkey == "formnew8tab1_6") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `ខកញ្ចប់ទី៦_លទ្ធផល` = sum(total, na.rm = TRUE),
      `ខកញ្ចប់ទី៦_ស.ក` = sum(after_pregnant, na.rm = TRUE),
      `ខកញ្ចប់ទី៦_មា/អា`     = sum(mother_guardian, na.rm = TRUE),
      `ខកញ្ចប់ទី៦_កុ<២ឆ្នាំ`     = sum(children_under2, na.rm = TRUE),
      .groups = "drop"
    )
  
  
  k16 <- df %>%
    dplyr::filter(formkey == "formnew8tab1_7") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `ខកញ្ចប់ទី៧_លទ្ធផល` = sum(total, na.rm = TRUE),
      `ខកញ្ចប់ទី៧_ស.ក` = sum(after_pregnant, na.rm = TRUE),
      `ខកញ្ចប់ទី៧_មា/អា`     = sum(mother_guardian, na.rm = TRUE),
      .groups = "drop"
    )
  
  k17 <- df %>%
    dplyr::filter(formkey == "formnew8tab1_8") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `ខកញ្ចប់ទី៨_លទ្ធផល` = sum(total, na.rm = TRUE),
      `ខកញ្ចប់ទី៨_ស.ផ`     = sum(pregnant, na.rm = TRUE),
      `ខកញ្ចប់ទី៨_ស.ក` = sum(after_pregnant, na.rm = TRUE),
      `ខកញ្ចប់ទី៨_មា/អា`     = sum(mother_guardian, na.rm = TRUE),
      .groups = "drop"
    )
  k18 <- df %>%
    dplyr::filter(formkey == "formnew8sub2_1") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `ឃុំ_លទ្ធផល` = sum(total, na.rm = TRUE),
      `ឃុំ_ស.ផ`     = sum(pregnant, na.rm = TRUE),,
      `ឃុំ_មា/អា`     = sum(mother_guardian, na.rm = TRUE),
      `ឃុំ_កុ<២ឆ្នាំ`     = sum(children_under2, na.rm = TRUE),
      `ឃុំ_អ្នកពាក់ព័ន្ធ`     = sum(relatetotal, na.rm = TRUE),
      .groups = "drop"
    )
  
  k19 <- df %>%
    dplyr::filter(formkey == "formnew8sub2_2") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `មណ្ឌលសុខភាព_លទ្ធផល` = sum(total, na.rm = TRUE),
      `មណ្ឌលសុខភាព_ស.ផ`     = sum(pregnant, na.rm = TRUE),,
      `មណ្ឌលសុខភាព_មា/អា`     = sum(mother_guardian, na.rm = TRUE),
      `មណ្ឌលសុខភាព_កុ<២ឆ្នាំ`     = sum(children_under2, na.rm = TRUE),
      `មណ្ឌលសុខភាព_អ្នកពាក់ព័ន្ធ`     = sum(relatetotal, na.rm = TRUE),
      .groups = "drop"
    )
  # ===== satisfied by type (supports form7 via formkey and form8 via servicetype) =====
  # ===== satisfied by type (supports form7 via formkey and form8 via servicetype) =====
  type_keys <- c("formnew7sub3_1","formnew7sub3_2","formnew7sub3_3","formnew7sub3_4")
  
  map_type <- function(x) dplyr::recode(
    x,
    "formnew7sub3_1" = "ប្រភេទ ក",
    "formnew7sub3_2" = "ប្រភេទ ខ",
    "formnew7sub3_3" = "ប្រភេទ គ",
    "formnew7sub3_4" = "ប្រភេទ ឃ",
    .default = NA_character_
  )
  
  # create a vector for servicetype even if the column doesn't exist
  svc_col <- if ("servicetype" %in% names(df)) df$servicetype else rep(NA_character_, nrow(df))
  
  sub3_types_wide <- df %>%
    dplyr::mutate(
      # pick type from formkey (form7) or servicetype (form8)
      type_code = dplyr::coalesce(
        dplyr::if_else(formkey %in% type_keys, formkey, NA_character_),
        dplyr::if_else(svc_col %in% type_keys, svc_col, NA_character_)
      ),
      type_kh = map_type(type_code),
      .satisfied_num = readr::parse_number(as.character(satisfied)),
      .satisfied_num = dplyr::coalesce(.satisfied_num, 0)
    ) %>%
    dplyr::filter(!is.na(type_kh)) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys)), type_kh) %>%
    dplyr::summarise(val = sum(.satisfied_num, na.rm = TRUE), .groups = "drop") %>%
    tidyr::pivot_wider(
      names_from  = type_kh,
      values_from = val,
      values_fill = 0
    )
  
  sub1_wide_total <- sub3_types_wide %>%                       # or the chain where you build final
    dplyr::left_join(citizen_joined, by = keys) %>%
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of(c("ប្រភេទ ក","ប្រភេទ ខ","ប្រភេទ គ","ប្រភេទ ឃ")),
        ~ tidyr::replace_na(.x, 0)
      )
    )
  
  # ===== dissatisfied by type (supports form7 via formkey and form8 via servicetype) =====
  # ===== dissatisfied by type (supports form7 via formkey and form8 via servicetype) =====
  
  dis_map_type <- function(x) dplyr::recode(
    x,
    "formnew7sub3_1" = "មិនប្រភេទ ក",
    "formnew7sub3_2" = "មិនប្រភេទ ខ",
    "formnew7sub3_3" = "មិនប្រភេទ គ",
    "formnew7sub3_4" = "មិនប្រភេទ ឃ",
    .default = NA_character_
  )
  
  # create a vector for servicetype even if the column doesn't exist
  
  sub3_types_diss <- df %>%
    dplyr::mutate(
      # pick type from formkey (form7) or servicetype (form8)
      type_code = dplyr::coalesce(
        dplyr::if_else(formkey %in% type_keys, formkey, NA_character_),
        dplyr::if_else(svc_col %in% type_keys, svc_col, NA_character_)
      ),
      type_kh = dis_map_type(type_code),
      .dissatisfied_num = readr::parse_number(as.character(dissatisfied)),
      .dissatisfied_num = dplyr::coalesce(.dissatisfied_num, 0)
    ) %>%
    dplyr::filter(!is.na(type_kh)) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys)), type_kh) %>%
    dplyr::summarise(val = sum(.dissatisfied_num, na.rm = TRUE), .groups = "drop") %>%
    tidyr::pivot_wider(
      names_from  = type_kh,
      values_from = val,
      values_fill = 0
    )
  
  sub1_wide_diss <- sub3_types_diss %>%                       # or the chain where you build final
    dplyr::left_join(sub1_wide_total, by = keys) %>%
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of(c("មិនប្រភេទ ក","មិនប្រភេទ ខ","មិនប្រភេទ គ","មិនប្រភេទ ឃ")),
        ~ tidyr::replace_na(.x, 0)
      )
    )
  
  
  map_type_solve <- function(x) dplyr::recode(
    x,
    "formnew7sub3_1" = "ដោះស្រាយ_ប្រភេទ ក",
    "formnew7sub3_2" = "ដោះស្រាយ_ប្រភេទ ខ",
    "formnew7sub3_3" = "ដោះស្រាយ_ប្រភេទ គ",
    "formnew7sub3_4" = "ដោះស្រាយ_ប្រភេទ ឃ",
    .default = NA_character_
  )
  
  svc_col   <- if ("servicetype" %in% names(df)) as.character(df$servicetype) else rep(NA_character_, nrow(df))
  solve_col <- if ("solve"       %in% names(df)) as.numeric(df$solve)         else rep(NA_real_,      nrow(df))
  
  solve_types_wide <- df %>%
    dplyr::mutate(
      type_code = dplyr::coalesce(
        dplyr::if_else(formkey %in% type_keys, as.character(formkey), NA_character_),
        dplyr::if_else(svc_col %in% type_keys,  svc_col,               NA_character_)
      ),
      type_kh    = map_type_solve(type_code),
      .solve_num = dplyr::coalesce(solve_col, 0),
      # count as 1 if solve > 0, else 0
      .solve_flag = dplyr::if_else(.solve_num > 0, 1L, 0L)
    ) %>%
    dplyr::filter(!is.na(type_kh)) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys)), type_kh) %>%
    dplyr::summarise(val = sum(.solve_flag, na.rm = TRUE), .groups = "drop") %>%
    tidyr::pivot_wider(
      names_from  = type_kh,
      values_from = val,
      values_fill = 0
    )
  
  
  
  fix_final <- sub1_wide_diss %>%
    dplyr::left_join(solve_types_wide, by = keys) %>%
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of(c("ដោះស្រាយ_ប្រភេទ ក","ដោះស្រាយ_ប្រភេទ ខ","ដោះស្រាយ_ប្រភេទ គ","ដោះស្រាយ_ប្រភេទ ឃ")),
        ~ tidyr::replace_na(.x, 0)
      )
    )
  
  map_type_solve_transfer <- function(x) dplyr::recode(
    x,
    "formnew7sub3_1" = "ដោះស្រាយ_ប្រភេទបញ្ជូន ក",
    "formnew7sub3_2" = "ដោះស្រាយ_ប្រភេទបញ្ជូន ខ",
    "formnew7sub3_3" = "ដោះស្រាយ_ប្រភេទបញ្ជូន គ",
    "formnew7sub3_4" = "ដោះស្រាយ_ប្រភេទបញ្ជូន ឃ",
    .default = NA_character_
  )
  
  svc_col_tranfer   <- if ("servicetype" %in% names(df)) as.character(df$servicetype) else rep(NA_character_, nrow(df))
  solve_col_tranfer <- if ("tranfer"       %in% names(df)) as.numeric(df$tranfer)         else rep(NA_real_,      nrow(df))
  
  solve_types_tranfer_wide <- df %>%
    dplyr::mutate(
      type_code = dplyr::coalesce(
        dplyr::if_else(formkey %in% type_keys, as.character(formkey), NA_character_),
        dplyr::if_else(svc_col_tranfer %in% type_keys,  svc_col_tranfer,               NA_character_)
      ),
      type_kh    = map_type_solve_transfer(type_code),
      .solve_num = dplyr::coalesce(solve_col_tranfer, 0),
      # count as 1 if solve > 0, else 0
      .solve_flag = dplyr::if_else(.solve_num > 0, 1L, 0L)
    ) %>%
    dplyr::filter(!is.na(type_kh)) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys)), type_kh) %>%
    dplyr::summarise(val = sum(.solve_flag, na.rm = TRUE), .groups = "drop") %>%
    tidyr::pivot_wider(
      names_from  = type_kh,
      values_from = val,
      values_fill = 0
    )
  
  fix_tranfer <- fix_final %>%
    dplyr::left_join(solve_types_tranfer_wide, by = keys) %>%
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of(c("ដោះស្រាយ_ប្រភេទបញ្ជូន ក","ដោះស្រាយ_ប្រភេទបញ្ជូន ខ","ដោះស្រាយ_ប្រភេទបញ្ជូន គ","ដោះស្រាយ_ប្រភេទបញ្ជូន ឃ")),
        ~ tidyr::replace_na(.x, 0)
      )
    )


  # --- Only use the last month with data (per keys incl. year) ---
  # supporter_status_wide <- df %>%
  #   dplyr::filter(!is.na(supporterhelthvillage_id)) %>%
  #   dplyr::mutate(
  #     end_date = as.Date(end_date),
  #     year  = if (!"year"  %in% names(.))  lubridate::year(end_date)  else year,
  #     month = if (!"month" %in% names(.)) lubridate::month(end_date) else month,
  #     sh_status = if ("supporterhelthstatus" %in% names(.))
  #       tolower(trimws(as.character(supporterhelthstatus))) else NA_character_,
  #     sh_gender = if ("supporterhelthgender" %in% names(.))
  #       supporterhelthgender else NA_character_,
  #     female = is_female(sh_gender),
  #     # sh_training = if ("training_date" %in% names(.)) {
  #     #   td <- trimws(as.character(training_date))
  #     #   !is.na(td) & td != ""
  #     # } else FALSE
  #     sh_training = if ("training_date" %in% names(.)) {
  #       ifelse(is.na(training_date), 0, 1)
  #     } else {
  #       0
  #     }
  # 
  #   ) %>%
  #   dplyr::filter(!is.na(sh_status), sh_status %in% c("resign","replace","onboard")) %>%
  # 
  #   # find the last month that has any rows for each group (keys include `year`)
  #   dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
  #   dplyr::mutate(.last_month = max(month, na.rm = TRUE)) %>%
  #   dplyr::ungroup() %>%
  #   dplyr::filter(month == .last_month) %>%   # keep only the last-month rows
  # 
  #   # now summarise **only** that month
  #   dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
  #   dplyr::summarise(
  #     onboard_n  = sum(sh_status == "onboard",  na.rm = TRUE),
  #     replace_n  = sum(sh_status == "replace",  na.rm = TRUE),
  #     resign_raw = sum(sh_status == "resign",   na.rm = TRUE),
  # 
  #     `លាឈប់សរុប`          = pmax(0, pmin(2, 2 - (onboard_n + replace_n))),
  #     `លាឈប់ស្រី`           = sum(sh_status == "resign"  & female == 1, na.rm = TRUE),
  #     `ជំនួសសរុប`          = replace_n,
  #     `ជំនួសស្រី`           = sum(sh_status == "replace" & female == 1, na.rm = TRUE),
  #     `បច្ចុប្បន្នភាពសរុប` = onboard_n,
  #     `បច្ចុប្បន្នភាពស្រី`  = sum(sh_status == "onboard" & female == 1, na.rm = TRUE),
  #     `បណ្តុះបណ្តាលសរុប` = sum( training, na.rm = TRUE),
  #     `បណ្តុះបណ្តាលស្រី` = sum( training & female == 1, na.rm = TRUE),
  #     .groups = "drop"
  #   ) %>%
  #   dplyr::select(-onboard_n, -replace_n, -resign_raw)

  supporter_status_wide <- df %>%
    dplyr::filter(!is.na(supporterhelthvillage_id)) %>%
    dplyr::mutate(
      end_date = as.Date(end_date),
      year  = if (!"year"  %in% names(.))  lubridate::year(end_date)  else year,
      month = if (!"month" %in% names(.)) lubridate::month(end_date) else month,
      sh_status = if ("supporterhelthstatus" %in% names(.))
        tolower(trimws(as.character(supporterhelthstatus))) else NA_character_,
      sh_gender = if ("supporterhelthgender" %in% names(.))
        supporterhelthgender else NA_character_,
      female = is_female(sh_gender),
      # keep your existing training flag exactly as-is
      sh_training = if ("training_date" %in% names(.)) {
        ifelse(is.na(training_date), 0, 1)
      } else {
        0
      }
    ) %>%
    dplyr::filter(!is.na(sh_status), sh_status %in% c("resign","replace","onboard")) %>%
    # last-month filter within each keys-group
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::mutate(.last_month = max(month, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::filter(month == .last_month) %>%
    # summarise only that month
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      onboard_n  = sum(sh_status == "onboard",  na.rm = TRUE),
      replace_n  = sum(sh_status == "replace",  na.rm = TRUE),
      resign_raw = sum(sh_status == "resign",   na.rm = TRUE),
      
      # female resigns in this (already filtered) month
      resign_female_n = sum(sh_status == "resign" & female == 1, na.rm = TRUE),
      
      # compute need once (scalar), then use for both Khmer fields
      resign_need = as.integer(pmax(0, pmin(2, 2 - (onboard_n + replace_n)))),
      
      `លាឈប់សរុប` = resign_need,
      `លាឈប់ស្រី` = dplyr::case_when(
        resign_need == 0L ~ 0L,                                   # don't find data
        resign_need == 1L ~ as.integer(resign_female_n >= 1L),    # find by village: 1 if any female resign
        TRUE              ~ as.integer(pmin(2L, resign_female_n)) # if need 2, cap at 2
      ),
      
      `ជំនួសសរុប`          = replace_n,
      `ជំនួសស្រី`           = sum(sh_status == "replace" & female == 1, na.rm = TRUE),
      `បច្ចុប្បន្នភាពសរុប` = onboard_n,
      `បច្ចុប្បន្នភាពស្រី`  = sum(sh_status == "onboard" & female == 1, na.rm = TRUE),
      `បណ្តុះបណ្តាលសរុប` = sum( training, na.rm = TRUE),          # (kept as you wrote)
      `បណ្តុះបណ្តាលស្រី` = sum( training & female == 1, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::select(-onboard_n, -replace_n, -resign_raw, -resign_female_n, -resign_need)


  # join onto your current table built before final
  fix_tranfer2 <- fix_tranfer %>%
    dplyr::left_join(supporter_status_wide, by = keys) %>%
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of(c(
          "លាឈប់សរុប","លាឈប់ស្រី",
          "ជំនួសសរុប","ជំនួសស្រី",
          "បច្ចុប្បន្នភាពសរុប","បច្ចុប្បន្នភាពស្រី",
          # NEW columns
          "បណ្តុះបណ្តាលសរុប","បណ្តុះបណ្តាលស្រី"
        )),
        ~ tidyr::replace_na(.x, 0)
      )
    )
  
  
  # ---- Meetings (formnew8sub5_1): first 6 dates, blanks dropped ----
  # (requires lubridate)
  
  km_digit <- c("១","២","៣","៤","៥","៦")
  meeting_cols <- as.vector(outer(km_digit, c(".គ.គ.ម", ".មិន.គ.គ.ម"), paste0))
  
  meeting_participation <- df %>%
    dplyr::filter(formkey == "formnew8sub5_1") %>%
    dplyr::mutate(
      # normalize raw text
      meeting_raw = trimws(as.character(meetingdate)),
      meeting_raw = dplyr::na_if(meeting_raw, ""),
      meeting_raw = dplyr::na_if(meeting_raw, "NA"),
      meeting_raw = dplyr::na_if(meeting_raw, "N/A"),
      meeting_raw = dplyr::na_if(meeting_raw, "0000-00-00"),
      # robust parse (no errors, just NA when it can't parse)
      meeting_dt  = lubridate::parse_date_time(
        meeting_raw,
        orders = c("Ymd","Y/m/d","Ymd HMS","Y/m/d HMS","dmY","mdY","d-m-Y","m-d-Y"),
        quiet  = TRUE
      ),
      meetingdate = as.Date(meeting_dt),
      indicator_id = trimws(as.character(indicator_id)),
      participant  = readr::parse_number(as.character(participant))
    ) %>%
    # drop rows with missing/invalid meetingdate
    dplyr::filter(!is.na(meetingdate)) %>%
    dplyr::arrange(dplyr::across(dplyr::all_of(keys)), meetingdate) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    # same index for both rows on the same date
    dplyr::mutate(meeting_num = dplyr::dense_rank(meetingdate)) %>%
    dplyr::ungroup() %>%
    dplyr::filter(
      meeting_num <= 6,
      indicator_id %in% c("f8_tbl5_1_1","f8_tbl5_1_2")
    ) %>%
    dplyr::mutate(
      idx_kh  = km_digit[meeting_num],
      col_name = dplyr::case_when(
        indicator_id == "f8_tbl5_1_1" ~ paste0(idx_kh, ".គ.គ.ម"),
        indicator_id == "f8_tbl5_1_2" ~ paste0(idx_kh, ".មិន.គ.គ.ម"),
        TRUE ~ NA_character_
      )
    ) %>%
    dplyr::filter(!is.na(col_name)) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys)), col_name) %>%
    dplyr::summarise(val = sum(participant, na.rm = TRUE), .groups = "drop") %>%
    tidyr::pivot_wider(names_from = col_name, values_from = val, values_fill = 0) %>%
    {
      # ensure all 12 meeting columns exist and are ordered
      miss <- setdiff(meeting_cols, names(.))
      out  <- if (length(miss)) tibble::add_column(., !!!setNames(rep(list(0L), length(miss)), miss), .after = ncol(.)) else .
      out[, c(colnames(out)[!colnames(out) %in% meeting_cols], meeting_cols), drop = FALSE]
    }
  
  # join to your table
  fix_tranfer2 <- fix_tranfer2 %>%
    dplyr::left_join(meeting_participation, by = keys) %>%
    dplyr::mutate(
      dplyr::across(dplyr::any_of(meeting_cols), ~ tidyr::replace_na(.x, 0))
    )
  
  
  # k20 <- df %>%
  #   # dplyr::filter(formkey == "formnew8sub5_1") %>%
  #   dplyr::filter(indicator_id == "f8_tbl5_1_1") %>%
  #   dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
  #   dplyr::summarise(
  #     `សមាជិក សរុប` = sum(participant, na.rm = TRUE),
  #     `សមាជិក ស្រី`     = sum(participantfemale, na.rm = TRUE),
  #     .groups = "drop"
  #   )
  # 
  # k21 <- df %>%
  #   # dplyr::filter(formkey == "formnew8sub5_1") %>%
  #   dplyr::filter(indicator_id == "f8_tbl5_1_2") %>%
  #   dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
  #   dplyr::summarise(
  #     `មិនសមាជិក សរុប` = sum(participant, na.rm = TRUE),
  #     `មិនសមាជិក ស្រី`     = sum(participantfemale, na.rm = TRUE),
  #     .groups = "drop"
  #   )
  
  k22 <- df %>%
    dplyr::filter(formkey == "formnew8sub6_1") %>%
    dplyr::filter(indicator_id == "f8_tbl6_1_1") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `សកម្មភាពទី១ លទ្ធផល`     = sum(total, na.rm = TRUE),
      `សកម្មភាពទី១ សរុប` = sum(participant, na.rm = TRUE),
      `សកម្មភាពទី១ ស្រី`     = sum(participantfemale, na.rm = TRUE),
      .groups = "drop"
    )
  k23 <- df %>%
    dplyr::filter(formkey == "formnew8sub6_1") %>%
    dplyr::filter(indicator_id == "f8_tbl6_1_2") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `សកម្មភាពទី២ លទ្ធផល`     = sum(total, na.rm = TRUE),
      `សកម្មភាពទី២ សរុប` = sum(participant, na.rm = TRUE),
      `សកម្មភាពទី២ ស្រី`     = sum(participantfemale, na.rm = TRUE),
      .groups = "drop"
    )
  k24 <- df %>%
    dplyr::filter(formkey == "formnew8sub6_1") %>%
    dplyr::filter(indicator_id == "f8_tbl6_1_3") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `សកម្មភាពទី៣ លទ្ធផល`     = sum(total, na.rm = TRUE),
      `សកម្មភាពទី៣ សរុប` = sum(participant, na.rm = TRUE),
      `សកម្មភាពទី៣ ស្រី`     = sum(participantfemale, na.rm = TRUE),
      .groups = "drop"
    )
  
  k25 <- df %>%
    dplyr::filter(formkey == "formnew8sub6_1") %>%
    dplyr::filter(indicator_id == "f8_tbl6_1_4") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `សកម្មភាពទី៤ លទ្ធផល`     = sum(total, na.rm = TRUE),
      `សកម្មភាពទី៤ សរុប` = sum(participant, na.rm = TRUE),
      `សកម្មភាពទី៤ ស្រី`     = sum(participantfemale, na.rm = TRUE),
      .groups = "drop"
    )
  
  k26 <- df %>%
    dplyr::filter(formkey == "formnew8sub6_1") %>%
    dplyr::filter(indicator_id == "f8_tbl6_1_5") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `សកម្មភាពទី៥ លទ្ធផល`     = sum(total, na.rm = TRUE),
      `សកម្មភាពទី៥ សរុប` = sum(participant, na.rm = TRUE),
      `សកម្មភាពទី៥ ស្រី`     = sum(participantfemale, na.rm = TRUE),
      .groups = "drop"
    )
  
  k27 <- df %>%
    dplyr::filter(formkey == "formnew8sub6_1") %>%
    dplyr::filter(indicator_id == "f8_tbl6_1_6") %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `សកម្មភាពទី៦ លទ្ធផល`     = sum(total, na.rm = TRUE),
      `សកម្មភាពទី៦ សរុប` = sum(participant, na.rm = TRUE),
      `សកម្មភាពទី៦ ស្រី`     = sum(participantfemale, na.rm = TRUE),
      .groups = "drop"
    )
  
  
  k28 <- df %>%
    dplyr::filter(
      formkey == "formnew8sub6_1",
      indicator_id %in% c("f8_tbl6_1_7", "f8_tbl6_1_8", "f8_tbl6_1_9")
    ) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `សកម្មភាពទីផ្សេងៗ លទ្ធផល` = sum(total, na.rm = TRUE),
      `សកម្មភាពទីផ្សេងៗ សរុប`   = sum(participant, na.rm = TRUE),
      `សកម្មភាពទីផ្សេងៗ ស្រី`    = sum(participantfemale, na.rm = TRUE),
      .groups = "drop"
    )

  
  
  k29 <- df %>%
    filter(
      formkey == "formnew8sub7_1",
      indicator_id == "f8_tbl7_1_1"
    ) %>%
    # Get month as integer (1–12): prefer `month` column; otherwise derive from end_date
    mutate(
      .mm = coalesce(
        suppressWarnings(as.integer(month)),
        month(suppressWarnings(ymd(end_date)))
      )
    ) %>%
    group_by(across(all_of(keys))) %>%
    summarise(
      `១.ក ផែនការឆ្នាំ` = round(sum(if_else(.mm %in% c(1L, 7L), coalesce(semesterplan, 0), 0),
                                    na.rm = TRUE), 2),
      
      `១.ក ឆមាសមុន`     = round(sum(if_else(.mm %in% 1:6,           coalesce(current,      0), 0), na.rm = TRUE), 2), # Jan–Jun
      `១.ក ឆមាសអនុវត្ត` = round(sum(if_else(.mm %in% 7:12,          coalesce(current,      0), 0), na.rm = TRUE), 2), # Jul–Dec
      .groups = "drop"
    )
  
  # k30
  k30 <- df %>%
    filter(
      formkey == "formnew8sub7_1",
      indicator_id == "f8_tbl7_1_2"
    ) %>%
    # Get month as integer (1–12): prefer `month` column; otherwise derive from end_date
    mutate(
      .mm = coalesce(
        suppressWarnings(as.integer(month)),
        month(suppressWarnings(ymd(end_date)))
      )
    ) %>%
    group_by(across(all_of(keys))) %>%
    summarise(
      `១.ខ ផែនការឆ្នាំ` = round(sum(if_else(.mm %in% c(1L, 7L), coalesce(semesterplan, 0), 0),
                                    na.rm = TRUE), 2),
      
      `១.ខ ឆមាសមុន`     = round(sum(if_else(.mm %in% 1:6,           coalesce(current,      0), 0), na.rm = TRUE), 2), # Jan–Jun
      `១.ខ ឆមាសអនុវត្ត` = round(sum(if_else(.mm %in% 7:12,          coalesce(current,      0), 0), na.rm = TRUE), 2), # Jul–Dec
      .groups = "drop"
    )
  # k31
  k31 <- df %>%
    filter(
      formkey == "formnew8sub7_1",
      indicator_id == "f8_tbl7_1_3"
    ) %>%
    # Get month as integer (1–12): prefer `month` column; otherwise derive from end_date
    mutate(
      .mm = coalesce(
        suppressWarnings(as.integer(month)),
        month(suppressWarnings(ymd(end_date)))
      )
    ) %>%
    group_by(across(all_of(keys))) %>%
    summarise(
      `១.គ ផែនការឆ្នាំ` = round(sum(if_else(.mm %in% c(1L, 7L), coalesce(semesterplan, 0), 0),
                                    na.rm = TRUE), 2),
      
      `១.គ ឆមាសមុន`     = round(sum(if_else(.mm %in% 1:6,           coalesce(current,      0), 0), na.rm = TRUE), 2), # Jan–Jun
      `១.គ ឆមាសអនុវត្ត` = round(sum(if_else(.mm %in% 7:12,          coalesce(current,      0), 0), na.rm = TRUE), 2), # Jul–Dec
      .groups = "drop"
    )
  # k32
  
  k32 <- df %>%
    filter(
      formkey == "formnew8sub7_2",
      indicator_id == "f8_tbl7_2_1"
    ) %>%
    # Get month as integer (1–12): prefer `month` column; otherwise derive from end_date
    mutate(
      .mm = coalesce(
        suppressWarnings(as.integer(month)),
        month(suppressWarnings(ymd(end_date)))
      )
    ) %>%
    group_by(across(all_of(keys))) %>%
    summarise(
      `២.ក ផែនការឆ្នាំ` = round(sum(if_else(.mm %in% c(1L, 7L), coalesce(semesterplan, 0), 0),
                                    na.rm = TRUE), 2),
      
      `២.ក ឆមាសមុន`     = round(sum(if_else(.mm %in% 1:6,           coalesce(current,      0), 0), na.rm = TRUE), 2), # Jan–Jun
      `២.ក ឆមាសអនុវត្ត` = round(sum(if_else(.mm %in% 7:12,          coalesce(current,      0), 0), na.rm = TRUE), 2), # Jul–Dec
      .groups = "drop"
    )
  
  
  # k33
  k33 <- df %>%
    filter(
      formkey == "formnew8sub7_2",
      indicator_id == "f8_tbl7_2_2"
    ) %>%
    # Get month as integer (1–12): prefer `month` column; otherwise derive from end_date
    mutate(
      .mm = coalesce(
        suppressWarnings(as.integer(month)),
        month(suppressWarnings(ymd(end_date)))
      )
    ) %>%
    group_by(across(all_of(keys))) %>%
    summarise(
      `២.ខ ផែនការឆ្នាំ` = round(sum(if_else(.mm %in% c(1L, 7L), coalesce(semesterplan, 0), 0),
                                    na.rm = TRUE), 2),
      
      `២.ខ ឆមាសមុន`     = round(sum(if_else(.mm %in% 1:6,           coalesce(current,      0), 0), na.rm = TRUE), 2), # Jan–Jun
      `២.ខ ឆមាសអនុវត្ត` = round(sum(if_else(.mm %in% 7:12,          coalesce(current,      0), 0), na.rm = TRUE), 2), # Jul–Dec
      .groups = "drop"
    )
  
  # join everything
  final <- fix_tranfer2 %>%
    dplyr::left_join(k1, by = keys) %>%
    dplyr::left_join(k2, by = keys) %>%
    dplyr::left_join(k3, by = keys) %>%
    dplyr::left_join(k4, by = keys) %>%
    dplyr::left_join(k5, by = keys) %>%
    dplyr::left_join(k6, by = keys) %>%
    dplyr::left_join(k7, by = keys) %>%
    dplyr::left_join(k8, by = keys) %>%
    dplyr::left_join(k9, by = keys) %>%
    dplyr::left_join(k10, by = keys) %>%
    dplyr::left_join(k11, by = keys) %>%
    dplyr::left_join(k12, by = keys) %>%
    dplyr::left_join(k13, by = keys) %>%
    dplyr::left_join(k14, by = keys) %>%
    dplyr::left_join(k15, by = keys) %>%
    dplyr::left_join(k16, by = keys) %>%
    dplyr::left_join(k17, by = keys) %>%
    dplyr::left_join(k18, by = keys) %>%
    dplyr::left_join(k19, by = keys) %>%
    # dplyr::left_join(k20, by = keys) %>%
    # dplyr::left_join(k21, by = keys) %>%
    dplyr::left_join(k22, by = keys) %>%
    dplyr::left_join(k23, by = keys) %>%
    dplyr::left_join(k24, by = keys) %>%
    dplyr::left_join(k25, by = keys) %>%
    dplyr::left_join(k26, by = keys) %>%
    dplyr::left_join(k27, by = keys) %>%
    dplyr::left_join(k28, by = keys) %>%
    dplyr::left_join(k29, by = keys) %>%
    dplyr::left_join(k30, by = keys) %>%
    dplyr::left_join(k31, by = keys) %>%
    dplyr::left_join(k32, by = keys) %>%
    dplyr::left_join(k33, by = keys) %>%
    
    
    dplyr::mutate(
      dplyr::across(
        c(
          `កញ្ចប់ទី១_លទ្ធផល`, `កញ្ចប់ទី១_ស.ផ`,
          `កញ្ចប់ទី២_លទ្ធផល`, `កញ្ចប់ទី២_ស.ក`,
          `កញ្ចប់ទី៣_លទ្ធផល`, `កញ្ចប់ទី៣_កុ<២ឆ្នាំ`,
          `កញ្ចប់ទី៤_លទ្ធផល`, `កញ្ចប់ទី៤_កុ<២ឆ្នាំ`,
          `កញ្ចប់ទី៥_លទ្ធផល`, `កញ្ចប់ទី៥_ស.ផ`, `កញ្ចប់ទី៥_ស.ក`, `កញ្ចប់ទី៥_មា/អា`, `កញ្ចប់ទី៥_កុ<២ឆ្នាំ`,
          `កញ្ចប់ទី៦_លទ្ធផល`, `កញ្ចប់ទី៦_ស.ក`, `កញ្ចប់ទី៦_មា/អា`, `កញ្ចប់ទី៦_កុ<២ឆ្នាំ`,
          `កញ្ចប់ទី៧_លទ្ធផល`, `កញ្ចប់ទី៧_ស.ក`, `កញ្ចប់ទី៧_មា/អា`,
          `កញ្ចប់ទី៨_លទ្ធផល`, `កញ្ចប់ទី៨_ស.ផ`, `កញ្ចប់ទី៨_ស.ក`, `កញ្ចប់ទី៨_មា/អា`,
          `កញ្ចប់ទី៩_លទ្ធផល`,
          `ខកញ្ចប់ទី១_លទ្ធផល`, `ខកញ្ចប់ទី១_ស.ផ`,
          `ខកញ្ចប់ទី២_លទ្ធផល`, `ខកញ្ចប់ទី២_ស.ក`,`ខកញ្ចប់ទី២_មា/អា`,
          `ខកញ្ចប់ទី៣_លទ្ធផល`, `ខកញ្ចប់ទី៣_កុ<២ឆ្នាំ`,
          `ខកញ្ចប់ទី៤_លទ្ធផល`, `ខកញ្ចប់ទី៤_កុ<២ឆ្នាំ`,
          `ខកញ្ចប់ទី៥_លទ្ធផល`, `ខកញ្ចប់ទី៥_ស.ផ`, `ខកញ្ចប់ទី៥_ស.ក`, `ខកញ្ចប់ទី៥_មា/អា`, `ខកញ្ចប់ទី៥_កុ<២ឆ្នាំ`,
          `ខកញ្ចប់ទី៦_លទ្ធផល`, `ខកញ្ចប់ទី៦_ស.ក`, `ខកញ្ចប់ទី៦_មា/អា`, `ខកញ្ចប់ទី៦_កុ<២ឆ្នាំ`,
          `ខកញ្ចប់ទី៧_លទ្ធផល`, `ខកញ្ចប់ទី៧_ស.ក`, `ខកញ្ចប់ទី៧_មា/អា`,
          `ខកញ្ចប់ទី៨_លទ្ធផល`, `ខកញ្ចប់ទី៨_ស.ផ`, `ខកញ្ចប់ទី៨_ស.ក`, `ខកញ្ចប់ទី៨_មា/អា`,
          
          
          `ឃុំ_លទ្ធផល` ,
          `ឃុំ_ស.ផ`,
          `ឃុំ_មា/អា`,
          `ឃុំ_កុ<២ឆ្នាំ`,
          `ឃុំ_អ្នកពាក់ព័ន្ធ`,
          
          
          `មណ្ឌលសុខភាព_លទ្ធផល`,
          `មណ្ឌលសុខភាព_ស.ផ`,
          `មណ្ឌលសុខភាព_មា/អា`,
          `មណ្ឌលសុខភាព_កុ<២ឆ្នាំ`,
          `មណ្ឌលសុខភាព_អ្នកពាក់ព័ន្ធ`,
          # `សមាជិក សរុប`,
          # `សមាជិក ស្រី`,
          # `មិនសមាជិក សរុប`,
          # `មិនសមាជិក ស្រី`,
          # 
          `សកម្មភាពទី១ លទ្ធផល`,`សកម្មភាពទី១ សរុប`,`សកម្មភាពទី១ ស្រី`,
          `សកម្មភាពទី២ លទ្ធផល`,`សកម្មភាពទី២ សរុប`,`សកម្មភាពទី២ ស្រី`,
          `សកម្មភាពទី៣ លទ្ធផល`,`សកម្មភាពទី៣ សរុប`,`សកម្មភាពទី៣ ស្រី`,
          `សកម្មភាពទី៤ លទ្ធផល`,`សកម្មភាពទី៤ សរុប`,`សកម្មភាពទី៤ ស្រី`,
          `សកម្មភាពទី៥ លទ្ធផល`,`សកម្មភាពទី៥ សរុប`,`សកម្មភាពទី៥ ស្រី`,
          `សកម្មភាពទី៦ លទ្ធផល`,`សកម្មភាពទី៦ សរុប`,`សកម្មភាពទី៦ ស្រី`,
          # # NEW: Others
          # `សកម្មភាពទីផ្សេងៗ លទ្ធផល`,`សកម្មភាពទីផ្សេងៗ សរុប`,`សកម្មភាពទីផ្សេងៗ`
        ),
        ~ tidyr::replace_na(.x, 0)
      )
    ) %>%
    dplyr::arrange(province_kh, district_kh, commune_kh, year) %>%
    dplyr::transmute(
      `កូដខេត្ត`          = province_id,
      `ឈ្មោះខេត្ត`        = province_kh,
      `កូដក្រុង ស្រុក`     = district_id,
      `ឈ្មោះក្រុង ស្រុក`  = district_kh,
      `កូដឃុំ សង្កាត់`     = commune_id,
      `ឈ្មោះឃុំ សង្កាត់`  = commune_kh,
      
      # Sub1 totals
      `គ្រួសារ`           = `គ្រួសារ`,
      `ស.ផ`               = `ស.ផ`,
      `ស.ក`               = `ស.ក`,
      `កុ<២ឆ្នាំ`          = `កុ<២ឆ្នាំ`,
      `ទា.សំ.ណ`          = `ទា.សំ.ណ`,

      # Sub1 indigenous totals
      `គ្រួសារ_ដើមភាគតិច`   = `គ្រួសារ_ដើមភាគតិច`,
      `ស.ផ_ដើមភាគតិច`       = `ស.ផ_ដើមភាគតិច`,
      `ស.ក_ដើមភាគតិច`       = `ស.ក_ដើមភាគតិច`,
      `កុ<២ឆ្នាំ_ដើមភាគតិច`  = `កុ<២ឆ្នាំ_ដើមភាគតិច`,
      `ទា.សំ.ណ_ដើមភាគតិច`  = `ទា.សំ.ណ_ដើមភាគតិច`,

      # Sub2 additions
      `កញ្ចប់ទី១_លទ្ធផល`,
      `កញ្ចប់ទី១_ស.ផ`,
      `កញ្ចប់ទី២_លទ្ធផល`,
      `កញ្ចប់ទី២_ស.ក`,
      `កញ្ចប់ទី៣_លទ្ធផល`,
      `កញ្ចប់ទី៣_កុ<២ឆ្នាំ`,
      `កញ្ចប់ទី៤_លទ្ធផល`,
      `កញ្ចប់ទី៤_កុ<២ឆ្នាំ`,
      `កញ្ចប់ទី៥_លទ្ធផល`,
      `កញ្ចប់ទី៥_ស.ផ`,
      `កញ្ចប់ទី៥_ស.ក`,
      `កញ្ចប់ទី៥_មា/អា`,
      `កញ្ចប់ទី៥_កុ<២ឆ្នាំ`,
      `កញ្ចប់ទី៦_លទ្ធផល`,
      `កញ្ចប់ទី៦_ស.ក`,
      `កញ្ចប់ទី៦_មា/អា`,
      `កញ្ចប់ទី៦_កុ<២ឆ្នាំ`,
      `កញ្ចប់ទី៧_លទ្ធផល`,
      `កញ្ចប់ទី៧_ស.ក`,
      `កញ្ចប់ទី៧_មា/អា`,
      `កញ្ចប់ទី៨_លទ្ធផល`,
      `កញ្ចប់ទី៨_ស.ផ`,
      `កញ្ចប់ទី៨_ស.ក`,
      `កញ្ចប់ទី៨_មា/អា`,
      `កញ្ចប់ទី៩_លទ្ធផល`,


      `ខកញ្ចប់ទី១_លទ្ធផល`,
      `ខកញ្ចប់ទី១_ស.ផ`,
      `ខកញ្ចប់ទី២_លទ្ធផល`,
      `ខកញ្ចប់ទី២_ស.ក`,
      `ខកញ្ចប់ទី២_មា/អា`,
      `ខកញ្ចប់ទី៣_លទ្ធផល`,
      `ខកញ្ចប់ទី៣_កុ<២ឆ្នាំ`,
      `ខកញ្ចប់ទី៤_លទ្ធផល`,
      `ខកញ្ចប់ទី៤_កុ<២ឆ្នាំ`,
      `ខកញ្ចប់ទី៥_លទ្ធផល`,
      `ខកញ្ចប់ទី៥_ស.ផ`,
      `ខកញ្ចប់ទី៥_ស.ក`,
      `ខកញ្ចប់ទី៥_មា/អា`,
      `ខកញ្ចប់ទី៥_កុ<២ឆ្នាំ`,
      `ខកញ្ចប់ទី៦_លទ្ធផល`,
      `ខកញ្ចប់ទី៦_ស.ក`,
      `ខកញ្ចប់ទី៦_មា/អា`,
      `ខកញ្ចប់ទី៦_កុ<២ឆ្នាំ`,
      `ខកញ្ចប់ទី៧_លទ្ធផល`,
      `ខកញ្ចប់ទី៧_ស.ក`,
      `ខកញ្ចប់ទី៧_មា/អា`,
      `ខកញ្ចប់ទី៨_លទ្ធផល`,
      `ខកញ្ចប់ទី៨_ស.ផ`,
      `ខកញ្ចប់ទី៨_ស.ក`,
      `ខកញ្ចប់ទី៨_មា/អា`,

      `ឃុំ_លទ្ធផល` ,
      `ឃុំ_ស.ផ`,
      `ឃុំ_មា/អា`,
      `ឃុំ_កុ<២ឆ្នាំ`,
      `ឃុំ_អ្នកពាក់ព័ន្ធ`,


      `មណ្ឌលសុខភាព_លទ្ធផល`,
      `មណ្ឌលសុខភាព_ស.ផ`,
      `មណ្ឌលសុខភាព_មា/អា`,
      `មណ្ឌលសុខភាព_កុ<២ឆ្នាំ`,
      `មណ្ឌលសុខភាព_អ្នកពាក់ព័ន្ធ`,

      `សរុប`,
      `ស្ត្រី`,
      `ពិការភាព`,
      `អ្នកក្រីក្រ`,
      `ជនជាតិ.តិច`,


      # << NEW: satisfaction by type
      `ប្រភេទ ក`, `ប្រភេទ ខ`, `ប្រភេទ គ`, `ប្រភេទ ឃ`,
      `មិនប្រភេទ ក`, `មិនប្រភេទ ខ`, `មិនប្រភេទ គ`, `មិនប្រភេទ ឃ`,

      `ដោះស្រាយ_ប្រភេទ ក`,
      `ដោះស្រាយ_ប្រភេទ ខ`,
      `ដោះស្រាយ_ប្រភេទ គ`,
      `ដោះស្រាយ_ប្រភេទ ឃ`,

      `ដោះស្រាយ_ប្រភេទបញ្ជូន ក`,
      `ដោះស្រាយ_ប្រភេទបញ្ជូន ខ`,
      `ដោះស្រាយ_ប្រភេទបញ្ជូន គ`,
      `ដោះស្រាយ_ប្រភេទបញ្ជូន ឃ`,

      `លាឈប់សរុប`,
      `លាឈប់ស្រី`,
      `ជំនួសសរុប`,
      `ជំនួសស្រី`,
      `បច្ចុប្បន្នភាពសរុប`,
      `បច្ចុប្បន្នភាពស្រី`,
      `បណ្តុះបណ្តាលសរុប`,`បណ្តុះបណ្តាលស្រី`,

      `១.គ.គ.ម`, `១.មិន.គ.គ.ម`,
      `២.គ.គ.ម`, `២.មិន.គ.គ.ម`,
      `៣.គ.គ.ម`, `៣.មិន.គ.គ.ម`,
      `៤.គ.គ.ម`, `៤.មិន.គ.គ.ម`,
      `៥.គ.គ.ម`, `៥.មិន.គ.គ.ម`,
      `៦.គ.គ.ម`, `៦.មិន.គ.គ.ម`,


      `សកម្មភាពទី១ លទ្ធផល`,
      `សកម្មភាពទី១ សរុប`,
      `សកម្មភាពទី១ ស្រី`,

      `សកម្មភាពទី២ លទ្ធផល`,
      `សកម្មភាពទី២ សរុប`,
      `សកម្មភាពទី២ ស្រី`,


      `សកម្មភាពទី៣ លទ្ធផល`,
      `សកម្មភាពទី៣ សរុប`,
      `សកម្មភាពទី៣ ស្រី`,


      `សកម្មភាពទី៤ លទ្ធផល`,
      `សកម្មភាពទី៤ សរុប`,
      `សកម្មភាពទី៤ ស្រី`,

      `សកម្មភាពទី៥ លទ្ធផល`,
      `សកម្មភាពទី៥ សរុប`,
      `សកម្មភាពទី៥ ស្រី`,

      `សកម្មភាពទី៦ លទ្ធផល`,
      `សកម្មភាពទី៦ សរុប`,
      `សកម្មភាពទី៦ ស្រី`,

      `សកម្មភាពទីផ្សេងៗ លទ្ធផល`,
      `សកម្មភាពទីផ្សេងៗ សរុប`,
      `សកម្មភាពទីផ្សេងៗ ស្រី`,

      
      `១.ក ផែនការឆ្នាំ` ,
      `១.ក ឆមាសមុន` ,
      `១.ក ឆមាសអនុវត្ត`,
      
      `១.ខ ផែនការឆ្នាំ` , 
      `១.ខ ឆមាសមុន` ,
      `១.ខ ឆមាសអនុវត្ត`,
      
      `១.គ ផែនការឆ្នាំ` , 
      `១.គ ឆមាសមុន` ,
      `១.គ ឆមាសអនុវត្ត`,
      
      `២.ក ផែនការឆ្នាំ` , 
      `២.ក ឆមាសមុន` ,
      `២.ក ឆមាសអនុវត្ត`,
      
      `២.ខ ផែនការឆ្នាំ` , 
      `២.ខ ឆមាសមុន` ,
      `២.ខ ឆមាសអនុវត្ត`,
      
    )
  
  final
})