# source("global.R")
# source("R/Server.R")
sub1_summary <- reactive({
  validate(need(exists("form7_all_with_loc"),
                "form7_all_with_loc not found (build it in global.R)"))
  
  df <- form7_all_with_loc %>%
    dplyr::mutate(
      start_date = as.Date(start_date),
      end_date   = as.Date(end_date),
      year  = if (!"year"  %in% names(.))  lubridate::year(end_date)  else year,
      month = if (!"month" %in% names(.)) lubridate::month(end_date) else month
    )
  
  # ---- apply filters ----
  if (!is.null(input$flt_province) && input$flt_province != "All")
    df <- df %>% dplyr::filter(province_kh == input$flt_province)
  if (!is.null(input$flt_district) && input$flt_district != "All")
    df <- df %>% dplyr::filter(district_kh == input$flt_district)
  if (!is.null(input$flt_commune) && input$flt_commune != "All")
    df <- df %>% dplyr::filter(commune_kh == input$flt_commune)
  if (!is.null(input$flt_village) && input$flt_village != "All")
    df <- df %>% dplyr::filter(village_kh == input$flt_village)
  if (!is.null(input$flt_year) && input$flt_year != "All")
    df <- df %>% dplyr::filter(year == as.integer(input$flt_year))
  if (!is.null(input$flt_month) && input$flt_month != "All")
    df <- df %>% dplyr::filter(month == as.integer(input$flt_month))
  
  keys <- c("province_id","province_kh",
            "district_id","district_kh",
            "commune_id","commune_kh",
            "village_id","village_kh",
            "year","month"
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
  
  
  # # mapping for the 3 columns
  # ind_map_sub3 <- tibble::tibble(
  #   indicator_id = c("f7_tbl3_2","f7_tbl3_3","f7_tbl3_4"),
  #   ind_kh       = c("ពិការភាព","អ្នកក្រីក្រ","ជនជាតិ.តិច")
  # )
  # 
  # # --- build base_sub3 robustly ---
  # base_sub3 <- df %>%
  #   dplyr::filter(formkey == "formnew7sub3") %>%
  #   dplyr::mutate(
  #     # normalize keys & numeric fields
  #     indicator_id = trimws(as.character(indicator_id)),
  #     # if you have readr, parse_number handles commas/strings; fallback to as.numeric if not
  #     total        = readr::parse_number(as.character(total)),
  #     total_girl   = readr::parse_number(as.character(total_girl))
  #   ) %>%
  #   dplyr::left_join(ind_map_sub3, by = "indicator_id")
  # 
  # # --- (optional) sanity check: any IDs not mapped? show them in the console ---
  # unmapped_sub3 <- base_sub3 %>%
  #   dplyr::filter(is.na(ind_kh)) %>%
  #   dplyr::count(indicator_id, sort = TRUE)
  # if (nrow(unmapped_sub3) > 0) {
  #   message("formnew7sub3: unmapped indicator_id values:\n", paste(capture.output(print(unmapped_sub3)), collapse = "\n"))
  # }
  # 
  # # totals per location
  # sub3_tot <- base_sub3 %>%
  #   dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
  #   dplyr::summarise(
  #     `សរុប`  = sum(total, na.rm = TRUE),
  #     `ស្ត្រី` = sum(total_girl, na.rm = TRUE),
  #     .groups  = "drop"
  #   )
  # 
  # # pivot the three indicator-specific totals
  # sub3_ind <- base_sub3 %>%
  #   dplyr::filter(!is.na(ind_kh)) %>%
  #   dplyr::group_by(dplyr::across(dplyr::all_of(keys)), ind_kh) %>%
  #   dplyr::summarise(val = sum(total, na.rm = TRUE), .groups = "drop") %>%
  #   tidyr::pivot_wider(names_from = ind_kh, values_from = val, values_fill = 0)
  
  ind_map_sub3 <- tibble::tibble(
    indicator_id = c("f7_tbl3_2","f7_tbl3_3","f7_tbl3_4"),
    ind_kh       = c("ពិការភាព","អ្នកក្រីក្រ","ជនជាតិ.តិច")
  )
  
  base_sub3 <- df %>%
    dplyr::filter(formkey == "formnew7sub3") %>%     # keep your current formkey gate
    dplyr::mutate(
      indicator_id = trimws(as.character(indicator_id)),
      total        = readr::parse_number(as.character(total)),
      total_girl   = readr::parse_number(as.character(total_girl)),
      number       = readr::parse_number(as.character(number))  # <-- use `number`
    ) %>%
    dplyr::left_join(ind_map_sub3, by = "indicator_id")
  
  # optional sanity check
  unmapped_sub3 <- base_sub3 %>%
    dplyr::filter(is.na(ind_kh)) %>%
    dplyr::count(indicator_id, sort = TRUE)
  if (nrow(unmapped_sub3) > 0) {
    message("form7 sub3: unmapped indicator_id values:\n",
            paste(capture.output(print(unmapped_sub3)), collapse = "\n"))
  }
  
  # Totals per location (still from total/total_girl)
  sub3_tot <- base_sub3 %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys))) %>%
    dplyr::summarise(
      `សរុប`  = sum(total, na.rm = TRUE),
      `ស្ត្រី` = sum(total_girl, na.rm = TRUE),
      .groups  = "drop"
    )
  
  # Per-type values from `number` (sum)
  sub3_ind <- base_sub3 %>%
    dplyr::filter(indicator_id %in% ind_map_sub3$indicator_id, !is.na(ind_kh)) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys)), ind_kh) %>%
    dplyr::summarise(val = sum(number, na.rm = TRUE), .groups = "drop") %>%  # <-- sum(number)
    tidyr::pivot_wider(
      names_from  = ind_kh,
      values_from = val,
      values_fill = 0
    )
  
  # join onto Sub1 (as you intended)
  sub3_wide <- sub1_wide %>%
    dplyr::left_join(sub3_tot, by = keys) %>%
    dplyr::left_join(sub3_ind, by = keys) %>%
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of(c("សរុប","ស្ត្រី","ពិការភាព","អ្នកក្រីក្រ","ជនជាតិ.តិច")),
        ~ tidyr::replace_na(.x, 0)
      )
    )
  
  # --- keep your existing satisfied-by-type block ---
  sub3_types_wide <- df %>%
    dplyr::filter(formkey %in% c("formnew7sub3_1","formnew7sub3_2","formnew7sub3_3","formnew7sub3_4")) %>%
    dplyr::mutate(
      type_kh = dplyr::recode(
        formkey,
        "formnew7sub3_1" = "ប្រភេទ ក",
        "formnew7sub3_2" = "ប្រភេទ ខ",
        "formnew7sub3_3" = "ប្រភេទ គ",
        "formnew7sub3_4" = "ប្រភេទ ឃ"
      ),
      .satisfied_num = suppressWarnings(as.numeric(satisfied))
    ) %>%
    dplyr::mutate(.satisfied_num = dplyr::coalesce(.satisfied_num, 0)) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys)), type_kh) %>%
    dplyr::summarise(val = sum(.satisfied_num, na.rm = TRUE), .groups = "drop") %>%
    tidyr::pivot_wider(
      names_from  = type_kh,
      values_from = val,
      values_fill = 0
    )
  
  # --- NEW: dissatisfied-by-type block (mirrors the above) ---
  sub3_types_dissat_wide <- df %>%
    dplyr::filter(formkey %in% c("formnew7sub3_1","formnew7sub3_2","formnew7sub3_3","formnew7sub3_4")) %>%
    dplyr::mutate(
      type_kh_minh = dplyr::recode(
        formkey,
        "formnew7sub3_1" = "មិនប្រភេទ ក",
        "formnew7sub3_2" = "មិនប្រភេទ ខ",
        "formnew7sub3_3" = "មិនប្រភេទ គ",
        "formnew7sub3_4" = "មិនប្រភេទ ឃ"
      ),
      .dissat_num = suppressWarnings(as.numeric(dissatisfied))
    ) %>%
    dplyr::mutate(.dissat_num = dplyr::coalesce(.dissat_num, 0)) %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(keys)), type_kh_minh) %>%
    dplyr::summarise(val = sum(.dissat_num, na.rm = TRUE), .groups = "drop") %>%
    tidyr::pivot_wider(
      names_from  = type_kh_minh,
      values_from = val,
      values_fill = 0
    )
  
  # --- UPDATE: include both satisfied + dissatisfied, then join sub3_wide ---
  sub1_wide_total <- sub3_types_wide %>%
    dplyr::left_join(sub3_wide, by = keys) %>%
    dplyr::left_join(sub3_types_dissat_wide, by = keys) %>%
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of(c("ប្រភេទ ក","ប្រភេទ ខ","ប្រភេទ គ","ប្រភេទ ឃ",
                        "មិនប្រភេទ ក","មិនប្រភេទ ខ","មិនប្រភេទ គ","មិនប្រភេទ ឃ")),
        ~ tidyr::replace_na(.x, 0)
      )
    )
  
  
  
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
  
  # join everything
  final <- sub1_wide_total %>%
    dplyr::left_join(k1, by = keys) %>%
    dplyr::left_join(k2, by = keys) %>%
    dplyr::left_join(k3, by = keys) %>%
    dplyr::left_join(k4, by = keys) %>%
    dplyr::left_join(k5, by = keys) %>%
    dplyr::left_join(k6, by = keys) %>%
    dplyr::left_join(k7, by = keys) %>%
    dplyr::left_join(k8, by = keys) %>%
    dplyr::left_join(k9, by = keys) %>%
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
          `កញ្ចប់ទី៩_លទ្ធផល`
        ),
        ~ tidyr::replace_na(.x, 0)
      )
    ) %>%
    dplyr::arrange(province_kh, district_kh, commune_kh, village_kh,, year, month) %>%
    dplyr::transmute(
      `កូដខេត្ត`          = province_id,
      `ឈ្មោះខេត្ត`        = province_kh,
      `កូដក្រុង ស្រុក`     = district_id,
      `ឈ្មោះក្រុង ស្រុក`  = district_kh,
      `កូដឃុំ សង្កាត់`     = commune_id,
      `ឈ្មោះឃុំ សង្កាត់`  = commune_kh,
      `កូដភូមិ`          = village_id,
      `ឈ្មោះភូមិ`       = village_kh,
      
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
      
      `សរុប`,
      `ស្ត្រី`,
      `ពិការភាព`,
      `អ្នកក្រីក្រ`,
      `ជនជាតិ.តិច`,
      
      # << NEW: satisfaction by type
      `ប្រភេទ ក`, `ប្រភេទ ខ`, `ប្រភេទ គ`, `ប្រភេទ ឃ`,
      `មិនប្រភេទ ក`, `មិនប្រភេទ ខ`, `មិនប្រភេទ គ`, `មិនប្រភេទ ឃ`
      
    )
  
  final
})