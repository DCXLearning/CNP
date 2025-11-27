# ==================== server.R (scoped by form level, light) ====================
library(shiny)
library(shinymanager)
library(DBI)
library(RMariaDB)
library(pool)
library(DT)
library(dplyr)
library(tidyr)
library(lubridate)
library(openxlsx2)
library(htmltools)

server <- function(input, output, session) {
  
  # ---------- 1. Secure login ----------
  # my_check(), fetch_user_by_identifier(), `%||%`, and pool `con`
  # are defined in global.R
  auth <- shinymanager::secure_server(check_credentials = my_check)
  
  # ---------- 2. Logged-in user profile ----------
  user_profile <- reactive({
    req(auth$user)
    fetch_user_by_identifier(auth$user) %>% dplyr::slice_head(n = 1)
  })
  
  # ---------- 5.3 Scope IDs from users table ----------
  # 0 or NA  => no restriction for that level
  scope_ids <- reactive({
    prof <- req(user_profile())
    
    get_id <- function(col) {
      if (!col %in% names(prof)) return(NA_integer_)
      val <- suppressWarnings(as.integer(prof[[col]][1]))
      if (is.na(val) || val == 0L) NA_integer_ else val
    }
    
    ids <- list(
      province_id = get_id("province_id"),
      district_id = get_id("district_id"),
      commune_id  = get_id("commune_id")
    )
    
    message(sprintf(
      "User scope IDs -> province:%s district:%s commune:%s",
      ids$province_id, ids$district_id, ids$commune_id
    ))
    
    ids
  })
  
  # ---------- 3. Scope IDs from users table ----------
  # 0 or NA  => no restriction for that level
  scope_ids <- reactive({
    prof <- req(user_profile())
    
    get_id <- function(col) {
      if (!col %in% names(prof)) return(NA_integer_)
      val <- suppressWarnings(as.integer(prof[[col]][1]))
      if (is.na(val) || val == 0L) NA_integer_ else val
    }
    
    ids <- list(
      province_id = get_id("province_id"),
      district_id = get_id("district_id"),
      commune_id  = get_id("commune_id")
    )
    
    message(sprintf(
      "User scope IDs -> province:%s district:%s commune:%s",
      ids$province_id, ids$district_id, ids$commune_id
    ))
    
    ids
  })
  
  # ---------- 4. Who am I (top-right) ----------
  output$whoami <- renderUI({
    prof <- req(user_profile())
    fullname <- prof$fullname[1] %||% prof$name[1]
    role     <- prof$permission_title[1] %||% "User"
    
    tags$div(
      class = "whoami",
      sprintf("សួស្តី %s (%s)", fullname, role),
      tags$a(
        href = "#",
        "ចាកចេញ",
        onclick = "Shiny.setInputValue('shinymanager_logout', Date.now()); return false;"
      )
    )
  })
  
  # ---------- 5.4 Scope helper (applied to any form df) ----------
  apply_location_scope <- function(df) {
    if (is.null(df) || !nrow(df)) return(df)
    
    ids <- scope_ids()
    message("---- apply_location_scope() BEFORE: ", nrow(df), " rows")
    
    # Province filter (if user has province_id)
    if ("province_id" %in% names(df) && !is.na(ids$province_id)) {
      df <- df %>% dplyr::filter(.data$province_id == ids$province_id)
      message("  filter province_id = ", ids$province_id, " -> ", nrow(df), " rows")
    }
    
    # District filter (if user has district_id)
    if ("district_id" %in% names(df) && !is.na(ids$district_id)) {
      df <- df %>% dplyr::filter(.data$district_id == ids$district_id)
      message("  filter district_id = ", ids$district_id, " -> ", nrow(df), " rows")
    }
    
    # Commune filter (if user has commune_id)
    if ("commune_id" %in% names(df) && !is.na(ids$commune_id)) {
      df <- df %>% dplyr::filter(.data$commune_id == ids$commune_id)
      message("  filter commune_id = ", ids$commune_id, " -> ", nrow(df), " rows")
    }
    
    message("---- apply_location_scope() AFTER: ", nrow(df), " rows")
    df
  }
  
  
  source(file.path("R", "Form", "Form7.R"), local = TRUE, encoding = "UTF-8")
  source(file.path("R", "Form", "Form8.R"), local = TRUE, encoding = "UTF-8")
  source(file.path("R", "Form", "Form9.R"), local = TRUE, encoding = "UTF-8")
  source(file.path("R", "Form", "Form10.R"), local = TRUE, encoding = "UTF-8")
  source(file.path("R", "Form", "Form11.R"), local = TRUE, encoding = "UTF-8")
  source(file.path("R", "Form", "Form12.R"), local = TRUE, encoding = "UTF-8")
  source(file.path("R", "Form", "Form13.R"), local = TRUE, encoding = "UTF-8")
  source(file.path("R", "Form", "Form14.R"), local = TRUE, encoding = "UTF-8")
  source(file.path("R", "Form", "Form15.R"), local = TRUE, encoding = "UTF-8")
  
  output$form7_sub1_summary <- renderDT({
    df <- sub1_summary()
    
    dt <- datatable(
      df,
      rownames = FALSE,
      class = "kh-dt display compact stripe hover row-border order-column",
      caption = htmltools::tags$caption(
        class = "khmer-caption",
        "ទម្រង់ទី៧៖​ សម្រាប់បំពេញរបាយការណ៌ភូមិ"
      ),
      options = list(
        scrollX    = TRUE,
        pageLength = 15,
        lengthMenu = c(15, 25, 50, 100),
        order      = list(),        # keep your current order
        dom        = "lfrtip",
        language   = list(
          sProcessing = "កំពុងដំណើរការ...",
          sLengthMenu = "បង្ហាញ _MENU_ ជួរ",
          sZeroRecords = "មិនមានទិន្នន័យ",
          sInfo = "បង្ហាញ _START_ - _END_ ក្នុងចំណោម _TOTAL_ ជួរ",
          sInfoEmpty = "មិនមានទិន្នន័យ",
          sInfoFiltered = "(ច្រោះពីចំនួនសរុប _MAX_ ជួរ)",
          sSearch = "ស្វែងរក:",
          oPaginate = list(
            sFirst="ដំបូង", sPrevious="‹", sNext="›", sLast="ចុងក្រោយ"
          )
        )
      )
    )
  })
  #=========================Form7+8===============================
  output$from7_8_summary_table <- renderDT({
    df <- from7_8_summary()
    
    dt <- datatable(
      df,
      rownames = FALSE,
      class = "kh-dt display compact stripe hover row-border order-column",
      caption = htmltools::tags$caption(
        class = "khmer-caption",
        "ទម្រង់ទី៨៖ ទម្រង់សម្រាប់បំពេញរបាយការណ៌ប្រចាំខែឃុំសង្កាត់ភូមិ"
      ),
      options = list(
        scrollX    = TRUE,
        pageLength = 15,
        lengthMenu = c(15, 25, 50, 100),
        order      = list(),        # keep your current order
        dom        = "lfrtip",
        language   = list(
          sProcessing = "កំពុងដំណើរការ...",
          sLengthMenu = "បង្ហាញ _MENU_ ជួរ",
          sZeroRecords = "មិនមានទិន្នន័យ",
          sInfo = "បង្ហាញ _START_ - _END_ ក្នុងចំណោម _TOTAL_ ជួរ",
          sInfoEmpty = "មិនមានទិន្នន័យ",
          sInfoFiltered = "(ច្រោះពីចំនួនសរុប _MAX_ ជួរ)",
          sSearch = "ស្វែងរក:",
          oPaginate = list(
            sFirst="ដំបូង", sPrevious="‹", sNext="›", sLast="ចុងក្រោយ"
          )
        )
      )
    )
  })
  
  
  
  
  output$from9_summary_table <- renderDT({
    df <- from9_summary()
    
    dt <- datatable(
      df,
      rownames = FALSE,
      class = "kh-dt display compact stripe hover row-border order-column",
      caption = htmltools::tags$caption(    
        class = "khmer-caption",
        "ទម្រង់ទី៩៖ ទម្រង់សម្រាប់បំពេញរបាយការណ៌ប្រចាំឆ្នាំឃុំសង្កាត់ "
      ),
      options = list(
        scrollX    = TRUE,
        pageLength = 15,
        lengthMenu = c(15, 25, 50, 100),
        order      = list(),        # keep your current order
        dom        = "lfrtip",
        language   = list(
          sProcessing = "កំពុងដំណើរការ...",
          sLengthMenu = "បង្ហាញ _MENU_ ជួរ",
          sZeroRecords = "មិនមានទិន្នន័យ",
          sInfo = "បង្ហាញ _START_ - _END_ ក្នុងចំណោម _TOTAL_ ជួរ",
          sInfoEmpty = "មិនមានទិន្នន័យ",
          sInfoFiltered = "(ច្រោះពីចំនួនសរុប _MAX_ ជួរ)",
          sSearch = "ស្វែងរក:",
          oPaginate = list(
            sFirst="ដំបូង", sPrevious="‹", sNext="›", sLast="ចុងក្រោយ"
          )
        )
      )
    )
  })
  
  
  
  
  
  
  
  output$from10_summary_table <- renderDT({
    df <- from10_summary()
    
    dt <- datatable(
      df,
      rownames = FALSE,
      class = "kh-dt display compact stripe hover row-border order-column",
      caption = htmltools::tags$caption(
        class = "khmer-caption",
        "ទម្រង់ទី១០៖ ទម្រង់សម្រាប់បំពេញរបាយការណ៌ប្រចាំខែក្រុងស្រុក  "
      ),
      options = list(
        scrollX    = TRUE,
        pageLength = 15,
        lengthMenu = c(15, 25, 50, 100),
        order      = list(),        # keep your current order
        dom        = "lfrtip",
        language   = list(
          sProcessing = "កំពុងដំណើរការ...",
          sLengthMenu = "បង្ហាញ _MENU_ ជួរ",
          sZeroRecords = "មិនមានទិន្នន័យ",
          sInfo = "បង្ហាញ _START_ - _END_ ក្នុងចំណោម _TOTAL_ ជួរ",
          sInfoEmpty = "មិនមានទិន្នន័យ",
          sInfoFiltered = "(ច្រោះពីចំនួនសរុប _MAX_ ជួរ)",
          sSearch = "ស្វែងរក:",
          oPaginate = list(
            sFirst="ដំបូង", sPrevious="‹", sNext="›", sLast="ចុងក្រោយ"
          )
        )
      )
    )
  })
  
  
  output$from11_summary_table <- renderDT({
    df <- from11_summary()
    
    dt <- datatable(
      df,
      rownames = FALSE,
      class = "kh-dt display compact stripe hover row-border order-column",
      caption = htmltools::tags$caption(
        class = "khmer-caption",
        "ទម្រង់ទី១១៖ ទម្រង់សម្រាប់បំពេញរបាយការណ៌ប្រចាំឆមាសក្រុងស្រុក "
      ),
      options = list(
        scrollX    = TRUE,
        pageLength = 15,
        lengthMenu = c(15, 25, 50, 100),
        order      = list(),        # keep your current order
        dom        = "lfrtip",
        language   = list(
          sProcessing = "កំពុងដំណើរការ...",
          sLengthMenu = "បង្ហាញ _MENU_ ជួរ",
          sZeroRecords = "មិនមានទិន្នន័យ",
          sInfo = "បង្ហាញ _START_ - _END_ ក្នុងចំណោម _TOTAL_ ជួរ",
          sInfoEmpty = "មិនមានទិន្នន័យ",
          sInfoFiltered = "(ច្រោះពីចំនួនសរុប _MAX_ ជួរ)",
          sSearch = "ស្វែងរក:",
          oPaginate = list(
            sFirst="ដំបូង", sPrevious="‹", sNext="›", sLast="ចុងក្រោយ"
          )
        )
      )
    )
  })
  
  
  output$from12_summary_table <- renderDT({
    df <- from12_summary()
    
    dt <- datatable(
      df,
      rownames = FALSE,
      class = "kh-dt display compact stripe hover row-border order-column",
      caption = htmltools::tags$caption(
        class = "khmer-caption",
        "ទម្រង់ទី១២៖ ទម្រង់សម្រាប់បំពេញរបាយការណ៌ប្រចាំឆ្នាំក្រុងស្រុក  "
      ),
      options = list(
        scrollX    = TRUE,
        pageLength = 15,
        lengthMenu = c(15, 25, 50, 100),
        order      = list(),        # keep your current order
        dom        = "lfrtip",
        language   = list(
          sProcessing = "កំពុងដំណើរការ...",
          sLengthMenu = "បង្ហាញ _MENU_ ជួរ",
          sZeroRecords = "មិនមានទិន្នន័យ",
          sInfo = "បង្ហាញ _START_ - _END_ ក្នុងចំណោម _TOTAL_ ជួរ",
          sInfoEmpty = "មិនមានទិន្នន័យ",
          sInfoFiltered = "(ច្រោះពីចំនួនសរុប _MAX_ ជួរ)",
          sSearch = "ស្វែងរក:",
          oPaginate = list(
            sFirst="ដំបូង", sPrevious="‹", sNext="›", sLast="ចុងក្រោយ"
          )
        )
      )
    )
  })
  
  
  
  output$from13_summary_table <- renderDT({
    df <- from13_summary()
    
    dt <- datatable(
      df,
      rownames = FALSE,
      class = "kh-dt display compact stripe hover row-border order-column",
      caption = htmltools::tags$caption(
        class = "khmer-caption",
        " ទម្រង់ទី១៣៖ ទម្រង់សម្រាប់បំពេញរបាយការណ៌ប្រចាំខែខេត្ត "
      ),
      options = list(
        scrollX    = TRUE,
        pageLength = 15,
        lengthMenu = c(15, 25, 50, 100),
        order      = list(),        # keep your current order
        dom        = "lfrtip",
        language   = list(
          sProcessing = "កំពុងដំណើរការ...",
          sLengthMenu = "បង្ហាញ _MENU_ ជួរ",
          sZeroRecords = "មិនមានទិន្នន័យ",
          sInfo = "បង្ហាញ _START_ - _END_ ក្នុងចំណោម _TOTAL_ ជួរ",
          sInfoEmpty = "មិនមានទិន្នន័យ",
          sInfoFiltered = "(ច្រោះពីចំនួនសរុប _MAX_ ជួរ)",
          sSearch = "ស្វែងរក:",
          oPaginate = list(
            sFirst="ដំបូង", sPrevious="‹", sNext="›", sLast="ចុងក្រោយ"
          )
        )
      )
    )
  })
  
  output$from14_summary_table <- renderDT({
    df <- from14_summary()
    
    dt <- datatable(
      df,
      rownames = FALSE,
      class = "kh-dt display compact stripe hover row-border order-column",
      caption = htmltools::tags$caption(
        class = "khmer-caption",
        "ទម្រង់ទី១៤៖ ទម្រង់សម្រាប់បំពេញរបាយការណ៌ប្រចាំឆមាសខេត្ត "
      ),
      options = list(
        scrollX    = TRUE,
        pageLength = 15,
        lengthMenu = c(15, 25, 50, 100),
        order      = list(),        # keep your current order
        dom        = "lfrtip",
        language   = list(
          sProcessing = "កំពុងដំណើរការ...",
          sLengthMenu = "បង្ហាញ _MENU_ ជួរ",
          sZeroRecords = "មិនមានទិន្នន័យ",
          sInfo = "បង្ហាញ _START_ - _END_ ក្នុងចំណោម _TOTAL_ ជួរ",
          sInfoEmpty = "មិនមានទិន្នន័យ",
          sInfoFiltered = "(ច្រោះពីចំនួនសរុប _MAX_ ជួរ)",
          sSearch = "ស្វែងរក:",
          oPaginate = list(
            sFirst="ដំបូង", sPrevious="‹", sNext="›", sLast="ចុងក្រោយ"
          )
        )
      )
    )
  })
  
  output$from15_summary_table <- renderDT({
    df <- from15_summary()
    
    dt <- datatable(
      df,
      rownames = FALSE,
      class = "kh-dt display compact stripe hover row-border order-column",
      caption = htmltools::tags$caption(
        class = "khmer-caption",
        "ទម្រង់ទី១៥៖ ទម្រង់សម្រាប់បំពេញរបាយការណ៌ប្រចាំឆ្នាំខេត្ត"
      ),
      options = list(
        scrollX    = TRUE,
        pageLength = 15,
        lengthMenu = c(15, 25, 50, 100),
        order      = list(),        # keep your current order
        dom        = "lfrtip",
        language   = list(
          sProcessing = "កំពុងដំណើរការ...",
          sLengthMenu = "បង្ហាញ _MENU_ ជួរ",
          sZeroRecords = "មិនមានទិន្នន័យ",
          sInfo = "បង្ហាញ _START_ - _END_ ក្នុងចំណោម _TOTAL_ ជួរ",
          sInfoEmpty = "មិនមានទិន្នន័យ",
          sInfoFiltered = "(ច្រោះពីចំនួនសរុប _MAX_ ជួរ)",
          sSearch = "ស្វែងរក:",
          oPaginate = list(
            sFirst="ដំបូង", sPrevious="‹", sNext="›", sLast="ចុងក្រោយ"
          )
        )
      )
    )
  })
  
  loc_view <- reactive({
    validate(need(exists("master7_with_kh"),
                  "No data found — build master7_with_kh in global.R"))
    
    df <- master7_with_kh
    
    # apply filters if not "All"
    if (!is.null(input$flt_province) && input$flt_province != "All")
      df <- df %>% filter(province_kh == input$flt_province)
    if (!is.null(input$flt_district) && input$flt_district != "All")
      df <- df %>% filter(district_kh == input$flt_district)
    if (!is.null(input$flt_commune) && input$flt_commune != "All")
      df <- df %>% filter(commune_kh == input$flt_commune)
    if (!is.null(input$flt_village) && input$flt_village != "All")
      df <- df %>% filter(village_kh == input$flt_village)
    if (!is.null(input$flt_year) && input$flt_year != "All")
      df <- df %>% filter(year == as.integer(input$flt_year))
    if (!is.null(input$flt_month) && input$flt_month != "All")
      df <- df %>% filter(month == as.integer(input$flt_month))
    
    # keep one row per location + date (your de-dup rule)
    df %>%
      distinct(province_kh, district_kh, commune_kh, village_kh,
               start_date, end_date, .keep_all = TRUE) %>%
      transmute(
        `លេខកូដ`       = id,
        `ខេត្ត`         = province_kh,
        `ក្រុង ស្រុក`  = district_kh,
        `ឃុំ សង្កាត់`  = commune_kh,
        `ភូមិ`         = village_kh,
        `ចាប់ពីថ្ងៃ`    = format(as.Date(start_date), "%d-%m-%Y"),
        `ដល់ថ្ងៃ`       = format(as.Date(end_date),   "%d-%m-%Y")
      ) %>%
      arrange(`ខេត្ត`, `ក្រុង ស្រុក`, `ឃុំ សង្កាត់`, `ភូមិ`, `ចាប់ពីថ្ងៃ`, `ដល់ថ្ងៃ`)
  })
  
  
  
  output$tbl_locations <- renderDT({
    df <- loc_view()
    
    # auto-format Date columns to dd-mm-YYYY (like your screenshot)
    date_cols <- which(sapply(df, inherits, what = "Date"))
    
    dt <- datatable(
      df,
      rownames = FALSE,
      class = "kh-dt display compact stripe hover row-border order-column",
      caption = htmltools::tags$caption(
        class = "khmer-caption",
        "ទម្រង់ទី៧៖​ សម្រាប់បំពេញរបាយការណ៌ភូមិ"
      ),
      options = list(
        scrollX    = TRUE,
        pageLength = 15,
        lengthMenu = c(15, 25, 50, 100),
        order      = list(),        # keep your current order
        dom        = "lfrtip",
        language   = list(
          sProcessing = "កំពុងដំណើរការ...",
          sLengthMenu = "បង្ហាញ _MENU_ ជួរ",
          sZeroRecords = "មិនមានទិន្នន័យ",
          sInfo = "បង្ហាញ _START_ - _END_ ក្នុងចំណោម _TOTAL_ ជួរ",
          sInfoEmpty = "មិនមានទិន្នន័យ",
          sInfoFiltered = "(ច្រោះពីចំនួនសរុប _MAX_ ជួរ)",
          sSearch = "ស្វែងរក:",
          oPaginate = list(
            sFirst="ដំបូង", sPrevious="‹", sNext="›", sLast="ចុងក្រោយ"
          )
        )
      )
    )
    
    # apply date format if we have date columns
    if (length(date_cols) > 0) {
      dt <- DT::formatDate(dt, columns = date_cols, method = "toLocaleDateString",
                           params = list('en-GB'))  # dd/mm/yyyy; switch to custom if you prefer
    }
    
    dt
  })
  
  #================================== Filter Block ====================================
  
  # Khmer label for "All"
  km_all_label <- "ទាំងអស់"
  
  # helper: build choices with Khmer labels; the first entry shows Khmer but has value "All"
  with_all_kh <- function(values, labels = values) {
    c(setNames("All", km_all_label), setNames(values, labels))
  }
  
  # --------- INITIALIZE PROVINCE / YEAR / MONTH IN KHMER ON LOAD ----------
  observeEvent(TRUE, {
    # Province choices (Khmer)
    p <- master7_with_kh %>%
      distinct(province_kh) %>%
      filter(!is.na(province_kh)) %>%
      arrange(province_kh) %>%
      pull(province_kh)
    
    updateSelectInput(session, "flt_province",
                      choices = with_all_kh(p), selected = "All")
    
    # Year choices
    yrs <- master7_with_kh %>%
      distinct(year) %>% filter(!is.na(year)) %>%
      arrange(year) %>% pull(year)
    updateSelectInput(session, "flt_year",
                      choices = c(setNames("All", km_all_label), setNames(as.character(yrs), as.character(yrs))),
                      selected = "All")
    
    # Month choices (Khmer month names)
    km_month_names <- c("មករា","កុម្ភៈ","មិនា","មេសា","ឧសភា","មិថុនា",
                        "កក្កដា","សីហា","កញ្ញា","តុលា","វិច្ឆិកា","ធ្នូ")
    mths <- master7_with_kh %>%
      distinct(month) %>% filter(!is.na(month)) %>%
      arrange(month) %>% pull(month)
    updateSelectInput(session, "flt_month",
                      choices = c(setNames("All", km_all_label),
                                  setNames(as.character(mths), km_month_names[mths])),
                      selected = "All")
  }, once = TRUE)
  
  # --------- Province → District (labels & values in Khmer) ----------
  observeEvent(input$flt_province, {
    df <- master7_with_kh
    if (!is.null(input$flt_province) && input$flt_province != "All")
      df <- df %>% filter(province_kh == input$flt_province)
    
    d <- df %>% distinct(district_kh) %>%
      filter(!is.na(district_kh)) %>% arrange(district_kh) %>% pull(district_kh)
    
    updateSelectInput(session, "flt_district",
                      choices = with_all_kh(d), selected = "All")
    
    # reset downstream
    updateSelectInput(session, "flt_commune", choices = setNames("All", km_all_label), selected = "All")
    updateSelectInput(session, "flt_village", choices = setNames("All", km_all_label), selected = "All")
  })
  
  # --------- District → Commune ----------
  observeEvent(input$flt_district, {
    df <- master7_with_kh
    if (!is.null(input$flt_province) && input$flt_province != "All")
      df <- df %>% filter(province_kh == input$flt_province)
    if (!is.null(input$flt_district) && input$flt_district != "All")
      df <- df %>% filter(district_kh == input$flt_district)
    
    d <- df %>% distinct(commune_kh) %>%
      filter(!is.na(commune_kh)) %>% arrange(commune_kh) %>% pull(commune_kh)
    
    updateSelectInput(session, "flt_commune",
                      choices = with_all_kh(d), selected = "All")
    
    updateSelectInput(session, "flt_village", choices = setNames("All", km_all_label), selected = "All")
  })
  
  # --------- Commune → Village ----------
  observeEvent(input$flt_commune, {
    df <- master7_with_kh
    if (!is.null(input$flt_province) && input$flt_province != "All")
      df <- df %>% filter(province_kh == input$flt_province)
    if (!is.null(input$flt_district) && input$flt_district != "All")
      df <- df %>% filter(district_kh == input$flt_district)
    if (!is.null(input$flt_commune) && input$flt_commune != "All")
      df <- df %>% filter(commune_kh == input$flt_commune)
    
    d <- df %>% distinct(village_kh) %>%
      filter(!is.na(village_kh)) %>% arrange(village_kh) %>% pull(village_kh)
    
    updateSelectInput(session, "flt_village",
                      choices = with_all_kh(d), selected = "All")
  })
  
  # --------- Any location change → Year/Month (still Khmer labels) ----------
  observeEvent(list(input$flt_province, input$flt_district, input$flt_commune, input$flt_village ), {
    df <- master7_with_kh
    if (!is.null(input$flt_province) && input$flt_province != "All")
      df <- df %>% filter(province_kh == input$flt_province)
    if (!is.null(input$flt_district) && input$flt_district != "All")
      df <- df %>% filter(district_kh == input$flt_district)
    if (!is.null(input$flt_commune) && input$flt_commune != "All")
      df <- df %>% filter(commune_kh == input$flt_commune)
    if (!is.null(input$flt_village) && input$flt_village != "All")
      df <- df %>% filter(village_kh == input$flt_village)
    
    yrs  <- df %>% distinct(year) %>% filter(!is.na(year)) %>% arrange(year) %>% pull(year)
    sems <- df %>% distinct(semester) %>% filter(!is.na(semester)) %>% arrange(semester) %>% pull(semester)
    mths <- df %>% distinct(month)%>% filter(!is.na(month))%>% arrange(month)%>% pull(month)
    
    updateSelectInput(session, "flt_year",
                      choices = c(setNames("All", km_all_label), setNames(as.character(yrs), as.character(yrs))),
                      selected = "All")
    
    km_semester_names <- c("ឆមាសទី ១", "ឆមាសទី ២")
    updateSelectInput(
      session, "flt_semester",
      choices = c(setNames("All", km_all_label),
                  setNames(as.character(sems), km_semester_names[sems])),
      selected = "All"
    )
    
    km_month_names <- c("មករា","កុម្ភៈ","មិនា","មេសា","ឧសភា","មិថុនា",
                        "កក្កដា","សីហា","កញ្ញា","តុលា","វិច្ឆិកា","ធ្នូ")
    updateSelectInput(session, "flt_month",
                      choices = c(setNames("All", km_all_label),
                                  setNames(as.character(mths), km_month_names[mths])),
                      selected = "All")
  })
  
  output$downloadReport <- downloadHandler(
    filename = function() {
      ts  <- format(as.POSIXct(Sys.time(), tz = "Asia/Phnom_Penh"), "%Y-%m-%d_%H-%M")
      sel <- req(input$sheet_to_export)  # "Village" or "Commune_Monthly"
      sprintf("%s_%s.xlsx", sel, ts)
    },
    content = function(file) {
      to_chr <- function(df) {
        df[] <- lapply(df, function(x) {
          if (inherits(x, "Date"))        format(x, "%Y-%m-%d")
          else if (inherits(x, "POSIXt")) format(x, "%Y-%m-%d %H:%M:%S")
          else as.character(x)
        })
        df
      }
      
      sel <- req(input$sheet_to_export)
      
      # Build ONLY what's needed
      x <- switch(
        sel,
        "Village" = to_chr(as.data.frame(sub1_summary(),    stringsAsFactors = FALSE, check.names = FALSE)),
        "Commune_Monthly" = to_chr(as.data.frame(from7_8_summary(), stringsAsFactors = FALSE, check.names = FALSE)),
        "Commune_Annual" = to_chr(as.data.frame(from9_summary(), stringsAsFactors = FALSE, check.names = FALSE)),
        "District_Monthly" = to_chr(as.data.frame(from10_summary(), stringsAsFactors = FALSE, check.names = FALSE)),
        "District_Semester" = to_chr(as.data.frame(from11_summary(), stringsAsFactors = FALSE, check.names = FALSE)),
        "District_Annual" = to_chr(as.data.frame(from12_summary(), stringsAsFactors = FALSE, check.names = FALSE)),
        "Province_Monthly" = to_chr(as.data.frame(from13_summary(), stringsAsFactors = FALSE, check.names = FALSE)),
        "Province_Semester" = to_chr(as.data.frame(from14_summary(), stringsAsFactors = FALSE, check.names = FALSE)),
        "Province_Annual" = to_chr(as.data.frame(from15_summary(), stringsAsFactors = FALSE, check.names = FALSE)),
        stop("Unknown sheet selection: ", sel)
      )
      
      template_path <- "Report/Data Sheet Format_2_JUNE_2025.xlsx"
      if (!file.exists(template_path)) stop("Template not found: ", template_path)
      
      wb <- openxlsx2::wb_load(template_path)
      
      # Ensure selected sheet exists
      if (!sel %in% wb$get_sheet_names()) {
        wb$add_worksheet(sel)
      }

      # Write ONLY to the selected sheet (A5), no headers
      if (nrow(x) > 0 && ncol(x) > 0) {
        wb$add_data(
          sheet    = sel,
          x        = x,
          startCol = 1,   # A
          startRow = 5,   # 5
          colNames = FALSE
        )
      }

      

      # ----- IMPORTANT: Remove all other sheets so the export has ONLY the selected sheet -----
      other_sheets <- setdiff(wb$get_sheet_names(), sel)
      if (length(other_sheets) > 0) {
        for (s in other_sheets) {
          # openxlsx2 supports this workbook method:
          wb$remove_worksheet(sheet = s)
        }
      }
      
      # Save to the output file
      openxlsx2::wb_save(wb, file, overwrite = TRUE)
    },
    contentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
  )
  
  
  
  
  # in server(...)
  observeEvent(input$reset_filters, ignoreInit = TRUE, {
    # If your selects are plain selectInput
    updateSelectInput(session, "flt_province", selected = "All")
    updateSelectInput(session, "flt_district", selected = "All")
    updateSelectInput(session, "flt_commune",  selected = "All")
    # If village uses server-side selectize:
    # updateSelectizeInput(session, "flt_village", selected = "All", server = TRUE)
    updateSelectInput(session, "flt_village",  selected = "All")  # <- use this if not selectize
    
    updateSelectInput(session, "flt_year",     selected = "All")
    updateSelectInput(session, "flt_month",    selected = "All")
    updateSelectInput(session, "flt_quarter",    selected = "All")
    
  })
  
  
  output$excel_download_btn <- renderUI({
    div(style = "width:100%;",
        downloadButton(
          outputId = "downloadReport",
          label = HTML(
            # '<span class="dl-icon"><i class="fa fa-file-excel-o" aria-hidden="true"></i></span>
            '<span class="dl-text">ទាញចេញ</span>'
          ),
          class = "btn btn-dl btn-block"
        )
    )
  })
}
# ==================== end server.R ====================
