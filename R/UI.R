library(shiny)
library(shinyjs)
library(DT)
library(shinymanager)
# library(shinycssloaders)  # not needed anymore if you don't want spinners

# ------------------ Base UI (wrapped later by secure_app) ------------------
base_ui <- fluidPage(
  useShinyjs(),
  
  # ---------- Styles ----------
  tags$head(tags$style(HTML("
    @font-face { font-family: 'Khmer OS Battambang';
      src: url('fonts/KhmerOSBattambang.ttf') format('truetype'); font-display: swap; }
    @font-face { font-family: 'Khmer OS Muol Light';
      src: url('fonts/KhmerOSMuolLight.ttf') format('truetype'); font-display: swap; }

    body { background:#f5f7f9; }
    .kh-ui, .kh-ui label, .kh-ui .selectize-input, .kh-ui .form-control,
    .kh-ui .btn, .kh-ui .dataTables_filter label, .kh-ui .dataTables_length label {
      font-family: 'Khmer OS Battambang','Noto Sans Khmer','Khmer OS',sans-serif;
      letter-spacing: .1px;
    }
    .kh-title { font-family:'Khmer OS Muol Light',sans-serif; font-size:22px; margin:0 0 6px 0; }

    .kh-card { background:#fff; border:1px solid #e5e9ef; border-radius:10px; padding:14px; }

    .filters-grid {
      display:grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap:12px 14px;
      align-items:end;
    }
    @media (min-width: 1200px) {
      .filters-grid { grid-template-columns: repeat(6, minmax(180px, 1fr)); }
    }

    .kh-ui .control-label, .kh-ui label { 
      font-size:13px; color:#334155; margin-bottom:6px; font-weight:600;
    }

    .kh-ui .selectize-control {
      border: 0 !important; background: transparent !important;
      box-shadow: none !important; padding: 0 !important;
    }
    .kh-ui .selectize-control.single .selectize-input {
      height:40px; min-height:40px; padding:8px 12px;
      border:1px solid #e5e9ef; border-radius:8px; box-shadow:none; background:#fff;
    }
    .kh-ui .selectize-input.focus {
      border-color:#0ea5e9; box-shadow:0 0 0 3px rgba(14,165,233,.15);
    }
    .kh-ui .selectize-dropdown { border:1px solid #e5e9ef; border-radius:8px; }

    .kh-ui select.form-control, .kh-ui select.form-select {
      height:40px; padding:8px 12px; border:1px solid #e5e9ef; border-radius:8px; box-shadow:none; background:#fff;
    }
    .kh-ui select.form-control:focus, .kh-ui select.form-select:focus {
      border-color:#0ea5e9; box-shadow:0 0 0 3px rgba(14,165,233,.15);
    }

    .kh-ui .btn { height:40px; border-radius:8px; font-weight:700; }
    .kh-ui .btn-success { background:#22c55e; border-color:#22c55e; color:#fff; }
    .kh-ui .btn-success:hover { filter:brightness(.95); }
    .kh-ui .btn-reset { background:#94a3b8; border-color:#94a3b8; color:#fff; }
    .kh-ui .btn-reset:hover { filter:brightness(.95); }
    .kh-ui .btn-block { width:100%; display:block; }

    .filters-grid .grid-btn { align-self: stretch; display:flex; }
    .filters-grid .grid-btn > .btn-dl {
      width: 200px; height: 250px; padding: 0;
      display: flex; align-items: center; justify-content: center; flex-direction: column;
    }

    .btn-dl {
      display:flex; flex-direction:column; align-items:center; justify-content:center;
      gap:6px;
      width:140px; height:72px;
      background:#fff !important;
      border:2px solid #22c55e !important;
      color:#16a34a !important;
      border-radius:10px;
      font-weight:700;
      box-shadow:none;
    }
    .btn-dl:hover, .btn-dl:focus {
      background:#f0fff4 !important;
      color:#15803d !important;
      border-color:#16a34a !important;
    }

    .btn-dl .dl-icon { font-size:26px; line-height:1; }
    .btn-dl .dl-text { font-size:16px; margin-top:2px; }

    .btn-dl.btn-block{ width: 150px;}
    .whoami { text-align:right; padding:10px 6px 0 6px; color:#334155; }
  "))),
  
  # ---------- App Body ----------
  div(class = "kh-ui",
      br(), br(),
      
      fluidRow(
        column(8, h4(class = "kh-title", "ទិន្នន័យ ស្រុក/ខេត្តជាប់ពាក់​ព័ន្ធ")),
        column(4, uiOutput("whoami"))
      ),
      
      div(class = "kh-card",
          div(class = "filters-grid",
              selectizeInput("flt_province", "ខេត្ត",
                             choices = province_choices, selected = "All",
                             options = list(placeholder="ជ្រើសរើស")),
              selectizeInput("flt_district", "ក្រុង ស្រុក",
                             choices = district_choices, selected = "All",
                             options = list(placeholder="ជ្រើសរើស")),
              selectizeInput("flt_commune", "ឃុំ សង្កាត់",
                             choices = commune_choices, selected = "All",
                             options = list(placeholder="ជ្រើសរើស")),
              selectizeInput("flt_village", "ភូមិ",
                             choices = village_choices, selected = "All",
                             options = list(placeholder="ជ្រើសរើស")),
              selectInput("flt_year",  "ឆ្នាំ", choices = year_choices,    selected = "All"),
              selectInput("flt_semester", "ឆមាស", choices = semester_choices, selected = "All"),
              selectInput("flt_month","ខែ",  choices = month_choices,     selected = "All"),
              
              div(class = "grid-btn",
                  actionButton("reset_filters", label = " កំណត់ឡើងវិញ",
                               icon = icon("rotate-right"),
                               class = "btn btn-reset")
              ),
              div(class = "grid-btn", uiOutput("excel_download_btn")),
              
              selectInput(
                inputId = "sheet_to_export",
                label   = "Which sheet to export?",
                choices = c(
                  "",
                  "Village",
                  "Commune_Monthly",
                  "Commune_Annual",   # <- remove Form 9 for now
                  "District_Monthly",
                  "District_Semester",
                  "District_Annual",
                  "Province_Monthly",
                  "Province_Semester",
                  "Province_Annual"
                ),
                selected = ""
              )
          )
      ),
      
      br(),
      # div(class = "kh-card", DTOutput("form7_sub1_summary")),
      # br(),
      # div(class = "kh-card", DTOutput("from7_8_summary_table")),
      # br(),
      # div(class = "kh-card", DTOutput("from9_summary_table")),
      # br(),
      # div(class = "kh-card", DTOutput("from10_summary_table")),
      # br(),
      # div(class = "kh-card", DTOutput("from11_summary_table")),
      # br(),
      # div(class = "kh-card", DTOutput("from12_summary_table")),
      br(),
      div(class = "kh-card", DTOutput("from13_summary_table")),
      br(),
      div(class = "kh-card", DTOutput("from14_summary_table")),
      br(),
      div(class = "kh-card", DTOutput("from15_summary_table"))
  )
)

# ----------- Secure wrapper (login page) -----------
ui <- shinymanager::secure_app(
  base_ui,
  enable_admin = FALSE,
  language = "en",
  tags_top = div(tags$h4("Hello (: ", style = "margin:0;")),
  timeout = 0
)
