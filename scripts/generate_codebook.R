library(qualtRics)
library(tidyverse)
library(sjlabelled)
library(stringr)

daily <- fetch_survey("SV_9tSCJIEm6mf2ezA", include_display_order = FALSE)
intake <- fetch_survey("SV_3L7EVwomBZvCKma", include_display_order = FALSE)

intake_codeboook <- intake |>
  select(-c(StartDate:consent), -c(gpsAuthUrl:verifiedPS)) |>
  select(RANDOM_ID, everything()) |>
  select(-ends_with("_TEXT")) |>
  get_label() |>
  enframe() |>
  mutate(value = str_remove(value, "- Selected Choice ")) |>
  separate(value, into = c("stem", "item"), sep = " - ", extra = "drop", fill = "right", remove = FALSE) |>
  mutate(
    Item = if_else(is.na(item), value, item),
    `Stem text` = if_else(grepl(" - ", value), stem, NA),
    .keep = "unused"
  ) |>
  mutate(
    Source = case_when(
      grepl("wemwbs", name) ~ "Warwick-Edinburgh Mental Wellbeing Scale (https://doi.org/10.1186/1477-7525-5-63)",
      grepl("promis", name) ~ "PROMIS Short Form 8a Adult Depression Scale (https://doi.org/10.1177/1073191111411667)",
      grepl("bangs", name) ~ "Basic Needs in Games Scale (https://doi.org/10.31234/osf.io/4965z7)",
      grepl("trojan", name) ~ "Trojan Player Typology (https://doi.org/10.1016/j.chb.2015.03.018)",
      grepl("gdt", name) ~ "Gaming Disorder Test (https://doi.org/10.1007/s11469-019-00088-z)",
      grepl("mctq", name) ~ "Munich Chronotype Questionnaire (https://doi.org/10.1177/0748730402239679)",
      grepl("pqsi", name) ~ "Pittsburgh Sleep Quality Index (https://doi.org/10.1016/0165-1781(89)90047-4)",
      grepl("eps", name) ~ "Epwoth Sleepiness Scale (https://doi.org/10.1093/sleep/14.6.540)",
      grepl("BFI", name) ~ "Extra-short Big Five Inventory–2 (https://doi.org/10.1016/j.jrp.2017.02.004)",
      grepl("lifeSat", name) ~ "Cantril Self-anchoring Scale (Cantril, 1965)",
      TRUE ~ ""
    ),
    `Response Options` = case_when(
      grepl("age", name) ~ "18-99",
      grepl("RANDOM_ID", name) ~ "Unique 10-digit numeric identifier",
      grepl("gender", name) ~ "Man; Woman; Non-binary; Prefer to specify; Prefer not to say",
      grepl("wemwbs", name) ~ "1 - None of the time; 2 - Rarely; 3 - Some of the time; 4 - Often; 5 - All of the time",
      grepl("promis", name) ~ "Never; Rarely; Sometimes; Often; Never",
      grepl("bangs", name) ~ "1 Strongly Disagree; 2; 3; 4 Neither Agree nor Disagree; 5; 6; 7 Strongly Agree",
      grepl("trojan", name) ~ "1 - Strongly disagree; 2; 3; 4; 5 - Strongly agree",
      name %in% c("problematicPlay", "positives") ~ "free text response",
      grepl("displacement", name) ~ "Greatly interfered; Moderately interfered; Slightly interfered; No impact; Slightly supported;
Moderately supported; Greatly supported",
      grepl("timeUse", name) ~ "slider from 0-16 hours, with increments of .1",
      grepl("eps", name) ~ "No chance of dozing; Slight chance of dozing; Moderate chance of dozing; High chance of dozing",
      grepl("BFI", name) ~ "Disagree strongly; Disagree a little; Neutral, no opinion; Agree a little; Agree strongly",
      grepl("playProp", name) ~ "1-100 sliding scale",
      grepl("platforms_", name) ~ "Yes/No",
      grepl("isWilling", name) ~ "Yes/No",
      grepl("location", name) ~ "Yes/No",
      grepl("selfreport", name) ~ "Numeric entry",
      TRUE ~ "Open text response"
    ),
    `Stem text` = gsub("[\r\n]", "", `Stem text`)
  ) |>
  rename(Variable = name)

daily_codebook <- daily |>
  select(-c(StartDate:Q_RelevantIDLastStartDate), -c(EnrollmentDate:CompletionRecord), -c(tudArray:DIARY_URL), -c(bpnsfs_7:bpnsfs_12)) |>
  select(-ends_with("_TEXT"), -contains(c("Page Submit", "First Click", "Last Click", "Click Count"))) |>
  select(RANDOM_ID, Day, everything()) |>
  get_label() |>
  enframe() |>
  mutate(value = str_remove(value, "- Selected Choice ")) |>
  separate(value, into = c("stem", "item"), sep = " - ", extra = "drop", fill = "right", remove = FALSE) |>
  mutate(
    Item = if_else(is.na(item), value, item),
    `Stem text` = if_else(grepl(" - ", value), stem, NA),
    .keep = "unused"
  ) |>
  mutate(
    Source = case_when(
      grepl("wemwbs", name) ~ "Warwick-Edinburgh Mental Wellbeing Scale (https://doi.org/10.1186/1477-7525-5-63)",
      grepl("bangs", name) ~ "Basic Needs in Games Scale (https://doi.org/10.31234/osf.io/4965z7)",
      grepl("bpnsfs", name) ~ "Basic Psychological Need Satisfaction and Frustration Scale - Daily Edition (https://doi.org/10.1080/15295192.2018.1444131)",
      grepl("stress", name) ~ "Daily Inventory of Stressful Events (https://doi.org/10.1177/1073191102091006)",
      grepl("Stress", name) ~ "Daily Inventory of Stressful Events (https://doi.org/10.1177/1073191102091006)",
      grepl("lifeSat", name) ~ "Cantril Self-anchoring Scale (Cantril, 1965)",
      grepl("CSD", name) ~ "Consensus Sleep Diary (https://doi.org/10.5665/sleep.1642)",
      grepl("KSD", name) ~ "Karolinska Sleep Diary (https://doi.org/10.2466/pms.1994.79.1.287)",
      TRUE ~ ""
    ),
    `Response Options` = case_when(
      grepl("age", name) ~ "18-99",
      grepl("ethnicity", name) ~ "White; Mixed or multiple ethnic groups; Asian or Asian British; Black, African, Caribbean or Black British; Other ethnic group; Prefer not to say",
      grepl("politicalParty", name) ~ "Labour Party / Co-operative Party; Conservative and Unionist Party; Liberal Democrats; Scottish National Party (SNP); Sinn Féin; Reform UK; Democratic Unionist Party (DUP); Green Party of England and Wales; Plaid Cymru; Other (please specify); No affiliation / Independent; Prefer not to say",
      grepl("RANDOM_ID", name) ~ "Unique 10-digit numeric identifier",
      grepl("gender", name) ~ "Man; Woman; Non-binary; Prefer to specify; Prefer not to say",
      grepl("wemwbs", name) ~ "1 - None of the time; 2 - Rarely; 3 - Some of the time; 4 - Often; 5 - All of the time",
      grepl("bangs", name) ~ "1 Strongly Disagree; 2; 3; 4 Neither Agree nor Disagree; 5; 6; 7 Strongly Agree",
      grepl("bpnsfs", name) ~ "1 Strongly Disagree; 2; 3; 4 Neither Agree nor Disagree; 5; 6; 7 Strongly Agree",
      name %in% c("problematicPlay", "positives") ~ "free text response",
      grepl("displacement", name) ~ "Greatly interfered; Moderately interfered; Slightly interfered; No impact; Slightly supported; Moderately supported; Greatly supported",
      grepl("playProp", name) ~ "1-100 sliding scale",
      grepl("neuroIden", name) ~ "Yes/No",
      grepl("neuroDiag", name) ~ "Yes/No",
      grepl("isWilling", name) ~ "Yes/No",
      grepl("actiwatch", name) ~ "Yes/No",
      grepl("stressEvents", name) ~ "Yes/No",
      grepl("hadStress", name) ~ "Yes/No",
      grepl("howStressful", name) ~ "Not at all; not very; somewhat; very",
      grepl("androidSubmission", name) ~ "File upload",
      grepl("iosScreenshot", name) ~ "File upload",
      grepl("selfreport", name) ~ "Numeric entry",
      grepl("sd_0", name) ~ "Regular work day; Regular day off; Weekend; Holiday; Vacation day; Other (please specify)",
      grepl("CSD", name) ~ "HH:MM",
      grepl("KSD", name) ~ "1 (worst); 2; 3; 4; 5 (worst)",
      grepl("intervention", name) ~ "Yes/No",
      grepl("24h", name) ~ "Yes/No",
      grepl("marital", name) ~ "Single, never married; Married; In a domestic partnership or civil union; Separated; Divorced; Widowed; Other; Prefer not to say",
      name %in% c("weight", "height#1_1_1", "height#1_1_2") ~ "Numeric entry",
      grepl("lifeSat", name) ~ "0-100 sliding scale",
      TRUE ~ "Open text response"
    ),
    `Stem text` = gsub("[\r\n]", "", `Stem text`)
  ) |>
  rename(Variable = name)


write_csv(intake_codeboook, "codebooks/codebook_intake.csv")
write_csv(daily_codebook, "codebooks/codebook_daily.csv")
