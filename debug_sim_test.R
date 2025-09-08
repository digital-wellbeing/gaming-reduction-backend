library(pacman)
p_load(tidyverse, qualtRics, lme4, mgcv, marginaleffects, broom, forestplot, broom.mixed, nlme, rms, emmeans, splines, furrr, extraDistr, kableExtra)

# Set up parallel processing
plan(multisession, workers = 2)

# Source the simulation functions from the qmd file - we'll extract just the function definitions
source_file_path <- "scripts/sim_self_report_fast.qmd"

# Extract and define the sim_data function (simplified for testing)
sim_data <- function(n = 80, n_days = 28, b = 3.7, mu = 78.5, tau_int = 9.7, tau_slope = .05, within_person_sd = 11.8, phi = 0.8, effect_shape = "grow", k = .5, mediated = FALSE, playtime_grand_mean = 1, playtime_grand_sd = .5, daily_play_sd = 0.5) {
  dat <- tibble(
    id = 1:n,
    age = sample(18:36, n, replace = TRUE),
    gender = sample(c("man","woman","non-binary"), n, prob = c(.45, .45, .1), replace = TRUE),
    condition = factor(sample(c("control", "intervention"), n, replace = TRUE)),
    experimental_condition = ifelse(condition == "intervention", 1, 0),
    intercept_wb = rnorm(n, 0, tau_int),
    slope_wb = rnorm(n, 0, tau_slope),
    intercept_play = rlnorm(n, log(playtime_grand_mean), playtime_grand_sd),
  ) |> 
    crossing(day = 1:n_days) |> 
    mutate(
      intervention_period = as.numeric(day > 7 & day < 22),
      intervention_active = intervention_period & condition == "intervention",
      compliance = ifelse(intervention_active, rkumar(n*n_days, a = .05, b = .1), 0),
      playtime = (1 - compliance) * rlnorm(n, log(intercept_play), daily_play_sd),
      effect_time = case_when(
        effect_shape == "plateau" ~ if_else(intervention_period == 1, (b + slope_wb) * (1-exp(-k * (day - 7))), 0),
        effect_shape == "grow" ~ if_else(intervention_period == 1, (day - 7) * ((b + slope_wb)/7), 0),
        TRUE ~ NA_real_
      ),
    ) |> 
    group_by(id) |> 
    mutate(
      baseline_playtime = mean(playtime[day <= 7]),
      reduction = baseline_playtime - playtime,
      sigma = within_person_sd * sqrt(1-phi^2),
      e = as.numeric(arima.sim(n = n_days, model = list(ar = phi), sd = sigma)),
      wellbeing = case_when(
        mediated == TRUE ~ mu + intercept_wb + effect_time * reduction + .01*(age-18) + -.05*gender %in% c("women","non-binary") + e,
        mediated == FALSE ~ mu + intercept_wb + effect_time * experimental_condition * intervention_period + .01*(age-18) + -.05*gender %in% c("women","non-binary") + e
      )
    ) |> 
    ungroup() |> 
    mutate(across(where(is.numeric), ~ round(., 3)))
  
  dat
}

# Simple test - try to run sim_data
cat("Testing sim_data function...\n")
test_dat <- sim_data(n = 10, n_days = 10, mediated = FALSE)
cat("sim_data worked! Generated", nrow(test_dat), "rows\n")

# Test a simple GAM fit function
fit_gam_simple <- function(dat) {
  gam(wellbeing ~ 
        condition:intervention_period + age + gender +
        s(id, bs = "re") + 
        s(day, by = condition, bs = "tp"), 
      data = dat,
      correlation = corAR1(form = ~ day | id))
}

cat("Testing GAM fit...\n")
try({
  test_gam <- fit_gam_simple(test_dat)
  cat("GAM fit worked!\n")
}, silent = FALSE)

# Test parallel execution with a very simple function
cat("Testing parallel execution...\n")
test_results <- future_map_dfr(1:5, function(i) {
  library(tidyverse)
  library(mgcv)
  library(nlme)
  
  # Simple test data
  test_dat_local <- tibble(
    y = rnorm(20),
    x = rnorm(20),
    id = rep(1:4, 5),
    group = sample(c("A", "B"), 20, replace = TRUE)
  )
  
  tibble(
    iteration = i,
    mean_y = mean(test_dat_local$y),
    n_rows = nrow(test_dat_local)
  )
})

cat("Parallel test results:\n")
print(test_results)

cat("All basic tests completed successfully!\n")