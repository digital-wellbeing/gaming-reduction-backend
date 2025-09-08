# Power estimation using SimEngine – cleaned version (no play_sd_prop, no corr_noise_sd)

# CRITICAL: Set CRAN mirror FIRST for cluster environments
options(repos = c(CRAN = "https://cloud.r-project.org"))

# Install SimEngine if not already installed
if (!requireNamespace("SimEngine", quietly = TRUE)) {
  message("Installing SimEngine package...")
  install.packages("SimEngine", dependencies = TRUE)
}

# Check and load required packages
required_packages <- c("SimEngine", "lme4", "lmerTest", "compositions", "MASS", "dplyr", "extraDistr")
missing_packages <- required_packages[!sapply(required_packages, requireNamespace, quietly = TRUE)]

if (length(missing_packages) > 0) {
  message("Installing missing packages: ", paste(missing_packages, collapse = ", "))
  install.packages(missing_packages, dependencies = TRUE)
}

# Load required packages
library(SimEngine)
library(lme4)
library(lmerTest)
library(compositions)
library(MASS)
library(dplyr)
library(extraDistr)  # For Kumaraswamy distribution

# --------------------------------------------------------------------------------
# Command line argument parsing ---------------------------------------------------
# --------------------------------------------------------------------------------
# Parse command line arguments for flexible parameter adjustment
args <- commandArgs(trailingOnly = TRUE)

# Default values
default_sims <- 1000
default_cores <- 10
default_output <- NULL  # Will use default timestamped filename if not specified

# Parse arguments: --sims=VALUE --cores=VALUE --output=PATH
sims_param <- default_sims
cores_param <- default_cores
output_param <- default_output

if (length(args) > 0) {
  for (arg in args) {
    if (grepl("^--sims=", arg)) {
      sims_param <- as.numeric(sub("^--sims=", "", arg))
      if (is.na(sims_param) || sims_param <= 0) {
        warning("Invalid sims parameter, using default: ", default_sims)
        sims_param <- default_sims
      }
    } else if (grepl("^--cores=", arg)) {
      cores_param <- as.numeric(sub("^--cores=", "", arg))
      if (is.na(cores_param) || cores_param <= 0) {
        warning("Invalid cores parameter, using default: ", default_cores)
        cores_param <- default_cores
      }
    } else if (grepl("^--output=", arg)) {
      output_param <- sub("^--output=", "", arg)
      # Validate output path
      output_dir <- dirname(output_param)
      if (!dir.exists(output_dir)) {
        # Try to create the directory
        tryCatch({
          dir.create(output_dir, recursive = TRUE)
          message("Created output directory: ", output_dir)
        }, error = function(e) {
          warning("Could not create output directory: ", output_dir, ". Error: ", e$message, ". Using default location.")
          output_param <<- default_output
        })
      }
    }
  }
}

# Log the parameters being used
message("=== SIMULATION PARAMETERS ===")
message("Number of simulations: ", sims_param)
message("Number of cores: ", cores_param)
message("Output location: ", ifelse(is.null(output_param), "default (timestamped)", output_param))
message("==============================")

# --------------------------------------------------------------------------------
# UNDERSTANDING WITHIN-SUBJECT VARIABILITY (s_within) ---------------------------
# --------------------------------------------------------------------------------
# The s_within parameter controls day-to-day behavioral variability within individuals:
#
# s_within = 0.1  →  Very consistent routine (±15-20 min/day sedentary variation)
#                    Example: Office worker with same commute, same schedule
# s_within = 0.2  →  Moderately consistent (±30-40 min/day variation)  
#                    Example: Typical adult with some weekend/routine differences
# s_within = 0.3  →  Moderately variable (±45-60 min/day variation)
#                    Example: Mix of office/remote work, irregular activities
# s_within = 0.4  →  Highly variable (±60-80 min/day variation)
#                    Example: Shift work, freelancing, frequent travel
#
# POWER IMPACT: Higher s_within = lower power (noise obscures intervention effects)
#
# --------------------------------------------------------------------------------
# Main simulation wrapper ---------------------------------------------------------
# --------------------------------------------------------------------------------

est_power_simengine <- function(n_pg = 40,
                               effect_min_values = c(30),
                               s_between_values  = c(0.15),
                               s_within_values   = c(0.25),
                               baseline_days     = 7,
                               intervention_days = 14,
                               sims              = 500,
                               cores             = 4) {

  start_time <- Sys.time()
  message("Setting up SimEngine simulation …")

  # Create simulation object
  sim <- new_sim()

  # ------------------------------------------------------------------------------
  # LEVELS (note: no play_sd_prop, no corr_noise_sd) ------------------------------
  # ------------------------------------------------------------------------------
  sim %<>% set_levels(
    n_pg             = n_pg,
    effect_min       = effect_min_values,
    s_between        = s_between_values,
    s_within         = s_within_values,
    baseline_days    = baseline_days,
    intervention_days= intervention_days
  )

  # ------------------------------------------------------------------------------
  # Helper transformations (INSIDE function for parallel access) ------------------
  # ------------------------------------------------------------------------------
  
  comp_to_ilr <- function(x_min) {
    stopifnot(is.matrix(x_min), ncol(x_min) == 3)
    bad_row <- !is.finite(rowSums(x_min)) | rowSums(x_min) <= 0
    if (any(bad_row)) {
      x_min[bad_row, ] <- matrix(rep(c(600, 480, 360), each = sum(bad_row)), ncol = 3, byrow = TRUE)
    }
    x_min[x_min <= 0 | !is.finite(x_min)] <- 1e-6
    compositions::ilr(sweep(x_min, 1, rowSums(x_min), "/"))
  }

  ilr_to_minutes <- function(ilr_mat, total = 1440) {
    stopifnot(is.matrix(ilr_mat), ncol(ilr_mat) == 2)
    comp_obj <- compositions::ilrInv(ilr_mat)
    prop <- as.matrix(as.data.frame(comp_obj))
    bad <- apply(prop, 1, function(r) any(!is.finite(r) | r <= 0) ||
                   !is.finite(sum(r)) || abs(sum(r) - 1) > 1e-8)
    if (any(bad)) prop[bad, ] <- 1/3
    round(prop * total, 1)
  }

  # ------------------------------------------------------------------------------
  # Data‑generating function ------------------------------------------------------
  # ------------------------------------------------------------------------------
  generate_data <- function(n_pg, effect_min, baseline_days, intervention_days,
                            s_between, s_within, seed = NULL) {

    if (!is.null(seed)) set.seed(seed)

    N   <- n_pg * 2
    grp <- rep(0:1, each = n_pg)              # 0 = Control, 1 = Intervention

    # Mean daily compositions: (sedentary, sleep, physical)
    base_comp   <- c(600, 480, 360)
    active_comp <- c(600 - effect_min, 480, 360 + effect_min)

    # Person‑level random effects in ILR space
    b_ilr <- MASS::mvrnorm(N, mu = c(0, 0), Sigma = diag(s_between^2, 2))

    # Person-specific playtime proportion of sedentary time (10-40%)
    personal_play_prop <- sapply(1:N, function(i) {
      p <- rbeta(1, 2, 5) * 0.3 + 0.1  # right-skew between 0.1-0.4
      return(p)
    })

    # Person-specific compliance rates for intervention group (60-95%)
    # Control group gets compliance = 1 (no intervention to comply with)
    personal_compliance <- sapply(1:N, function(i) {
      if (grp[i] == 0) {
        return(1.0)  # Control group - no intervention
      } else {
        # Intervention group: Beta distribution shifted to 60-95% range
        # Beta(2,2) gives symmetric distribution, shifted to [0.6, 0.95]
        compliance <- rbeta(1, 2, 2) * 0.35 + 0.6
        return(compliance)
      }
    })
    
    # Daily compliance variation using Kumaraswamy distribution
    # This adds day-to-day variation in compliance within each person
    daily_compliance_variation <- function(n_days, base_compliance) {
      # Use Kumaraswamy distribution to add daily variation
      # Parameters chosen to create realistic daily fluctuations around base compliance
      daily_factors <- extraDistr::rkumar(n_days, a = 0.05, b = 0.1)
      # Scale the factors to create variation around base compliance
      # This creates realistic day-to-day variation in compliance behavior
      pmax(0, pmin(1, base_compliance * (0.8 + 0.4 * daily_factors)))
    }

    # Containers
    all_ids <- all_periods <- all_days <- NULL
    all_ilr <- matrix(, 0, 2)
    all_sedentary <- numeric()  # Store actual sedentary minutes

    for (i in seq_len(N)) {
      # Pre-generate daily compliance for this person during intervention period
      person_daily_compliance <- if (grp[i] == 1) {
        daily_compliance_variation(intervention_days, personal_compliance[i])
      } else {
        rep(1.0, intervention_days)  # Control group
      }
      
      intervention_day_counter <- 0
      
      for (period in c("baseline", "intervention")) {
        ndays   <- if (period == "baseline") baseline_days else intervention_days
        
        # Generate day-specific compositions based on compliance
        day_compositions <- matrix(nrow = ndays, ncol = 3)
        
        for (day_in_period in 1:ndays) {
          if (period == "baseline" || grp[i] == 0) {
            # Baseline period or control group: use base composition
            day_compositions[day_in_period, ] <- base_comp
          } else {
            # Intervention period for intervention group: apply compliance-adjusted effect
            intervention_day_counter <- intervention_day_counter + 1
            daily_compliance_rate <- person_daily_compliance[intervention_day_counter]
            
            # Apply effect proportional to compliance
            # Full compliance = full effect, partial compliance = proportional effect
            adjusted_effect <- effect_min * daily_compliance_rate
            day_compositions[day_in_period, ] <- c(
              base_comp[1] - adjusted_effect,  # sedentary decreases
              base_comp[2],                    # sleep stays same
              base_comp[3] + adjusted_effect   # physical activity increases
            )
          }
        }

        comp_ilr <- comp_to_ilr(day_compositions)
        comp_ilr <- sweep(comp_ilr, 2, b_ilr[i, ], "+")               # add person RE
        day_ilr  <- comp_ilr + MASS::mvrnorm(ndays, mu = c(0, 0),
                                             Sigma = diag(s_within^2, 2))

        # Index bookkeeping
        all_ids     <- c(all_ids, rep(i, ndays))
        all_periods <- c(all_periods, rep(period, ndays))
        all_days    <- c(all_days,
                         if (period == "baseline") seq_len(baseline_days)
                         else baseline_days + seq_len(intervention_days))
        all_ilr     <- rbind(all_ilr, day_ilr)
        
        # Store sedentary minutes for this person-period (will be calculated after ILR transformation)
        # We'll calculate playtime after we have the actual sedentary minutes
      }
    }

    # Back‑transform ILR → minutes and calculate playtime based on actual sedentary behavior
    mins <- ilr_to_minutes(all_ilr)
    colnames(mins) <- c("sedentary", "sleep", "physical")
    
    # Now generate playtime based on actual sedentary minutes
    playmin <- numeric(length(all_ids))
    
    # Pre-generate daily compliance for each person during intervention period
    daily_compliance <- list()
    for (person_id in 1:N) {
      if (grp[person_id] == 1) {  # Intervention group
        daily_compliance[[person_id]] <- daily_compliance_variation(
          intervention_days, 
          personal_compliance[person_id]
        )
      } else {
        daily_compliance[[person_id]] <- rep(1.0, intervention_days)  # Control group
      }
    }
    
    # Track intervention day counter for each person
    intervention_day_counter <- rep(0, N)
    
    for (i in seq_along(all_ids)) {
      person_id <- all_ids[i]
      period <- all_periods[i]
      actual_sedentary <- mins[i, "sedentary"]
      
      # Base playtime as proportion of actual sedentary time
      base_playtime <- personal_play_prop[person_id] * actual_sedentary
      
      # Add small amount of day-to-day noise (2% of base playtime)
      daily_sd <- 0.02 * base_playtime
      noisy_playtime <- rnorm(1, base_playtime, daily_sd)
      
      # Apply intervention effect for intervention group during intervention period
      if (period == "intervention" && grp[person_id] == 1) {
        # Increment intervention day counter for this person
        intervention_day_counter[person_id] <- intervention_day_counter[person_id] + 1
        
        # Get daily compliance for this person and day
        daily_compliance_rate <- daily_compliance[[person_id]][intervention_day_counter[person_id]]
        
        # Reduce playtime by effect_min * daily compliance rate
        # Perfect compliance = full effect_min reduction
        # Partial compliance = proportional reduction
        actual_reduction <- effect_min * daily_compliance_rate
        intervention_playtime <- pmax(0, noisy_playtime - actual_reduction)
        playmin[i] <- intervention_playtime
      } else {
        # Control group or baseline period: just use the playtime based on actual sedentary
        playmin[i] <- pmax(0, noisy_playtime)  # Ensure non-negative
      }
    }

    # Create daily compliance values for the dataset
    daily_compliance_values <- numeric(length(all_ids))
    intervention_day_counter <- rep(0, N)
    
    for (i in seq_along(all_ids)) {
      person_id <- all_ids[i]
      period <- all_periods[i]
      
      if (period == "intervention" && grp[person_id] == 1) {
        intervention_day_counter[person_id] <- intervention_day_counter[person_id] + 1
        daily_compliance_values[i] <- daily_compliance[[person_id]][intervention_day_counter[person_id]]
      } else {
        daily_compliance_values[i] <- personal_compliance[person_id]
      }
    }
    
    # Assemble data frame
    dat <- data.frame(
      id        = factor(all_ids),
      group     = factor(grp[all_ids], labels = c("Control", "Abstinence")),
      period    = factor(all_periods, levels = c("baseline", "intervention")),
      day       = all_days,
      sedentary = mins[, 1],
      sleep     = mins[, 2],
      physical  = mins[, 3],
      playtime  = playmin,
      compliance = daily_compliance_values,  # Add daily compliance to dataset
      base_compliance = personal_compliance[all_ids]  # Add base person-level compliance
    )

    dat <- dat %>%
      group_by(id) %>%
      mutate(
        base_play_mean      = mean(playtime[period == "baseline"]),
        playtime_reduction  = base_play_mean - playtime,
        intervention_active = as.integer(group == "Abstinence" & period == "intervention"),
        # Calculate actual compliance as proportion of intended reduction achieved
        intended_reduction  = ifelse(group == "Abstinence" & period == "intervention", effect_min, 0),
        actual_compliance   = ifelse(intended_reduction > 0, 
                                   pmin(1, playtime_reduction / intended_reduction), 
                                   compliance)
      ) %>%
      ungroup()

    return(dat)
  }

  # ------------------------------------------------------------------------------
  # Analysis function -------------------------------------------------------------
  # ------------------------------------------------------------------------------
  run_analysis <- function(data) {
    data_ilr <- data
    comp_matrix <- as.matrix(data[, c("sedentary", "sleep", "physical")])
    ilr_coords  <- comp_to_ilr(comp_matrix)
    data_ilr$ilr1 <- ilr_coords[, 1]

    results <- list()

    ## Between‑group effect during intervention ----------------------------------
    md <- subset(data_ilr, period == "intervention")
    mb <- try(lmer(ilr1 ~ group + (1 | id), data = md), silent = TRUE)
    results$p_between <- if (!inherits(mb, "try-error")) anova(mb)["group", "Pr(>F)"] else NA

    ## Within‑group effects -------------------------------------------------------
    mc <- try(lmer(ilr1 ~ period + (1 | id), data = subset(data_ilr, group == "Control")), silent = TRUE)
    results$p_control <- if (!inherits(mc, "try-error")) anova(mc)["period", "Pr(>F)"] else NA

    mi <- try(lmer(ilr1 ~ period + (1 | id), data = subset(data_ilr, group == "Abstinence")), silent = TRUE)
    results$p_intervention <- if (!inherits(mi, "try-error")) anova(mi)["period", "Pr(>F)"] else NA

    ## Interaction ----------------------------------------------------------------
    mx <- try(lmer(ilr1 ~ group * period + (1 | id), data = data_ilr), silent = TRUE)
    results$p_interaction <- if (!inherits(mx, "try-error")) anova(mx)["group:period", "Pr(>F)"] else NA

    ## Per‑protocol contrast (original change score approach) ---------------------
    mp_change <- try(lmer(ilr1 ~ intervention_active * playtime_reduction + (1 | id), data = data_ilr), silent = TRUE)
    results$p_protocol_change <- if (!inherits(mp_change, "try-error")) anova(mp_change)["intervention_active:playtime_reduction", "Pr(>F)"] else NA
    
    ## Per‑protocol contrast (robust approach without change scores) --------------
    # This model tests if the intervention effect on sedentary behavior (ilr1) varies 
    # as a function of actual playtime levels, controlling for baseline playtime.
    # More robust than change scores as it directly models the relationship between
    # current playtime and outcomes while adjusting for baseline differences.
    mp_robust <- try(lmer(ilr1 ~ intervention_active * playtime + base_play_mean + (1 | id), data = data_ilr), silent = TRUE)
    results$p_protocol_robust <- if (!inherits(mp_robust, "try-error")) anova(mp_robust)["intervention_active:playtime", "Pr(>F)"] else NA

    return(results)
  }

  # ------------------------------------------------------------------------------
  # Simulation script ------------------------------------------------------------
  # ------------------------------------------------------------------------------
  sim %<>% set_script(function() {
    set.seed(sample.int(1e7, 1))
    
    # Access simulation level variables correctly
    data <- generate_data(
      n_pg             = L$n_pg,
      effect_min       = L$effect_min,
      baseline_days    = L$baseline_days,
      intervention_days= L$intervention_days,
      s_between        = L$s_between,
      s_within         = L$s_within
    )
    
    # Run analysis and ensure proper error handling
    result <- tryCatch({
      run_analysis(data)
    }, error = function(e) {
      # Return NA values with proper names if analysis fails
      list(
        p_between = NA_real_, 
        p_control = NA_real_, 
        p_intervention = NA_real_,
        p_interaction = NA_real_, 
        p_protocol_change = NA_real_,
        p_protocol_robust = NA_real_
      )
    })
    
    # Ensure result is a proper list with all required elements
    if (!is.list(result)) {
      result <- list(
        p_between = NA_real_, 
        p_control = NA_real_, 
        p_intervention = NA_real_,
        p_interaction = NA_real_, 
        p_protocol_change = NA_real_,
        p_protocol_robust = NA_real_
      )
    }
    
    # Ensure all required columns exist
    required_names <- c("p_between", "p_control", "p_intervention", "p_interaction", "p_protocol_change", "p_protocol_robust")
    for (name in required_names) {
      if (!(name %in% names(result))) {
        result[[name]] <- NA_real_
      }
    }
    
    return(result)
  })

  # ------------------------------------------------------------------------------
  # Config & run -----------------------------------------------------------------
  # ------------------------------------------------------------------------------
  
  # Try parallel first, fallback to sequential if it fails
  parallel_success <- FALSE
  tryCatch({
    message("Attempting parallel execution with ", cores, " cores...")
    sim %<>% set_config(
      num_sim      = sims,
      parallel     = TRUE,   # Enable parallel processing
      n_cores      = cores,  # Use specified cores
      packages     = c("lme4", "lmerTest", "compositions", "MASS", "dplyr", "extraDistr"),
      progress_bar = TRUE
    )
    parallel_success <- TRUE
  }, error = function(e) {
    message("Parallel setup failed: ", e$message)
    message("Falling back to sequential processing...")
    sim %<>% set_config(
      num_sim      = sims,
      parallel     = FALSE,  # Disable parallel processing
      packages     = c("lme4", "lmerTest", "compositions", "MASS", "dplyr", "extraDistr"),
      progress_bar = TRUE
    )
  })

  
  # Add a test run to debug issues
  message("Testing data generation and analysis functions...")
  tryCatch({
    test_data <- generate_data(
      n_pg = 10,  # Small test
      effect_min = 30,
      baseline_days = 7,
      intervention_days = 14,
      s_between = 0.15,
      s_within = 0.25
    )
    message("✓ Data generation successful")
    message("Test data dimensions: ", nrow(test_data), " x ", ncol(test_data))
    
    test_results <- run_analysis(test_data)
    message("✓ Analysis function successful")
    message("Test results: ", paste(names(test_results), test_results, sep="=", collapse=", "))
  }, error = function(e) {
    message("❌ Test failed with error: ", e$message)
    stop("Stopping due to test failure. Fix the issue before running full simulation.")
  })

  message("Running simulations…")
  
  # Wrap simulation run in error handling
  sim_results <- tryCatch({
    sim %<>% run()
    sim
  }, error = function(e) {
    message("❌ Simulation failed with error: ", e$message)
    if (parallel_success && grepl("connection|unserialize|node", e$message, ignore.case = TRUE)) {
      message("🔄 This looks like a parallel processing issue. Retrying with sequential processing...")
      sim %<>% set_config(
        num_sim      = sims,
        parallel     = FALSE,  # Disable parallel processing
        packages     = c("lme4", "lmerTest", "compositions", "MASS", "dplyr", "extraDistr"),
        progress_bar = TRUE
      )
      sim %<>% run()
      return(sim)
    } else {
      stop("Simulation failed: ", e$message)
    }
  })
  
  # Update sim object
  sim <- sim_results

  # ------------------------------------------------------------------------------
  # Summarise power --------------------------------------------------------------
  # ------------------------------------------------------------------------------
  results <- sim$results
  
  # Add debugging information
  message("Debug: Checking simulation results...")
  message("Results object class: ", class(results))
  message("Results is null: ", is.null(results))
  if (!is.null(results)) {
    message("Results dimensions: ", nrow(results), " x ", ncol(results))
    message("Results column names: ", paste(names(results), collapse = ", "))
  }
  
  # Add error handling for when all simulations fail
  if (is.null(results) || (is.data.frame(results) && nrow(results) == 0)) {
    stop("All simulations failed. Check your simulation parameters and functions.")
  }
  
  # Check if required columns exist before processing
  required_cols <- c("p_between", "p_control", "p_intervention", "p_interaction", "p_protocol_change", "p_protocol_robust")
  missing_cols <- setdiff(required_cols, names(results))
  if (length(missing_cols) > 0) {
    stop(paste("Missing columns in results:", paste(missing_cols, collapse = ", ")))
  }
  
  for (col in required_cols) {
    results[[col]] <- as.numeric(as.character(results[[col]]))
  }

  power_df <- aggregate(
    cbind(
      power_between        = results$p_between        < 0.05,
      power_control        = results$p_control        < 0.05,
      power_intervention   = results$p_intervention   < 0.05,
      power_interaction    = results$p_interaction    < 0.05,
      power_protocol_change= results$p_protocol_change< 0.05,
      power_protocol_robust= results$p_protocol_robust< 0.05,
      valid_between        = !is.na(results$p_between),
      valid_control        = !is.na(results$p_control), 
      valid_intervention   = !is.na(results$p_intervention),
      valid_interaction    = !is.na(results$p_interaction),
      valid_protocol_change= !is.na(results$p_protocol_change),
      valid_protocol_robust= !is.na(results$p_protocol_robust)
    ),
    by = list(
      n_pg             = results$n_pg,
      effect_min       = results$effect_min,
      s_between        = results$s_between,
      s_within         = results$s_within,
      baseline_days    = results$baseline_days,
      intervention_days= results$intervention_days
    ),
    FUN = mean, na.rm = TRUE
  )

  end_time <- Sys.time()
  message(sprintf("Total elapsed time: %.2f mins", as.numeric(difftime(end_time, start_time, units = "mins"))))

  list(power_summary = power_df, sim_object = sim)
}

# --------------------------------------------------------------------------------
# Example call -------------------------------------------------------------------
# --------------------------------------------------------------------------------
result <- est_power_simengine(
  n_pg               = 40,  # 40 participants per group
  effect_min_values =  c(30, 60, 90),          
  s_between_values = seq(0, 0.5, by = 0.25),
  s_within_values = seq(0, 0.5, by = 0.1),
  baseline_days      = 7,
  intervention_days  = 14,
  sims               = sims_param, 
  cores              = cores_param     
)

# result <- est_power_simengine(
#   n_pg               = c(50),  # Multiple sample sizes
#   effect_min_values =  c(30, 60, 90, 120),          
#   s_between_values = seq(0.1, 0.3, by = 0.05),
#   s_within_values = seq(0.15, 0.35, by = 0.05),
#   baseline_days      = 7,
#   intervention_days  = 14,
#   sims               = sims_param, 
#   cores              = cores_param     
# )
# print(result$power_summary)

# Save results with user-specified or default timestamped filename
if (is.null(output_param)) {
  # Use default timestamped filename
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  filename <- paste0("scripts/sim_comp_debug/power_sim_results_", timestamp, ".RData")
} else {
  # Use user-specified filename
  filename <- output_param
  # Add .RData extension if not present
  if (!grepl("\\.(RData|rda)$", filename, ignore.case = TRUE)) {
    filename <- paste0(filename, ".RData")
  }
}

save(result, file = filename)

# Print power summary
# print(result$power_summary)

# Print save location
message("Results saved to: ", filename)

# POWER SUMMARY ANALYSIS
message("\n" , paste(rep("=", 60), collapse=""))
message("POWER SUMMARY - TOP PERFORMING SETTINGS")
message(paste(rep("=", 60), collapse=""))

power_data <- result$power_summary

# Summary for power_interaction
message("\n🎯 INTERACTION EFFECT POWER SUMMARY:")
message("-----------------------------------")

# Find maximum power for interaction
max_interaction_power <- max(power_data$power_interaction, na.rm = TRUE)
best_interaction <- power_data[which.max(power_data$power_interaction), ]

message(sprintf("Maximum Interaction Power: %.3f", max_interaction_power))
message("Best settings:")
message(sprintf("  • Sample size per group (n_pg): %d", best_interaction$n_pg))
message(sprintf("  • Effect size (effect_min): %d minutes", best_interaction$effect_min))
message(sprintf("  • Between-subject SD (s_between): %.3f", best_interaction$s_between))
message(sprintf("  • Within-subject SD (s_within): %.3f", best_interaction$s_within))

# Show top 3 settings for interaction
message("\nTop 3 settings for interaction power:")
top_interaction <- power_data[order(power_data$power_interaction, decreasing = TRUE)[1:min(3, nrow(power_data))], ]
for(i in 1:nrow(top_interaction)) {
  row <- top_interaction[i, ]
  message(sprintf("%d. Power=%.3f | n_pg=%d | effect=%d | s_between=%.3f | s_within=%.3f", 
                  i, row$power_interaction, row$n_pg, row$effect_min, row$s_between, row$s_within))
}

# Summary for power_protocol (both approaches)
message("\n🎯 PROTOCOL EFFECT POWER SUMMARY:")
message("--------------------------------")

# Change score approach
max_protocol_change_power <- max(power_data$power_protocol_change, na.rm = TRUE)
best_protocol_change <- power_data[which.max(power_data$power_protocol_change), ]

message("\n📊 CHANGE SCORE APPROACH:")
message(sprintf("Maximum Protocol Power (Change): %.3f", max_protocol_change_power))
message("Best settings:")
message(sprintf("  • Sample size per group (n_pg): %d", best_protocol_change$n_pg))
message(sprintf("  • Effect size (effect_min): %d minutes", best_protocol_change$effect_min))
message(sprintf("  • Between-subject SD (s_between): %.3f", best_protocol_change$s_between))
message(sprintf("  • Within-subject SD (s_within): %.3f", best_protocol_change$s_within))

# Robust approach
max_protocol_robust_power <- max(power_data$power_protocol_robust, na.rm = TRUE)
best_protocol_robust <- power_data[which.max(power_data$power_protocol_robust), ]

message("\n📊 ROBUST APPROACH (no change scores):")
message(sprintf("Maximum Protocol Power (Robust): %.3f", max_protocol_robust_power))
message("Best settings:")
message(sprintf("  • Sample size per group (n_pg): %d", best_protocol_robust$n_pg))
message(sprintf("  • Effect size (effect_min): %d minutes", best_protocol_robust$effect_min))
message(sprintf("  • Between-subject SD (s_between): %.3f", best_protocol_robust$s_between))
message(sprintf("  • Within-subject SD (s_within): %.3f", best_protocol_robust$s_within))

# Comparison
message("\n🔍 PROTOCOL APPROACH COMPARISON:")
message(sprintf("Change Score Approach - Mean: %.3f, Max: %.3f", 
                mean(power_data$power_protocol_change, na.rm = TRUE),
                max_protocol_change_power))
message(sprintf("Robust Approach - Mean: %.3f, Max: %.3f", 
                mean(power_data$power_protocol_robust, na.rm = TRUE),
                max_protocol_robust_power))
                
power_diff <- mean(power_data$power_protocol_robust, na.rm = TRUE) - mean(power_data$power_protocol_change, na.rm = TRUE)
message(sprintf("Robust approach is %.3f points %s on average", 
                abs(power_diff), 
                ifelse(power_diff > 0, "higher", "lower")))

# Overall summary statistics
message("\n📊 OVERALL POWER STATISTICS:")
message("---------------------------")
message(sprintf("Interaction Power - Mean: %.3f, Range: %.3f - %.3f", 
                mean(power_data$power_interaction, na.rm = TRUE),
                min(power_data$power_interaction, na.rm = TRUE),
                max(power_data$power_interaction, na.rm = TRUE)))
                
message(sprintf("Protocol Power (Change) - Mean: %.3f, Range: %.3f - %.3f", 
                mean(power_data$power_protocol_change, na.rm = TRUE),
                min(power_data$power_protocol_change, na.rm = TRUE),
                max(power_data$power_protocol_change, na.rm = TRUE)))
                
message(sprintf("Protocol Power (Robust) - Mean: %.3f, Range: %.3f - %.3f", 
                mean(power_data$power_protocol_robust, na.rm = TRUE),
                min(power_data$power_protocol_robust, na.rm = TRUE),
                max(power_data$power_protocol_robust, na.rm = TRUE)))

# DATA QUALITY ANALYSIS
message("\n" , paste(rep("=", 60), collapse=""))
message("DATA QUALITY ANALYSIS - VALIDITY RATES")
message(paste(rep("=", 60), collapse=""))

# Check validity rates for each contrast
validity_threshold <- 0.90
total_rows <- nrow(power_data)

# Function to analyze validity for each contrast
analyze_validity <- function(valid_col, contrast_name) {
  high_validity_count <- sum(power_data[[valid_col]] > validity_threshold, na.rm = TRUE)
  perfect_validity_count <- sum(power_data[[valid_col]] == 1.0, na.rm = TRUE)
  mean_validity <- mean(power_data[[valid_col]], na.rm = TRUE)
  min_validity <- min(power_data[[valid_col]], na.rm = TRUE)
  
  message(sprintf("\n🔍 %s VALIDITY:", toupper(contrast_name)))
  message(sprintf("  • Rows with validity > %.2f: %d/%d (%.1f%%)", 
                  validity_threshold, high_validity_count, total_rows, 
                  100 * high_validity_count / total_rows))
  message(sprintf("  • Rows with perfect validity (1.0): %d/%d (%.1f%%)", 
                  perfect_validity_count, total_rows, 
                  100 * perfect_validity_count / total_rows))
  message(sprintf("  • Mean validity: %.3f", mean_validity))
  message(sprintf("  • Minimum validity: %.3f", min_validity))
  
  # Identify problematic parameter combinations if any
  if (high_validity_count < total_rows) {
    low_validity_rows <- power_data[power_data[[valid_col]] <= validity_threshold, ]
    message(sprintf("  ⚠️  %d rows with validity ≤ %.2f:", 
                    nrow(low_validity_rows), validity_threshold))
    for(i in 1:min(3, nrow(low_validity_rows))) {  # Show up to 3 examples
      row <- low_validity_rows[i, ]
      message(sprintf("     Example %d: validity=%.3f | n_pg=%d | effect=%d | s_between=%.3f | s_within=%.3f", 
                      i, row[[valid_col]], row$n_pg, row$effect_min, row$s_between, row$s_within))
    }
    if (nrow(low_validity_rows) > 3) {
      message(sprintf("     ... and %d more problematic combinations", nrow(low_validity_rows) - 3))
    }
  } else {
    message("  ✅ All parameter combinations produced high-quality results!")
  }
  
  return(list(
    high_validity_count = high_validity_count,
    perfect_validity_count = perfect_validity_count,
    mean_validity = mean_validity,
    min_validity = min_validity
  ))
}

# Analyze each contrast type
contrasts <- list(
  "valid_between" = "Between-Group",
  "valid_control" = "Control Within-Group", 
  "valid_intervention" = "Intervention Within-Group",
  "valid_interaction" = "Group × Period Interaction",
  "valid_protocol_change" = "Per-Protocol (Change Score)",
  "valid_protocol_robust" = "Per-Protocol (Robust)"
)

validity_summary <- list()
for(col in names(contrasts)) {
  validity_summary[[col]] <- analyze_validity(col, contrasts[[col]])
}

# Overall validity summary
message("\n📋 OVERALL VALIDITY SUMMARY:")
message("---------------------------")
all_high_validity <- sapply(validity_summary, function(x) x$high_validity_count)
all_perfect_validity <- sapply(validity_summary, function(x) x$perfect_validity_count)
all_mean_validity <- sapply(validity_summary, function(x) x$mean_validity)

message(sprintf("Contrast with highest reliability: %s (%d/%d rows > %.2f)", 
                contrasts[[which.max(all_high_validity)]], 
                max(all_high_validity), total_rows, validity_threshold))
message(sprintf("Contrast with lowest reliability: %s (%d/%d rows > %.2f)", 
                contrasts[[which.min(all_high_validity)]], 
                min(all_high_validity), total_rows, validity_threshold))

# Check if all contrasts are highly reliable
if(all(all_high_validity == total_rows)) {
  message("✅ EXCELLENT: All contrasts have high validity (>95%) across all parameter combinations!")
} else {
  problematic_contrasts <- names(contrasts)[all_high_validity < total_rows]
  message(sprintf("⚠️  WARNING: %d contrast(s) have some parameter combinations with low validity:", 
                  length(problematic_contrasts)))
  for(contrast in problematic_contrasts) {
    message(sprintf("   • %s: %d/%d rows with validity ≤ %.2f", 
                    contrasts[[contrast]], 
                    total_rows - all_high_validity[[contrast]], 
                    total_rows, validity_threshold))
  }
}

message("\n" , paste(rep("=", 60), collapse=""))