# Load and examine power simulation results
# This script analyzes why ITT vs Per Protocol power difference is small

# Load the results
load("scripts/sim_comp_debug/power_sim_results_20250722_223508.RData")

# Extract power summary
power_data <- result$power_summary

# Print basic statistics
cat("=== POWER COMPARISON ANALYSIS ===\n")
cat("Total parameter combinations:", nrow(power_data), "\n\n")

# Compare ITT (interaction) vs Per Protocol approaches
cat("1. INTENTION TO TREAT (Group × Period Interaction):\n")
cat("   Mean power:", round(mean(power_data$power_interaction, na.rm = TRUE), 3), "\n")
cat("   Range:", round(range(power_data$power_interaction, na.rm = TRUE), 3), "\n")
cat("   SD:", round(sd(power_data$power_interaction, na.rm = TRUE), 3), "\n\n")

cat("2. PER PROTOCOL (Change Score Approach):\n")
cat("   Mean power:", round(mean(power_data$power_protocol_change, na.rm = TRUE), 3), "\n")
cat("   Range:", round(range(power_data$power_protocol_change, na.rm = TRUE), 3), "\n")
cat("   SD:", round(sd(power_data$power_protocol_change, na.rm = TRUE), 3), "\n\n")

cat("3. PER PROTOCOL (Robust Approach):\n")
cat("   Mean power:", round(mean(power_data$power_protocol_robust, na.rm = TRUE), 3), "\n")
cat("   Range:", round(range(power_data$power_protocol_robust, na.rm = TRUE), 3), "\n")
cat("   SD:", round(sd(power_data$power_protocol_robust, na.rm = TRUE), 3), "\n\n")

# Calculate differences
itt_vs_pp_change <- power_data$power_interaction - power_data$power_protocol_change
itt_vs_pp_robust <- power_data$power_interaction - power_data$power_protocol_robust

cat("4. POWER DIFFERENCES:\n")
cat("   ITT vs PP(Change) - Mean difference:", round(mean(itt_vs_pp_change, na.rm = TRUE), 3), "\n")
cat("   ITT vs PP(Change) - SD of differences:", round(sd(itt_vs_pp_change, na.rm = TRUE), 3), "\n")
cat("   ITT vs PP(Robust) - Mean difference:", round(mean(itt_vs_pp_robust, na.rm = TRUE), 3), "\n")
cat("   ITT vs PP(Robust) - SD of differences:", round(sd(itt_vs_pp_robust, na.rm = TRUE), 3), "\n\n")

# Look at correlations
cat("5. CORRELATIONS BETWEEN APPROACHES:\n")
cat("   ITT vs PP(Change):", round(cor(power_data$power_interaction, power_data$power_protocol_change, use = "complete.obs"), 3), "\n")
cat("   ITT vs PP(Robust):", round(cor(power_data$power_interaction, power_data$power_protocol_robust, use = "complete.obs"), 3), "\n")
cat("   PP(Change) vs PP(Robust):", round(cor(power_data$power_protocol_change, power_data$power_protocol_robust, use = "complete.obs"), 3), "\n\n")

# Examine specific scenarios where differences are largest
cat("6. SCENARIOS WITH LARGEST ITT vs PP DIFFERENCES:\n")
abs_diff_change <- abs(itt_vs_pp_change)
top_diff_indices <- order(abs_diff_change, decreasing = TRUE)[1:5]

for(i in 1:5) {
  idx <- top_diff_indices[i]
  row <- power_data[idx, ]
  diff <- itt_vs_pp_change[idx]
  cat(sprintf("   Rank %d: ITT=%.3f, PP(Change)=%.3f, Diff=%.3f | n_pg=%d, effect=%d, s_between=%.3f, s_within=%.3f\n",
              i, row$power_interaction, row$power_protocol_change, diff,
              row$n_pg, row$effect_min, row$s_between, row$s_within))
}

cat("\n7. ANALYSIS OF WHY DIFFERENCES ARE SMALL:\n")

# Check validity rates - if models are failing, power estimates may be unreliable
cat("   Model validity rates:\n")
cat("   ITT (interaction):", round(mean(power_data$valid_interaction, na.rm = TRUE), 3), "\n")
cat("   PP (change):", round(mean(power_data$valid_protocol_change, na.rm = TRUE), 3), "\n")
cat("   PP (robust):", round(mean(power_data$valid_protocol_robust, na.rm = TRUE), 3), "\n\n")

# Look at effect sizes and compliance patterns
cat("   Effect sizes tested:", unique(power_data$effect_min), "\n")
cat("   Between-subject variability tested:", unique(power_data$s_between), "\n")
cat("   Within-subject variability tested:", unique(power_data$s_within), "\n\n")

# Examine the simulation design by looking at the script components
cat("8. SIMULATION DESIGN INSIGHTS:\n")
cat("   From the simulation script, we can see:\n")
cat("   - Compliance rates: 60-95% (Beta distribution)\n")
cat("   - Daily compliance variation: Kumaraswamy distribution\n")
cat("   - Effect is proportional to compliance (adjusted_effect = effect_min * daily_compliance_rate)\n")
cat("   - Both ITT and PP are testing similar underlying relationships\n\n")

cat("POTENTIAL REASONS FOR SMALL DIFFERENCES:\n")
cat("1. High average compliance (77.5% midpoint) means ITT captures most of the effect\n")
cat("2. Per-protocol models may not be leveraging compliance variation effectively\n")
cat("3. The interaction effect in ITT already captures group differences well\n")
cat("4. Compositional data constraints may limit power differences\n")
cat("5. Random effects structure may absorb much of the individual variation\n")