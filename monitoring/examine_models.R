# Examine the model specifications to understand power differences

# The models from the simulation script:

cat("=== MODEL COMPARISON ANALYSIS ===\n\n")

cat("1. INTENTION TO TREAT (ITT) MODEL:\n")
cat("   Model: lmer(ilr1 ~ group * period + (1 | id))\n")
cat("   Tests: group:period interaction\n")
cat("   Logic: Tests if treatment group changes differently than control over time\n")
cat("   Strength: Simple, robust, tests the randomized assignment\n")
cat("   Limitation: Ignores actual compliance/adherence\n\n")

cat("2. PER-PROTOCOL (Change Score) MODEL:\n")
cat("   Model: lmer(ilr1 ~ intervention_active * playtime_reduction + (1 | id))\n")
cat("   Tests: intervention_active:playtime_reduction interaction\n")
cat("   Logic: Tests if being in intervention AND having playtime reduction affects outcomes\n")
cat("   Issue: This is testing a DIFFERENT hypothesis than ITT!\n\n")

cat("3. PER-PROTOCOL (Robust) MODEL:\n")
cat("   Model: lmer(ilr1 ~ intervention_active * playtime + base_play_mean + (1 | id))\n")
cat("   Tests: intervention_active:playtime interaction\n")
cat("   Logic: Tests if current playtime affects outcomes differently in intervention group\n")
cat("   Issue: Also testing a different hypothesis than ITT\n\n")

cat("=== KEY INSIGHTS FROM THE CODE ===\n\n")

cat("COMPLIANCE IMPLEMENTATION:\n")
cat("- Personal compliance: Beta(2,2) * 0.35 + 0.6 = 60-95% range\n")
cat("- Daily variation: Kumaraswamy distribution adds day-to-day fluctuation\n")
cat("- Effect application: adjusted_effect = effect_min * daily_compliance_rate\n")
cat("- Mean compliance ≈ 77.5%\n\n")

cat("WHY THE POWER DIFFERENCE IS SMALL:\n\n")

cat("1. FUNDAMENTAL ISSUE - DIFFERENT HYPOTHESES:\n")
cat("   - ITT tests: Does random assignment to intervention lead to different outcomes?\n")
cat("   - PP tests: Does actual behavior change (playtime reduction) lead to different outcomes?\n")
cat("   - These are answering different questions!\n\n")

cat("2. HIGH COMPLIANCE MASKS THE DIFFERENCE:\n")
cat("   - With 77.5% average compliance, ITT captures most of the effect\n")
cat("   - The 'dilution' from non-compliance is only ~22.5%\n")
cat("   - So ITT power ≈ PP power * 0.775, but this is still high\n\n")

cat("3. PER-PROTOCOL MODELS ARE SUBOPTIMAL:\n")
cat("   - Change score models have known issues (regression to mean, measurement error)\n")
cat("   - The PP models aren't directly comparable to ITT\n")
cat("   - They test different mechanistic questions\n\n")

cat("4. COMPOSITIONAL DATA CONSTRAINTS:\n")
cat("   - ILR transformation creates complex relationships\n")
cat("   - Random effects structure may absorb compliance variation\n")
cat("   - Small effect sizes relative to measurement noise\n\n")

cat("=== BETTER PER-PROTOCOL APPROACH ===\n\n")
cat("To see larger PP vs ITT differences, consider:\n\n")
cat("1. CACE (Complier Average Causal Effect) Model:\n")
cat("   - Use instrumental variable approach\n")
cat("   - Treatment assignment as instrument for compliance\n")
cat("   - Would show larger effects for actual compliers\n\n")

cat("2. Compliance-Stratified Analysis:\n")
cat("   - Analyze high vs low compliers separately\n")
cat("   - Should show larger effects in high compliance subgroup\n\n")

cat("3. Lower Compliance Scenarios:\n")
cat("   - Test compliance ranges like 30-70%\n")
cat("   - Would create larger ITT vs PP differences\n\n")

cat("4. Different Effect Modeling:\n")
cat("   - Current model: effect proportional to compliance\n")
cat("   - Alternative: threshold effects (need >50% compliance for any benefit)\n")
cat("   - Would create larger differences\n\n")

# Let's also check the actual compliance distribution
cat("=== COMPLIANCE DISTRIBUTION ANALYSIS ===\n")
cat("Beta(2,2) scaled to [0.6, 0.95]:\n")
set.seed(123)
compliance_sim <- rbeta(10000, 2, 2) * 0.35 + 0.6
cat("Mean:", round(mean(compliance_sim), 3), "\n")
cat("Median:", round(median(compliance_sim), 3), "\n")
cat("SD:", round(sd(compliance_sim), 3), "\n")
cat("25th percentile:", round(quantile(compliance_sim, 0.25), 3), "\n")
cat("75th percentile:", round(quantile(compliance_sim, 0.75), 3), "\n")
cat("\nThis shows high, relatively homogeneous compliance,\n")
cat("which reduces ITT vs PP power differences.\n")