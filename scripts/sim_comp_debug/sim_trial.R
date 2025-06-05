sim_trial <- function(
  n_pg              = 50,
  effect_min        = 120,        # sedentary → physical reallocation in ACTIVE arm
  baseline_days     = 7,
  intervention_days = 14,
  s_between         = 0.15,
  s_within          = 0.25,
  seed              = NULL,
  play_sd_prop      = 0.1,      # day-to-day noise as % of personal mean playtime
  corr_noise_sd     = 20,         # small noise around the 1:1 sed vs play reduction
  debug             = FALSE,      # return summary statistics by group/period
  return_dat        = FALSE
){
  if (!is.null(seed)) set.seed(seed)

  ## ── trial layout ───────────────────────────────────────────────────────────
  N        <- n_pg * 2
  grp      <- rep(0:1, each = n_pg)            # 0 = Control, 1 = Intervention
  tot_days <- baseline_days + intervention_days

  ## ── base & intervention compositions (sedentary, sleep, physical) ─────────
  base_comp   <- c(600, 480, 360)
  active_comp <- c(600 - effect_min, 480, 360 + effect_min)

  ## ── person-level random effects in ilr space ───────────────────────────────
  b_ilr <- MASS::mvrnorm(N, mu = c(0, 0), Sigma = diag(s_between^2, 2))

  ## ── containers ─────────────────────────────────────────────────────────────
  all_ids       <- all_periods <- all_days <- NULL
  all_ilr       <- matrix(, 0, 2)
  playmin       <- numeric()

  ## helper: each person's baseline mean playtime (10–40 % of sedentary) ------
  draw_baseline_play <- function() {
    p  <- rbeta(1, 2, 5) * 0.3 + 0.1          # right-skew
    pm <- p * base_comp[1]
    min(max(pm, 10), 240)
  }
  personal_mean_play <- replicate(N, draw_baseline_play())

  ## ── generate day-level data ────────────────────────────────────────────────
  for (i in seq_len(N)) {
    for (period in c("baseline", "intervention")) {

      ndays   <- if (period == "baseline") baseline_days else intervention_days
      comp_mu <- if (period == "baseline" || grp[i] == 0) base_comp else active_comp

      comp_ilr <- comp_to_ilr(matrix(rep(comp_mu, ndays), ncol = 3, byrow = TRUE))
      comp_ilr <- sweep(comp_ilr, 2, b_ilr[i, ], "+")                       # add RE
      day_ilr  <- comp_ilr + MASS::mvrnorm(ndays, mu = c(0, 0),
                                           Sigma = diag(s_within^2, 2))

      ## index bookkeeping -----------------------------------------------------
      all_ids        <- c(all_ids, rep(i, ndays))
      all_periods    <- c(all_periods, rep(period, ndays))
      all_days       <- c(all_days,
                          if (period == "baseline")
                            seq_len(baseline_days)
                          else
                            baseline_days + seq_len(intervention_days))
      all_ilr        <- rbind(all_ilr, day_ilr)

      ## playtime generation ---------------------------------------------------
      base_mean <- personal_mean_play[i]
      if (period == "baseline") {

        playday <- rnorm(ndays, base_mean, play_sd_prop * base_mean)

      } else {                           # intervention period

        if (grp[i] == 0) {               # control arm, unchanged
          playday <- rnorm(ndays, base_mean, play_sd_prop * base_mean)

        } else {                         # active arm, mirror sedentary reduction
          red     <- effect_min + rnorm(ndays, 0, corr_noise_sd)
          playday <- pmax(0, rnorm(ndays,
                                   base_mean - red,
                                   play_sd_prop * base_mean))
        }
      }
      playmin <- c(playmin, playday)
    }
  }

  ## ── back-transform ilr → minutes & assemble data frame ─────────────────────
  mins <- ilr_to_minutes(all_ilr)
  colnames(mins) <- c("sedentary", "sleep", "physical")

  dat  <- tibble::tibble(
           id        = factor(all_ids),
           group     = factor(grp[all_ids], labels = c("Control", "Intervention")),
           period    = factor(all_periods, levels = c("baseline", "intervention")),
           day       = all_days,
           sedentary = mins[, 1],
           sleep     = mins[, 2],
           physical  = mins[, 3],
           playtime  = playmin
         ) |>
         dplyr::group_by(id) |>
         dplyr::mutate(
           base_play_mean      = mean(playtime[period == "baseline"]),
           playtime_reduction  = base_play_mean - playtime,
           sed_reduction       = mean(sedentary[period == "baseline"]) - sedentary,
		   intervention_active = as.integer(group == "Intervention" &
                                     period == "intervention")
         ) |>
         dplyr::ungroup()

  if (return_dat) return(dat)

  ## ── debug mode: summary statistics by group and period ────────────────────
  if (debug) {
    summary_stats <- dat |>
      dplyr::group_by(group, period) |>
      dplyr::summarise(
        n_obs = dplyr::n(),
        n_people = dplyr::n_distinct(id),
        sedentary_mean = round(mean(sedentary), 1),
        sedentary_sd = round(sd(sedentary), 1),
        sleep_mean = round(mean(sleep), 1),
        sleep_sd = round(sd(sleep), 1),
        physical_mean = round(mean(physical), 1),
        physical_sd = round(sd(physical), 1),
        playtime_mean = round(mean(playtime), 1),
        playtime_sd = round(sd(playtime), 1),
        playtime_reduction_mean = round(mean(playtime_reduction), 1),
        playtime_reduction_sd = round(sd(playtime_reduction), 1),
        .groups = "drop"
      )
    
    cat("\n=== COMPOSITIONAL & PLAYTIME SUMMARY BY GROUP/PERIOD ===\n")
    print(summary_stats)
    cat("\n")
  }

  ## ── helper: add ilr coordinates everywhere we need them ────────────────────
  add_ilr <- function(df) {
    result <- dplyr::bind_cols(
      df,
      tibble::as_tibble(
        comp_to_ilr(as.matrix(df[, c("sedentary", "sleep", "physical")]))
      )
    )
    names(result)[(ncol(result) - 1):ncol(result)] <- c("ilr1", "ilr2")
    return(result)
  }

  dat_ilr_all         <- add_ilr(dat)
  dat_ilr_intervention<- dplyr::filter(dat_ilr_all, period == "intervention")

  ## ── mixed models -----------------------------------------------------------
  fit_between      <- lmerTest::lmer(ilr1 ~ group + (1|id), data = dat_ilr_intervention)
  fit_control      <- lmerTest::lmer(ilr1 ~ period + (1|id),
                                  data = dplyr::filter(dat_ilr_all, group == "Control"))
  fit_intervention <- lmerTest::lmer(ilr1 ~ period + (1|id),
                                  data = dplyr::filter(dat_ilr_all, group == "Intervention"))
  fit_interaction  <- lmerTest::lmer(ilr1 ~ group * period + (1|id), data = dat_ilr_all)
  fit_pp           <- lmerTest::lmer(ilr1 ~ intervention_active * playtime_reduction + (1|id),
                                  data = dat_ilr_all)

  ## ── extract p-values -------------------------------------------------------
  p_between       <- anova(fit_between)     ["group",               "Pr(>F)"]
  p_control       <- anova(fit_control)     ["period",              "Pr(>F)"]
  p_intervention  <- anova(fit_intervention)["period",              "Pr(>F)"]
  p_interaction   <- anova(fit_interaction) ["group:period",        "Pr(>F)"]
  p_protocol      <- anova(fit_pp)          ["intervention_active:playtime_reduction", "Pr(>F)"]

  ## ── return -----------------------------------------------------------------
  result <- list(
    p_between_group      = p_between,        # group effect during intervention
    p_within_control     = p_control,        # period effect in control arm
    p_within_intervention= p_intervention,   # period effect in active arm
    p_interaction        = p_interaction,    # group × period (intention-to-treat)
    p_protocol           = p_protocol        # intervention_active × play-reduction (per-protocol)
  )
  
  if (debug) {
    result$summary_stats <- summary_stats
  }
  
  return(result)
}
# set.seed(1234)
# out <- sim_trial(seed = 1234)
# print(out)
