
# c_comma -----------------------------------------------------------------

c_comma <- function(x, accuracy = 1, big_mark = ".", decimal_mark = ",", ...) {
  
  comma(
    x,
    accuracy = accuracy,
    big.mark = big_mark,
    decimal.mark = decimal_mark,
    ...
  )
}

# c_percent ---------------------------------------------------------------

c_percent <- function(
    x,.
    accuracy = 1,
    suffix = " %",
    big_mark = ".",
    decimal_mark = ",",
    ...
) {
  
  percent(
    x,
    accuracy = accuracy,
    suffix = suffix,
    big.mark = big_mark,
    decimal.mark = decimal_mark,
    ...
  )
}

# c_dollar ----------------------------------------------------------------

c_dollar <- function(
    x,
    accuracy = 1,
    scale = 1e-6,
    big_mark = ".",
    decimal_mark = ",",
    ...
) {
  
  dollar(
    x,
    accuracy = accuracy,
    scale = scale,
    big.mark = big_mark,
    decimal.mark = decimal_mark,
    ...
  )
}

# avg_over ----------------------------------------------------------------

avg_over <- function(x, y, d) {
  slide_index_mean(x, y, before = days(d), after = -1L, complete = TRUE)
}

# n_over ------------------------------------------------------------------

n_over <- function(x, y, d) {
  slide_index_int(
    x,
    y,
    length,
    .before = days(d),
    .after = -1L,
    .complete = TRUE
  )
}

# cm_heat_custom ----------------------------------------------------------

cm_heat_custom <- function(x) 
{
  df <- as.data.frame.table(x[["table"]])
  names(df) <- c("Prediction", "Truth", "Freq")
  lvls <- levels(df[["Prediction"]])
  df[["Prediction"]] <- factor(df[["Prediction"]], levels = rev(lvls))
  axis_labels <- yardstick:::get_axis_labels(x)
  df |> 
    ggplot(aes(x = Truth, y = Prediction, fill = factor(Freq))) +
    geom_tile() + 
    scale_fill_brewer(palette = "Greens") +
    theme(panel.background = element_blank(), legend.position = "none") +
    geom_text(mapping = aes(label = c_comma(Freq))) +
    labs(x = axis_labels[["x"]], y = axis_labels[["y"]])
}

# ggplot_imp --------------------------------------------------------------

ggplot_imp <- function(...) {
  obj <- list(...)
  # metric_name <- attr(obj[[1]], "loss_name")
  metric_lab <- paste(
    # metric_name,
    # "after permutations\n(higher indicates more important)",
    "Indicador luego de permutaciones\n(mayor significa más importante)"
  )
  
  full_vip <- bind_rows(obj) |>
    filter(variable != "_baseline_")
  
  perm_vals <- full_vip |>
    filter(variable == "_full_model_") |>
    group_by(label) |>
    summarise(dropout_loss = mean(dropout_loss, na.rm = TRUE))
  
  p <- full_vip |>
    filter(variable != "_full_model_") |>
    mutate(variable = fct_reorder(variable, dropout_loss)) |>
    ggplot(aes(dropout_loss, variable))
  
  if (length(obj) > 1) {
    p <- p +
      facet_wrap(vars(label)) +
      geom_vline(
        data = perm_vals,
        aes(xintercept = dropout_loss, color = label),
        linewidth = 1.5,
        lty = 2,
        alpha = 0.75
      ) +
      geom_boxplot(aes(color = label, fill = label), alpha = 0.25)
  } else {
    p <- p +
      geom_vline(
        data = perm_vals,
        aes(xintercept = dropout_loss),
        linewidth = 1.5,
        lty = 2,
        alpha = 0.75
      ) +
      geom_boxplot(fill = "#91CBD765", color = "gray50", alpha = 0.5)
  }
  
  p +
    theme(legend.position = "none") +
    labs(
      x = metric_lab,
      y = NULL,
      fill = NULL,
      color = NULL,
      title = "Importancia de Características"
    )
}

# ggplot_pdp --------------------------------------------------------------

ggplot_pdp <- function(obj, x, profiles = FALSE) {
  p <- as_tibble(obj[["agr_profiles"]]) |>
    mutate(`_label_` = str_remove(`_label_`, "^[^_]*_")) |>
    ggplot(aes(`_x_`, `_yhat_`))
  
  if (isTRUE(profiles)) {
    p <- p +
      geom_line(
        data = as_tibble(obj[["cp_profiles"]]),
        aes(x = {{ x }}, group = `_ids_`),
        linewidth = 0.5,
        alpha = 0.05,
        color = "gray50"
      )
  }
  
  num_colors <- n_distinct(obj[["agr_profiles"]][["_label_"]])
  
  if (num_colors > 1) {
    p <- p +
      geom_line(aes(color = `_label_`), linewidth = 1.25, alpha  = 0.75) +
      scale_color_brewer(palette = "Set1")
  } else {
    p <- p +
      geom_line(color = "midnightblue", linewidth = 1.25, alpha = 0.75)
  }
  
  p +
    labs(
      x = englue("{{ x }}"),
      y = "Predicción Promedio",
      color = "Grupo",
      title = "Perfil de Dependencia Parcial"
    )
}

# ggplot_local ------------------------------------------------------------

ggplot_local <- function(df, x_ids) {
  df |>
    mutate(mean_val = mean(contribution, na.rm = TRUE), by = "variable") |>
    filter(!str_detect(variable, str_flatten(x_ids, "|"))) |> 
    mutate(variable = fct_reorder(variable, abs(mean_val))) |>
    ggplot(aes(contribution, variable, fill = mean_val > 0)) +
    geom_col(
      data = \(x) distinct(x, variable, mean_val),
      aes(mean_val, variable),
      alpha = 0.5
    ) +
    geom_boxplot(width = 0.5) +
    theme(legend.position = "none") +
    scale_fill_manual(values = c("TRUE" = "forestgreen", "FALSE" = "red")) +
    labs(x = "Contribución", y = NULL, title = "Explicación Local")
}
