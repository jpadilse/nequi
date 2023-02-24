
# Librerias ---------------------------------------------------------------

library(applicable)
library(viridis)
library(corrplot)
library(arrow)
library(bonsai)
library(DALEXtra)
library(doFuture)
library(rlang)
library(forcats)
library(stringr)
library(fs)
library(janitor)
library(parallel)
library(probably)
library(qs)
library(readr)
library(themis)
library(tidymodels)
library(ggthemes)
library(scales)
library(kableExtra)

tidymodels_prefer(quiet = FALSE)
conflict_prefer("explain", "DALEX")

# Fijar semilla para el generador de números aleatorios -------------------

set.seed(4378)

# Tema de visualización ---------------------------------------------------

theme_set(theme_economist_white(gray_bg = FALSE))

# Programación en paralelo ------------------------------------------------

# La mayoría de los aumentos producidos por el procesamiento en paralelo ocurren 
# cuando el procesamiento usa menos que el número de núcleos físicos disponibles

all_cores <- 5 # detectCores(logical = FALSE)
registerDoFuture()
cl <- makeCluster(floor(all_cores * 0.7))
plan(cluster, workers = cl)

# Importar datos ----------------------------------------------------------

exp_training_data <- read_parquet("./data/silver/data_sheet.gz.parquet")

raw_data <- rename(exp_training_data, Y_VAR = FRAUDULENT)

# Parámetros --------------------------------------------------------------

y <- "Y_VAR"
y_event <- "Si"
x_ids <- c("ID", "ACCOUNT_NUMBER", "USER_ID")
x <- setdiff(names(raw_data), c(y, x_ids))
x_date <- "TRANSACTION_DATE" 

# Dividir datos -----------------------------------------------------------

data_split <- initial_split(raw_data, prop = 0.8, strata = !!y)
data_training <- training(data_split)
data_test <- testing(data_split)

# Preprocesamiento --------------------------------------------------------

# Flujo de transformación -------------------------------------------------

main_rec <- recipe(data_training) |>
  update_role(!!y, new_role = "outcome") |>
  update_role(!!x_ids, new_role = "ID") |>
  update_role(!!x, new_role = "predictor") |>
  step_date(!!x_date, features = c("dow", "month")) |> 
  step_novel(all_nominal_predictors(), new_level = "(Nuevo)") |>
  step_other(all_nominal_predictors(), other = "(Otro)") |>
  step_nzv(all_numeric_predictors()) |>
  step_corr(all_numeric_predictors()) |>
  step_downsample(!!y, under_ratio = 4)

# Aplicar flujo -----------------------------------------------------------

prepped_rec <- prep(main_rec, verbose = TRUE, log_changes = TRUE)
baked_train <- bake(prepped_rec, new_data = NULL)
baked_test <- bake(prepped_rec, new_data = data_test)

qsave(prepped_rec, "./data/silver/prepped_rec.qs")

# Detalles del modelo -----------------------------------------------------

main_model <- boost_tree(
  trees = tune(),
  learn_rate = tune(),
  tree_depth = tune(),
  min_n = tune()
) |>
  set_engine("lightgbm") |>
  set_mode("classification")

model_workflow <- workflow() |>
  add_model(main_model) |>
  add_recipe(main_rec) 

# Optimización de Hiperparámetros -----------------------------------------

folds <- vfold_cv(data_training, strata = !!y, v = 10, repeats = 5)

main_grid <- grid_max_entropy(
  trees(),
  min_n(),
  tree_depth(),
  learn_rate(),
  size = 100
)

metrics_models <- metric_set(roc_auc, j_index)

main_tuning <- model_workflow |>
  tune_grid(
    resamples = folds,
    grid = main_grid,
    control = control_grid(save_pred = TRUE),
    metrics = metrics_models
  )

qsave(main_tuning, "./data/silver/main_tuning.qs")

main_tuning_bayes <- model_workflow |>
  tune_bayes(
    resamples = folds,
    metrics = metrics_models,
    initial = main_tuning,
    iter = 100,
    control = control_bayes(save_pred = TRUE)
  )

qsave(main_tuning_bayes, "./data/silver/main_tuning_bayes.qs")

main_best <- select_best(main_tuning_bayes, metric = "roc_auc")

main_pred_val <- main_tuning_bayes |>
  collect_predictions(parameters = main_best) |>
  roc_curve(all_of(str_glue(".pred_{y_event}")), truth = Y_VAR) 

# Modelo final ------------------------------------------------------------

final_workflow <- finalize_workflow(model_workflow, main_best)
last_fit_main <- last_fit(final_workflow, data_split)
model_pred <- collect_predictions(last_fit_main)

qsave(last_fit_main, "./data/silver/last_fit_main.qs")

# Incertidumbre -----------------------------------------------------------

threshold_data <- model_pred |>
  threshold_perf(
    all_of(y),
    all_of(str_glue(".pred_{y_event}")),
    thresholds = seq(0, 1, by = 0.001)
  ) |>
  filter(.metric != "distance") |>
  mutate(
    group = case_when(
      .metric %in% c("sens", "spec") ~ "1",
      TRUE ~ "2"
    )
  )

max_j_index_threshold <- threshold_data |>
  filter(.metric == "j_index") |>
  filter(.estimate == max(.estimate)) |>
  pull(.threshold)

test_pred <- model_pred |>
  mutate(
    .pred_with_eqz = make_two_class_pred(
      .data[[str_glue(".pred_{y_event}")]],
      levels(.data[[y]]),
      threshold = max_j_index_threshold,
      buffer = 0.05
    )
  )

# Expliación del modelo ---------------------------------------------------

model_fitted <- fit(final_workflow, data = data_training)

explainer_main <- explain_tidymodels(
  model_fitted,
  data = select(data_training, all_of(c(x, x_ids))),
  y = data_training |>
    mutate(Y_VAR = if_else(Y_VAR == y_event, 1L, 0L)) |>
    pull(Y_VAR),
  predict_function_target_column = y_event,
  label = "LightGBM"
)

vip_model <- model_parts(explainer_main, variables = x)

pdp_main <- model_profile(
  explainer_main,
  N = 1000,
  variables = "N_TRANSACTIONS_30D_ACCOUNT",
  groups = "MERCHANT_ID"
)

new_data_main <- data_training |>
  select(-Y_VAR) |>
  slice_sample(n = 1)

shap_main <- predict_parts(explainer_main, new_data_main, type = "shap", B = 20)

# Applicability domain ----------------------------------------------------

# num_predictors <- prepped_rec[["var_info"]] |>
#   filter(type == "numeric", role == "predictor") |>
#   pull(variable)
# 
# pca_stat <- data_training |>
#   select(!!num_predictors) |>
#   drop_na() |>
#   apd_pca()
# 
# score(pca_stat, select(data_test, !!num_predictors)) |>
#   select(starts_with("distance"))

stopCluster(cl) # Stop parallel computing
