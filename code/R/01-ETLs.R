
# Librerías ---------------------------------------------------------------

library(dplyr)
library(tidyr)
library(lubridate)
library(ggplot2)
library(stringr)
library(slider)
library(arrow)
library(skimr)
library(janitor)
library(MASS)
library(robustbase)
library(conflicted)

conflict_prefer("filter", "dplyr")
conflict_prefer("select", "dplyr")

# Importar datos brutos ---------------------------------------------------

raw_data <- read_parquet("./data/bronze/sample_data_0006_part_00.parquet") |> 
  clean_names(case = "all_caps")

# raw_data |> 
#   skim_without_charts()

# Eliminar inconsistencias ------------------------------------------------

pre_data <- raw_data |> 
  filter(year(TRANSACTION_DATE) == 2021) |> 
  distinct(ID, .keep_all = TRUE) |> 
  select(
    ID,
    ACCOUNT_NUMBER,
    USER_ID,
    MERCHANT_ID,
    SUBSIDIARY,
    TRANSACTION_DATE,
    TRANSACTION_TYPE,
    TRANSACTION_AMOUNT,
  ) |> 
  arrange(TRANSACTION_DATE)

# Validar Y ---------------------------------------------------------------

stage_data <- pre_data |> 
  mutate(
    AVG_AMOUNT_P_TRANSACTION_1D_ACCOUNT = avg_over(
      TRANSACTION_AMOUNT, 
      TRANSACTION_DATE, 
      1
    ),
    N_TRANSACTIONS_1D_ACCOUNT = n_over(TRANSACTION_AMOUNT, TRANSACTION_DATE, 1),
    .by = "ACCOUNT_NUMBER"
  ) |> 
  mutate(
    AVG_AMOUNT_P_TRANSACTION_1D_USER = avg_over(
      TRANSACTION_AMOUNT, 
      TRANSACTION_DATE, 
      1
    ),
    N_TRANSACTIONS_1D_USER = n_over(TRANSACTION_AMOUNT, TRANSACTION_DATE, 1),
    .by = "USER_ID"
  ) |> 
  mutate(
    across(
      AVG_AMOUNT_P_TRANSACTION_1D_ACCOUNT:N_TRANSACTIONS_1D_USER,
      \(x) if_else(is.nan(x), 0L, x)
    )
  ) |> 
  drop_na(AVG_AMOUNT_P_TRANSACTION_1D_ACCOUNT:N_TRANSACTIONS_1D_USER) 

stage_matrix <- stage_data |> 
  select(AVG_AMOUNT_P_TRANSACTION_1D_ACCOUNT:N_TRANSACTIONS_1D_USER)
  
robust_mcd <- covMcd(stage_matrix)

distances <- mahalanobis(
  x = stage_matrix, 
  center = robust_mcd[["center"]], 
  cov = ginv(robust_mcd[["cov"]]),
  inverted = TRUE
)

cutoff <- qchisq(p = 0.95, df = ncol(stage_matrix))

pre_y <- stage_data[distances > cutoff, ]

total_y <- pre_y |> 
  filter(N_TRANSACTIONS_1D_ACCOUNT > 5 | N_TRANSACTIONS_1D_USER > 5)

true_y <- stage_data |> 
  semi_join(total_y, join_by(ACCOUNT_NUMBER)) |> 
  mutate(FORMATED_DATE = as.character(TRANSACTION_DATE)) |> 
  mutate(
    FIRST_FRAUDULENT_TRANSACTION = slide_index_chr(
      FORMATED_DATE,
      TRANSACTION_DATE,
      first,
      .before = days(1),
      .after = -1L,
      .complete = TRUE
    ),
    .by = "ACCOUNT_NUMBER"
  )

final_y <- pre_data |> 
  mutate(TRANSACTION_DATE = as.character(TRANSACTION_DATE)) |> 
  semi_join(
    true_y |> 
      select(ID, ACCOUNT_NUMBER, FIRST_FRAUDULENT_TRANSACTION) |> 
      semi_join(total_y, join_by(ID)) |> 
      filter(!is.na(FIRST_FRAUDULENT_TRANSACTION)),
    join_by(ACCOUNT_NUMBER, TRANSACTION_DATE == FIRST_FRAUDULENT_TRANSACTION)
  ) |> 
  select(ID)

# Creación de matriz de características -----------------------------------

features_data <- pre_data |>
  semi_join(stage_data, join_by(ID)) |>
  filter(n() > 5, .by = "ACCOUNT_NUMBER") |> 
  mutate(
    across(
      TRANSACTION_AMOUNT,
      list(
        AVG_AMOUNT_P_TRANSACTION_1D_ACCOUNT = ~ avg_over(.x, TRANSACTION_DATE, 1),
        AVG_AMOUNT_P_TRANSACTION_3D_ACCOUNT = ~ avg_over(.x, TRANSACTION_DATE, 3),
        AVG_AMOUNT_P_TRANSACTION_7D_ACCOUNT = ~ avg_over(.x, TRANSACTION_DATE, 7),
        AVG_AMOUNT_P_TRANSACTION_15D_ACCOUNT = ~ avg_over(.x, TRANSACTION_DATE, 15),
        AVG_AMOUNT_P_TRANSACTION_30D_ACCOUNT = ~ avg_over(.x, TRANSACTION_DATE, 30),
        N_TRANSACTIONS_1D_ACCOUNT = ~ n_over(.x, TRANSACTION_DATE, 1),
        N_TRANSACTIONS_3D_ACCOUNT = ~ n_over(.x, TRANSACTION_DATE, 3),
        N_TRANSACTIONS_7D_ACCOUNT = ~ n_over(.x, TRANSACTION_DATE, 7),
        N_TRANSACTIONS_15D_ACCOUNT = ~ n_over(.x, TRANSACTION_DATE, 15),
        N_TRANSACTIONS_30D_ACCOUNT = ~ n_over(.x, TRANSACTION_DATE, 30)
      ),
      .names = "{.fn}"
    ),
    .by = "ACCOUNT_NUMBER"
  ) 

data_sheet <- features_data |> 
  select(
    ID,
    ACCOUNT_NUMBER,
    USER_ID,
    MERCHANT_ID,
    TRANSACTION_DATE,
    TRANSACTION_TYPE,
    TRANSACTION_AMOUNT,
    AVG_AMOUNT_P_TRANSACTION_1D_ACCOUNT,
    AVG_AMOUNT_P_TRANSACTION_3D_ACCOUNT,
    AVG_AMOUNT_P_TRANSACTION_7D_ACCOUNT,
    AVG_AMOUNT_P_TRANSACTION_15D_ACCOUNT,
    AVG_AMOUNT_P_TRANSACTION_30D_ACCOUNT,
    N_TRANSACTIONS_1D_ACCOUNT,
    N_TRANSACTIONS_3D_ACCOUNT,
    N_TRANSACTIONS_7D_ACCOUNT,
    N_TRANSACTIONS_15D_ACCOUNT,
    N_TRANSACTIONS_30D_ACCOUNT
  ) |>
  mutate(
    FRAUDULENT = if_else(ID %in% final_y[["ID"]], "Si", "No"),
    FRAUDULENT = factor(FRAUDULENT, c("Si", "No")),
    MERCHANT_ID = factor(MERCHANT_ID),
    TRANSACTION_TYPE = factor(TRANSACTION_TYPE)
  )

sub_data <- data_sheet |> 
  filter(FRAUDULENT == "No") |> 
  slice_sample(prop = 0.02) |> 
  bind_rows(filter(data_sheet, FRAUDULENT == "Si"))

# Exportar sabana de datos ------------------------------------------------

sub_data |> 
  write_parquet(
    "./data/silver/data_sheet.gz.parquet",
    compression = "gzip",
    compression_level = 9
  )
