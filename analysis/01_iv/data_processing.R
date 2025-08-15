# Limpiar el espacio de trabajo
rm(list = ls())
options(max.print = 1000)
options(width = 300)

# Verificar instalación y cargar los paquetes requeridos
list_of_packages <- c(
  "arrow", "tidyverse", "geosphere"
)
new_packages <- list_of_packages[
  !(list_of_packages %in% installed.packages()[, "Package"])
]
if (length(new_packages)) {
  install.packages(
    new_packages,
    repos = "http://cran.us.r-project.org"
  )
}
invisible(lapply(list_of_packages, library, character.only = TRUE))

# Establecer el directorio de trabajo
setwd("~/projects/innpulsa-analisis-exploracion/")
raw_input_dir <- "data/innpulsa_raw/"
input_dir <- "data/02_processed/geolocation/"
output_dir <- "data/03_analysis/01_iv/"


# Crear los directorios si no existen
for (dir in c(output_dir, raw_input_dir, input_dir)) {
  if (!dir.exists(dir)) {
    dir.create(dir, recursive = TRUE)
  }
}

# Asignar funciones comunes de dplyr
select <- dplyr::select
summarise <- dplyr::summarise
summarize <- dplyr::summarize
bind_rows <- dplyr::bind_rows

# ------------------------------------------------------------------------------
# CARGANDO DATOS
# ------------------------------------------------------------------------------

# Leer los datos de ambas fuentes
cat("Leyendo los datasets de RUES y ZASCA...\n")
data_with_coords <- read_csv(
  file.path(input_dir, "data_with_coords.csv"),
  locale = locale(encoding = "UTF-8"),
  col_types = cols(.default = col_guess(), `...1` = col_character())
)

# ------------------------------------------------------------------------------
# ASIGNAR COORDENADAS DEL CENTRO ZASCA Y DEL CENTRO CIUDAD
# ------------------------------------------------------------------------------

# make a dictionary from scratch
centros_ciudad <- list(
  "20Julio" = c(4.5711143, -74.0943969),
  "Baranoa" = c(10.7966, -74.9150),
  "Bucaramanga" = c(7.12, -73.1276),
  "Cali Norte" = c(3.4516, -76.5320),
  "Cartagena" = c(10.3932, -75.4832),
  "Caucasia" = c(7.9832, -75.1982),
  "Ciudad Bolivar" = c(4.5795, -74.1574),
  "Cúcuta" = c(7.89391, -72.50782),
  "Manizales" = c(5.0630, -75.5028),
  "Manrique" = c(6.2650487, -75.5536652),
  "Medellín" = c(6.2527, -75.5628),
  "Riohacha" = c(11.5384, -72.9168),
  "Suba" = c(4.7208, -74.0748)
)

centros_zasca <- list(
  "Bucaramanga" = c(7.1049364854763475, -73.12383197704348),
  "Manrique" = c(6.284881727521926, -75.54409932364932),
  "Medellín" = c(6.232088566149681, -75.56902649888393),
  "Cúcuta" = c(7.829409950541552, -72.46036608947021),
  "20Julio" = c(4.569429291819494, -74.09478949758527),
  "Baranoa" = c(10.803854499386958, -74.91244952786113),
  "Cali Norte" = c(3.4703660708293342, -76.53109251974698),
  "Cartagena" = c(10.408413725517383, -75.46504629117649),
  "Caucasia" = c(7.996741312367327, -75.19635027124215),
  "Ciudad Bolivar" = c(4.543213679818289, -74.1469410119057),
  "Manizales" = c(5.063846037654722, -75.50186555759247),
  "Riohacha" = c(11.539682147003058, -72.91511631324943),
  "Suba" = c(4.7461323779336295, -74.08267727408058)
)
# ------------------------------------------------------------------------------
# PIVOTE A FORMATO LONGITUDINAL PARA PANEL
# ------------------------------------------------------------------------------

cat("Pivotando los datos a formato largo para análisis de panel...\n")

# Identificar variables con sufijos de año (2023 y 2024)
year_vars <- c(
  "activos_total", "cantidad_establecimientos", "cantidad_mujeres_empleadas",
  "cantidad_mujeres_en_cargos_direc", "ciiu_principal", "codigo_tamano_empresa",
  "empleados", "ingresos_actividad_ordinaria", "resultado_del_periodo", "state"
)

# Crear el dataset en formato largo
data_long <- data_with_coords %>%
  pivot_longer(
    cols = matches("_(2023|2024)$"),
    names_to = c(".value", "year"),
    names_pattern = "(.+)_(\\d{4})$"
  ) %>%
  mutate(
    year = as.numeric(year)
  ) %>%
  arrange(up_id, year)

cat("Datos pivotados exitosamente a formato largo.\n")
cat("Dimensiones prev.:", nrow(data_with_coords), "filas x", ncol(data_with_coords), "columnas\n")
cat("Nuevas dimensiones:", nrow(data_long), "filas x", ncol(data_long), "columnas\n")
cat("Años en el panel:", paste(sort(unique(data_long$year)), collapse = ", "), "\n")

cat("\nPrimeras filas de los datos transformados:\n")
print(head(data_long, 10))

cat("\nNombres de columnas en formato largo:\n")
print(names(data_long))

# ------------------------------------------------------------------------------
# CALCULAR DISTANCIAS A CENTROS DE CIUDAD Y CENTROS ZASCA
# ------------------------------------------------------------------------------

cat("Calculando distancias a centros de ciudad y centros ZASCA...\n")

# Función para calcular distancia mínima a un conjunto de centros
calcular_distancia_minima <- function(lat, lon, centros_lista) {
  if (is.na(lat) || is.na(lon)) {
    NA
  } else {
    distancias <- sapply(centros_lista, function(centro) {
      distHaversine(c(lon, lat), c(centro[2], centro[1])) / 1000 # nolint
    })
    min(distancias)
  }
}

# Función para obtener el nombre del centro más cercano
obtener_centro_mas_cercano <- function(lat, lon, centros_lista) {
  if (is.na(lat) || is.na(lon)) {
    NA
  }

  distancias <- sapply(centros_lista, function(centro) {
    distHaversine(c(lon, lat), c(centro[2], centro[1])) / 1000 # nolint
  })

  names(centros_lista)[which.min(distancias)]
}

# Función para asignar centro prioritizando el match exacto del campo "centro"
asignar_centro_prio_match <- function(centro_exacto, lat, lon, centros_lista) {
  # Si hay un centro exacto disponible y está en la lista de centros, usarlo
  if (!is.na(centro_exacto) && centro_exacto %in% names(centros_lista)) {
    centro_exacto
  } else {
    # Si no hay centro exacto o no está en la lista, calcular el más cercano
    obtener_centro_mas_cercano(lat, lon, centros_lista)
  }
}

# Función para calcular distancia a un centro específico
calcular_dist_centro <- function(centro_asignado, lat, lon, centros_lista) {
  if (is.na(lat) || is.na(lon) || is.na(centro_asignado)) {
    NA
  } else if (centro_asignado %in% names(centros_lista)) {
    centro_coords <- centros_lista[[centro_asignado]]
    distHaversine(c(lon, lat), c(centro_coords[2], centro_coords[1])) / 1000 # nolint
  } else {
    NA
  }
}

# Aplicar cálculos a los datos
data_long <- data_long %>%
  mutate(
    # Centro de ciudad más cercano (prioritizando match exacto del campo "centro")
    centro_ciudad_cercano = pmap_chr(
      list(centro, latitude, longitude),
      ~ asignar_centro_prio_match(..1, ..2, ..3, centros_ciudad)
    ),

    # Distancia al centro de ciudad asignado
    distancia_centro_ciudad = pmap_dbl(
      list(centro_ciudad_cercano, latitude, longitude),
      ~ calcular_dist_centro(..1, ..2, ..3, centros_ciudad)
    ),

    # Centro ZASCA más cercano (prioritizando match exacto del campo "centro")
    centro_zasca_cercano = pmap_chr(
      list(centro, latitude, longitude),
      ~ asignar_centro_prio_match(..1, ..2, ..3, centros_zasca)
    ),

    # Distancia al centro ZASCA asignado
    distancia_centro_zasca = pmap_dbl(
      list(centro_zasca_cercano, latitude, longitude),
      ~ calcular_dist_centro(..1, ..2, ..3, centros_zasca)
    )
  )


# summary of distances
cat("\nEstadísticas de distancias calculadas:\n")
cat("Distancia a centro de ciudad (km):\n")
print(summary(data_long$distancia_centro_ciudad))

cat("\nDistancia a centro ZASCA (km):\n")
print(summary(data_long$distancia_centro_zasca))

cat("\nDistribución de centros de ciudad más cercanos:\n")
print(table(data_long$centro_ciudad_cercano, useNA = "ifany"))

cat("\nDistribución de centros ZASCA más cercanos:\n")
print(table(data_long$centro_zasca_cercano, useNA = "ifany"))

# Diagnostic information about exact matches vs. closest center calculations
cat("\nDiagnóstico de asignación de centros:\n")
cat("Observaciones con centro exacto disponible:", sum(!is.na(data_long$centro)), "\n")
cat(
  "Observaciones con centro exacto que coincide con centros disponibles:",
  sum(!is.na(data_long$centro) & data_long$centro %in% names(centros_ciudad)), "\n"
)
cat(
  "Observaciones que usan centro más cercano (centro_ciudad):",
  sum(is.na(data_long$centro) | !(data_long$centro %in% names(centros_ciudad))), "\n"
)
cat(
  "Observaciones que usan centro más cercano (centro_zasca):",
  sum(is.na(data_long$centro) | !(data_long$centro %in% names(centros_zasca))), "\n"
)

# david: algunos centros no coinciden con los centros disponibles?
centros_disponibles <- unique(data_long$centro[!is.na(data_long$centro)])
centros_no_match_ciudad <- centros_disponibles[!(centros_disponibles %in% names(centros_ciudad))]
centros_no_match_zasca <- centros_disponibles[!(centros_disponibles %in% names(centros_zasca))]

cat("\nCentros exactos disponibles que NO coinciden con centros_ciudad:")
if (length(centros_no_match_ciudad) > 0) {
  print(centros_no_match_ciudad)
} else {
  cat("OK")
}

cat("\nCentros exactos disponibles que NO coinciden con centros_zasca:")
if (length(centros_no_match_zasca) > 0) {
  print(centros_no_match_zasca)
} else {
  cat("OK")
}

cat("\nDistribución de todos los centros exactos disponibles:\n")
print(table(data_long$centro, useNA = "ifany"))

# quitar centros a más de 50 km de distancia
data_long <- data_long %>%
  filter(distancia_centro_ciudad <= 50) %>%
  filter(distancia_centro_zasca <= 50)

# Guardar el resultado en data/analysis/01_iv/data_long.csv
write.csv(
  data_long, file.path(
    output_dir, "data_long.csv"
  ),
  row.names = FALSE, fileEncoding = "UTF-8"
)
