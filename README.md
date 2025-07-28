# Análisis exploratorio de métodos cuasi-experimentales para ZASCA

Este repositorio contiene scripts para procesar conjuntos de datos RUES y ZASCA, junto con código fuente de apoyo para operaciones de carga y procesamiento de datos.

## Estructura

- `scripts/` - Scripts de procesamiento de datos
- `src/innpulsa/` - Paquete principal con cargadores, procesadores y utilidades
- `scripts/geolocation/` - Scripts adicionales de procesamiento de geolocalización

## Ejecución de los scripts

### Requisitos previos

- Entorno Python con pandas instalado
- Archivos de datos en bruto en el directorio de datos configurado

### Procesamiento de Datos

**Procesar datos RUES:**
```bash
python scripts/create_rues_clean_data.py
```

Este script carga conjuntos de datos RUES, los combina con datos de búsqueda de códigos postales, procesa el conjunto de datos combinado y guarda los datos limpios en `data/processed/rues_total.csv`.

**Procesar datos ZASCA:**
```bash
python scripts/create_zasca_clean_data.py
```

Este script carga archivos de cohortes ZASCA, los procesa, los cruza con datos RUES para crear indicadores de coincidencia y guarda el conjunto de datos mejorado en `data/processed/zasca_total.csv`.

### Pipeline de geolocalización

**Procesar direcciones RUES:**
```bash
python scripts/geolocation/rues/process_addresses.py --target 520
```

Procesa direcciones comerciales RUES utilizando LLM (Gemini) para estandarizar nombres de calles.

**Procesar direcciones ZASCA:**
```bash
python scripts/geolocation/zasca/process_addresses.py
```

Procesa direcciones ZASCA utilizando LLM para estandarización y limpieza.

**Geocodificar direcciones:**
```bash
# Usando Google Maps API
python scripts/geolocation/geocode_addresses.py --service google --dataset rues
python scripts/geolocation/geocode_addresses.py --service google --dataset zasca

# Usando Nominatim (OpenStreetMap)
python scripts/geolocation/geocode_addresses.py --service nominatim --dataset rues
python scripts/geolocation/geocode_addresses.py --service nominatim --dataset zasca
```

Geocodifica direcciones procesadas utilizando Google Maps API o Nominatim. Para Google Maps, requiere la variable de entorno `GMAPS_API_KEY`.

**Comparar coordenadas:**
```bash
python scripts/geolocation/compare_coordinates.py
```

Compara coordenadas obtenidas de Google Maps y Nominatim para validar resultados de geocodificación.

**Fusionar datos geocodificados:**
```bash
python scripts/geolocation/merge_rues_zasca.py
```

Fusiona datos RUES y ZASCA con coordenadas geocodificadas para crear un conjunto de datos unificado.

### Dependencias

Los scripts dependen del paquete `innpulsa` que proporciona:

- **Cargadores de datos** (`innpulsa.loaders`) - Funciones para cargar datos RUES, ZASCA y códigos postales
- **Módulos de procesamiento** (`innpulsa.processing`) - Funciones de limpieza y transformación de datos
- **Geolocalización** (`innpulsa.geolocation`) - Procesamiento de direcciones con LLM y geocodificación
- **Configuración** (`innpulsa.settings`) - Rutas de directorios de datos y configuraciones
- **Registro** (`innpulsa.logging`) - Configuración de registro estructurado

#### Requisitos adicionales para geolocalización

- **API de Google Maps** - Clave API requerida en variable de entorno `GMAPS_API_KEY` para geocodificación con Google
- **pandas, geopy** - Para manipulación de datos y geocodificación
- **Acceso a LLM Gemini** - Para procesamiento inteligente de direcciones

### Salida de datos

Los scripts crean archivos CSV procesados en el directorio `data/processed/` con codificación UTF-8:

- **Datos principales:** `rues_total.csv`, `zasca_total.csv`
- **Datos geocodificados:** `geolocation/rues_coordinates.csv`, `geolocation/zasca_coordinates.csv`
- **Direcciones procesadas:** `geolocation/rues_addresses.csv`, `geolocation/zasca_addresses.csv`
- **Comparaciones:** `geolocation/zasca_coordinates_comparison.csv`
- **Datos fusionados:** `geolocation/rues_total_merged.csv` 