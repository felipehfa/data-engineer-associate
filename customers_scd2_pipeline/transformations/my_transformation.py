# ============================================================================
# PIPELINE: customers_scd2_pipeline
# ============================================================================
#
# PROPÓSITO
# Mantener el historial completo de cambios en los datos de clientes
# (SCD Type 2), usando AUTO CDC de Lakeflow Declarative Pipelines.
#
# POR QUÉ ESTE PIPELINE Y NO UN NOTEBOOK NORMAL
# AUTO CDC (apply_changes) solo existe dentro del motor de Lakeflow
# Declarative Pipelines -- no se puede usar con spark.sql()/.write()
# en un notebook normal. Un MERGE manual en SQL clásico puede lograr
# lo mismo, pero requiere dos sentencias (cerrar + insertar) y es
# fácil introducir bugs sutiles (ver nota más abajo).
#
# FLUJO DE DATOS
#   01_landing/dim_tables/customers_*.csv   (cualquier snapshot presente)
#           │
#           ▼  Auto Loader (detecta archivos automáticamente,
#           │  filtrados con pathGlobFilter = "customers_*.csv")
#           ▼
#   02_bronze.customers_bronze              (copia cruda, sin transformar)
#           │
#           ▼  apply_changes (AUTO CDC, stored_as_scd_type=2)
#           ▼
#   03_silver.customers                     (historial completo, con
#                                             __START_AT / __END_AT)
#
# NOTA SOBRE LOS DATOS DE ORIGEN
# customers_v1.csv y customers_v2.csv son dos SNAPSHOTS COMPLETOS del
# mismo CRM, tomados en fechas distintas (no un feed incremental real
# de solo-los-cambios). AUTO CDC compara el contenido real entre
# snapshots: los clientes sin cambios NO generan una versión nueva,
# solo el ~10% que sí cambió queda con 2 versiones en el historial.
#
# POR QUÉ AUTO CDC EN VEZ DE UN MERGE MANUAL
# Un MERGE de una sola sentencia (WHEN MATCHED ... WHEN NOT MATCHED)
# no puede cerrar la versión vieja E insertar la nueva en la misma
# ejecución -- cada fila de origen solo puede caer en una rama.
# El patrón manual correcto necesita dos sentencias separadas y es
# vulnerable a: comparaciones NULL-unsafe, múltiples cambios al mismo
# key en un batch, y eventos fuera de orden. AUTO CDC resuelve todo
# esto de forma declarativa con sequence_by y stored_as_scd_type=2.
#
# ESQUEMAS DESTINO
# Cada tabla se califica explícitamente con su esquema en el nombre
# (`02_bronze`.customers_bronze / `03_silver`.customers) para que el
# pipeline pueda escribir en varias capas del medallion, sin depender
# del esquema "Destination" configurado por defecto en la UI.
#
# ============================================================================

import dlt
from pyspark.sql.functions import col

CATALOG = "data-engineer-associate"

# ------------------------------------------------------------------
# BRONZE -- el nombre incluye el esquema explícitamente
# ------------------------------------------------------------------
@dlt.table(
    name="`02_bronze`.customers_bronze",   # <- esquema explícito en el nombre
    comment="Todos los snapshots de customers detectados automáticamente en landing"
)
def customers_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"/Volumes/{CATALOG}/05_ops/checkpoints/customers_schema")
        .option("header", "true")
        .option("pathGlobFilter", "customers_*.csv")
        .load(f"/Volumes/{CATALOG}/01_landing/dim_tables/")
    )


# ------------------------------------------------------------------
# SILVER -- distinto esquema, mismo pipeline
# ------------------------------------------------------------------
dlt.create_streaming_table(
    name="`03_silver`.customers",          # <- esquema explícito aquí también
    comment="Historial completo de clientes -- SCD Type 2"
)

dlt.apply_changes(
    target="`03_silver`.customers",
    source="`02_bronze`.customers_bronze",  # referencia también calificada
    keys=["customer_id"],
    sequence_by=col("_updated_at"),
    stored_as_scd_type=2
)