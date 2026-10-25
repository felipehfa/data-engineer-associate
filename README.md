# data-engineer-associate

Proyecto de práctica para el examen **Databricks Certified Data Engineer Associate**: un pipeline Medallion (Bronze → Silver → Gold) sobre un dataset sintético de e-commerce, construido en Databricks con Unity Catalog, Auto Loader y Lakeflow Declarative Pipelines.

El objetivo no es el dataset, sino ejercitar cada tema del examen con un caso concreto: ingesta incremental, tipado, JSON anidado, historial con SCD Type 2 y, después, orquestación y gobernanza.

## Arquitectura

```
Fuente (CSV + JSON sintéticos, Faker con semillas fijas)
   │
   ▼  01_landing   archivos crudos en Volumes
   │
   ▼  COPY INTO (batch) / Auto Loader (streaming)
   │
   ▼  02_bronze    copia fiel de la fuente, todo como STRING
   │
   ▼  casteo, from_json() + explode(), AUTO CDC
   │
   ▼  03_silver    datos tipados, order_items aplanado, historial de clientes
   │
   ▼  04_gold      vistas materializadas para analítica (pendiente)
```

Los checkpoints de streaming viven en `05_ops`, que no es una capa de datos.

## Qué hay en el repo

| Archivo | Qué hace | Tema del examen |
|---|---|---|
| `01 Bronze.ipynb` | Carga stores y products con `COPY INTO` y 500.000 órdenes con Auto Loader | Ingesta incremental |
| `02 Silver.ipynb` | Casteo de tipos, `from_json()` + `explode()` para `order_items`, deduplicación y chequeos de calidad | Transformaciones en Spark SQL y PySpark |
| `customers_scd2_pipeline/` | Pipeline declarativo: Auto Loader a Bronze y `AUTO CDC` (SCD Type 2) a Silver | Lakeflow Declarative Pipelines |

## Decisiones que vale la pena mirar

- **Bronze es siempre `STRING`.** No se castea ni se valida: el tipado ocurre en Silver, así Bronze sigue siendo una copia fiel de lo que llegó.
- **`AUTO CDC` en vez de `MERGE` manual para SCD2.** Un solo `MERGE` no puede cerrar la versión vieja e insertar la nueva en la misma ejecución, y el patrón manual de dos sentencias es vulnerable a comparaciones con NULL, varios cambios del mismo cliente en un lote y eventos fuera de orden. `AUTO CDC` lo resuelve con `sequence_by`.
- **Dos snapshots completos, no un feed de cambios.** `customers_v1` y `customers_v2` son fotos del mismo CRM en fechas distintas: los clientes sin cambios no generan versión nueva y solo el ~10 % que cambió queda con dos.
- **Nombres calificados con su esquema** (`` `02_bronze`.customers_bronze ``): el pipeline escribe en varias capas sin depender del esquema destino de la interfaz.

## Estado

- [x] Datos sintéticos en `01_landing`
- [x] Bronze: stores, products y orders (Auto Loader)
- [x] Silver: stores, products, order_items y orders
- [x] Silver: customers con `AUTO CDC` (SCD Type 2)
- [ ] Gold: vistas materializadas (ventas por región, producto y mes)
- [ ] Lakeflow Job de punta a punta
- [ ] CI/CD con Databricks Asset Bundles
- [ ] Skew: provocarlo y diagnosticarlo en Spark UI
- [ ] Gobernanza: `GRANT`/`REVOKE`, column masking y row-level security

## Relación con otros proyectos

Es el campo de pruebas de las técnicas que uso en proyectos reales:

- [santiago-rentals-elt](https://github.com/felipehfa/santiago-rentals-elt): Medallion sobre Postgres, con SCD2 en Gold.
- [gran-concepcion-databricks](https://github.com/felipehfa/gran-concepcion-databricks): la versión Databricks del proyecto de arriendos de Concepción.

## Licencia

[MIT](LICENSE).
