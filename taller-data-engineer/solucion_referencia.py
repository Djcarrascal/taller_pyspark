"""Solución de referencia del Taller Spark (ETL local).
Ejecutar desde esta carpeta:  python solucion_referencia.py
Intenta primero cada paso por tu cuenta siguiendo GUIA.md; consulta esto solo para comparar.
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.master("local[*]")
    .appName("taller-data-engineer")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------------- PASO 1: lectura
def leer_csv(nombre):
    # inferSchema=False: todo llega como string y NOSOTROS decidimos los tipos.
    return spark.read.option("header", True).option("inferSchema", False).csv(f"datos/{nombre}.csv")

customers_raw = leer_csv("customers")
products_raw = leer_csv("products")
orders_raw = leer_csv("orders")
items_raw = leer_csv("order_items")

for n, df in [("customers", customers_raw), ("products", products_raw),
              ("orders", orders_raw), ("order_items", items_raw)]:
    print(n, df.count())

# ---------------------------------------------------------------- PASO 2: perfilamiento
def perfil_claves(df, clave):
    total = df.count()
    distintos = df.select(clave).distinct().count()
    nulos = df.filter(F.col(clave).isNull()).count()
    print(f"  {clave}: total={total} distintos={distintos} nulos={nulos} -> duplicados={total - distintos}")

def perfil_nulos(df):
    df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()

print("\n== Claves ==")
perfil_claves(customers_raw, "customer_id")
perfil_claves(products_raw, "product_id")
perfil_claves(orders_raw, "order_id")
perfil_claves(items_raw, "line_id")

print("== Nulos ==")
for df in (customers_raw, products_raw, orders_raw, items_raw):
    perfil_nulos(df)

print("== Dominios ==")
orders_raw.groupBy("status").count().show()
items_raw.groupBy("quantity").count().orderBy("quantity").show()
items_raw.groupBy("discount_pct").count().show()
products_raw.filter(F.expr("try_cast(unit_price AS decimal(10,2))").isNull()
                    | (F.expr("try_cast(unit_price AS decimal(10,2))") <= 0)).show()
orders_raw.filter(F.expr("try_to_timestamp(order_ts)").isNull()).show(truncate=False)

print("== Huérfanos ==")
orders_raw.join(customers_raw, "customer_id", "left_anti").show()
items_raw.join(orders_raw, "order_id", "left_anti").show()
items_raw.join(products_raw, "product_id", "left_anti").show()

# ---------------------------------------------------------------- PASO 3: limpieza
# customers: fila duplicada exacta (C0020) -> deduplicar. Si no, el join DUPLICA importes.
customers = customers_raw.dropDuplicates(["customer_id"])

# products: tipar precio; los inválidos (nulo / <=0) se quedan con precio nulo y sus
# líneas irán a cuarentena (no podemos inventar un precio).
products = (
    products_raw
    .withColumn("unit_price", F.expr("try_cast(unit_price AS decimal(10,2))"))
    .withColumn("precio_valido", F.col("unit_price").isNotNull() & (F.col("unit_price") > 0))
)

# orders: corregir typo de estado, tipar fechas y dejar 1 versión por order_id (la más reciente).
w_order = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
orders = (
    orders_raw
    .withColumn("status", F.when(F.col("status") == "deliverd", "delivered").otherwise(F.col("status")))
    .withColumn("order_ts", F.expr("try_to_timestamp(order_ts)"))     # '2026-99-31' -> null
    .withColumn("updated_at", F.expr("try_to_timestamp(updated_at)"))
    .withColumn("rn", F.row_number().over(w_order))
    .filter("rn = 1").drop("rn")
)

items = (
    items_raw
    .withColumn("quantity", F.expr("try_cast(quantity AS int)"))
    .withColumn("discount_pct", F.expr("try_cast(discount_pct AS decimal(5,4))"))
)

# Unimos TODO con left join (no perdemos filas) y luego decidimos fila a fila.
detalle = (
    items
    .join(orders.alias("o"), "order_id", "left")
    .join(customers.select("customer_id", "country"), "customer_id", "left")
    .join(products.select("product_id", "category", "unit_price", "precio_valido"), "product_id", "left")
)

motivo = (
    F.when(F.col("channel").isNull(), "pedido_huerfano")                 # order_id no existe
    .when(F.col("category").isNull(), "producto_huerfano")
    .when(~F.col("precio_valido"), "precio_invalido")
    .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "cantidad_fuera_de_rango")
    .when(F.col("discount_pct").isNull() | (F.col("discount_pct") < 0) | (F.col("discount_pct") >= 1), "descuento_fuera_de_dominio")
    .when(F.col("customer_id").isNull(), "pedido_sin_cliente")
    .when(F.col("country").isNull(), "cliente_huerfano")
    .when(F.col("order_ts").isNull(), "fecha_pedido_invalida")
    .when(F.col("status") == "cancelled", "pedido_cancelado")            # regla de negocio, no error
)
detalle = detalle.withColumn("motivo_rechazo", motivo).cache()

validas = (
    detalle.filter(F.col("motivo_rechazo").isNull())
    .withColumn("net_amount",
                F.round(F.col("quantity") * F.col("unit_price") * (F.lit(1) - F.col("discount_pct")), 2)
                .cast("decimal(18,2)"))
    .withColumn("mes", F.date_format("order_ts", "yyyy-MM"))
)
cuarentena = detalle.filter(F.col("motivo_rechazo").isNotNull())

print("\n== Destino de cada línea ==")
detalle.groupBy("motivo_rechazo").count().orderBy(F.desc("count")).show(truncate=False)

# ---------------------------------------------------------------- PASO 4: agregación (Spark SQL)
validas.createOrReplaceTempView("ventas_detalle")
resumen = spark.sql("""
    SELECT mes, country, channel, category,
           COUNT(DISTINCT order_id) AS pedidos,
           COUNT(*)                AS lineas,
           SUM(net_amount)         AS ventas_netas
    FROM ventas_detalle
    GROUP BY mes, country, channel, category
    ORDER BY mes, country, channel, category
""")
resumen.show(10)

# ---------------------------------------------------------------- PASO 6: guardar + QA
resumen.write.mode("overwrite").parquet("output/ventas_mensuales")
validas.write.mode("overwrite").parquet("output/detalle_valido")
cuarentena.write.mode("overwrite").parquet("output/cuarentena")

def controles(raw_items_count, validas_df, cuarentena_df, resumen_df):
    n_val, n_cua = validas_df.count(), cuarentena_df.count()
    print(f"QA1 conservación de filas: {n_val} válidas + {n_cua} cuarentena = {n_val + n_cua} "
          f"vs {raw_items_count} leídas -> {'OK' if n_val + n_cua == raw_items_count else 'FALLA'}")
    t_det = validas_df.agg(F.sum("net_amount")).first()[0]
    t_res = resumen_df.agg(F.sum("ventas_netas")).first()[0]
    print(f"QA2 reconciliación: detalle={t_det} resumen={t_res} -> {'OK' if t_det == t_res else 'FALLA'}")
    dup = validas_df.groupBy("line_id").count().filter("count > 1").count()
    print(f"QA3 line_id únicos tras joins: duplicados={dup} -> {'OK' if dup == 0 else 'FALLA'}")

print("\n== Controles de calidad (histórico) ==")
controles(items_raw.count(), validas, cuarentena, spark.read.parquet("output/ventas_mensuales"))

# ---------------------------------------------------------------- PASO 5: incremental idempotente
orders_inc = spark.read.json("datos/incremental/orders_incremental.jsonl")
items_inc = spark.read.json("datos/incremental/order_items_incremental.jsonl")

def aplicar_lote(orders_base, items_base, orders_lote, items_lote):
    """Une base + lote y se queda con la versión más reciente por clave.
    Es idempotente: aplicar el mismo lote dos veces da el mismo resultado."""
    cols_o, cols_i = orders_base.columns, items_base.columns
    o = (orders_base.unionByName(orders_lote.select(cols_o))
         .withColumn("rn", F.row_number().over(
             Window.partitionBy("order_id").orderBy(F.expr("try_to_timestamp(updated_at)").desc())))
         .filter("rn = 1").drop("rn"))
    i = items_base.unionByName(items_lote.select(cols_i)).dropDuplicates(["line_id"])
    return o, i

o1, i1 = aplicar_lote(orders_raw, items_raw, orders_inc, items_inc)
o2, i2 = aplicar_lote(o1, i1, orders_inc, items_inc)          # re-proceso del MISMO lote
print("\n== Incremental ==")
print("orders tras 1 aplicación:", o1.count(), "| tras 2 aplicaciones:", o2.count())
print("items  tras 1 aplicación:", i1.count(), "| tras 2 aplicaciones:", i2.count())
o2.filter("order_id = 'O00005'").show(truncate=False)          # actualización tardía ganó

spark.stop()
