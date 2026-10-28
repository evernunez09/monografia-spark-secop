"""Las 24 implementaciones de la monografía: 6 consultas x 4 variantes."""
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StringType

UMBRAL = 100_000_000
normalizar = F.udf(lambda s: (s or "").strip().upper(), StringType())

def _limpio(r):
    return (r or "").strip().upper()

# ---------- Q1: contratos y valor total por departamento ----------
def q1_rdd(df):
    return (df.select("departamento", "valor").rdd
              .map(lambda r: (_limpio(r[0]), (1, r[1])))
              .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])))

def q1_df(df):
    return (df.withColumn("dep", F.upper(F.trim("departamento")))
              .groupBy("dep").agg(F.count("*").alias("n"), F.sum("valor").alias("total")))

def q1_sql(df):
    df.createOrReplaceTempView("secop")
    return df.sparkSession.sql("""
        SELECT upper(trim(departamento)) AS dep, count(*) AS n, sum(valor) AS total
        FROM secop GROUP BY upper(trim(departamento))""")

def q1_udf(df):
    return (df.withColumn("dep", normalizar("departamento"))
              .groupBy("dep").agg(F.count("*").alias("n"), F.sum("valor").alias("total")))

# ---------- Q2: contratos de Antioquia por encima del umbral ----------
def q2_rdd(df):
    return (df.select("id_contrato", "nombre_entidad", "fecha", "valor", "departamento").rdd
              .filter(lambda r: _limpio(r[4]) == "ANTIOQUIA" and r[3] > UMBRAL)
              .map(lambda r: (r[0], r[1], r[2], r[3])))

def q2_df(df):
    return (df.filter((F.upper(F.trim("departamento")) == "ANTIOQUIA") & (F.col("valor") > UMBRAL))
              .select("id_contrato", "nombre_entidad", "fecha", "valor"))

def q2_sql(df):
    df.createOrReplaceTempView("secop")
    return df.sparkSession.sql(f"""
        SELECT id_contrato, nombre_entidad, fecha, valor FROM secop
        WHERE upper(trim(departamento)) = 'ANTIOQUIA' AND valor > {UMBRAL}""")

def q2_udf(df):
    return (df.filter((normalizar("departamento") == "ANTIOQUIA") & (F.col("valor") > UMBRAL))
              .select("id_contrato", "nombre_entidad", "fecha", "valor"))

# ---------- Q3: valor total por modalidad y mes ----------
def q3_rdd(df):
    return (df.select("modalidad_de_contratacion", "fecha", "valor").rdd
              .map(lambda r: ((_limpio(r[0]), r[1].strftime("%Y-%m")), r[2]))
              .reduceByKey(lambda a, b: a + b))

def q3_df(df):
    return (df.withColumn("modalidad", F.upper(F.trim("modalidad_de_contratacion")))
              .withColumn("mes", F.date_format("fecha", "yyyy-MM"))
              .groupBy("modalidad", "mes").agg(F.sum("valor").alias("total")))

def q3_sql(df):
    df.createOrReplaceTempView("secop")
    return df.sparkSession.sql("""
        SELECT upper(trim(modalidad_de_contratacion)) AS modalidad,
               date_format(fecha, 'yyyy-MM') AS mes, sum(valor) AS total
        FROM secop
        GROUP BY upper(trim(modalidad_de_contratacion)), date_format(fecha, 'yyyy-MM')""")

def q3_udf(df):
    return (df.withColumn("modalidad", normalizar("modalidad_de_contratacion"))
              .withColumn("mes", F.date_format("fecha", "yyyy-MM"))
              .groupBy("modalidad", "mes").agg(F.sum("valor").alias("total")))

# ---------- Q4: ranking de proveedores por valor (orden global) ----------
def q4_rdd(df):
    return (df.select("proveedor_adjudicado", "valor").rdd
              .map(lambda r: (_limpio(r[0]), r[1]))
              .reduceByKey(lambda a, b: a + b)
              .sortBy(lambda x: x[1], ascending=False))

def q4_df(df):
    return (df.withColumn("proveedor", F.upper(F.trim("proveedor_adjudicado")))
              .groupBy("proveedor").agg(F.sum("valor").alias("total"))
              .orderBy(F.desc("total")))

def q4_sql(df):
    df.createOrReplaceTempView("secop")
    return df.sparkSession.sql("""
        SELECT upper(trim(proveedor_adjudicado)) AS proveedor, sum(valor) AS total
        FROM secop GROUP BY upper(trim(proveedor_adjudicado))
        ORDER BY total DESC""")

def q4_udf(df):
    return (df.withColumn("proveedor", normalizar("proveedor_adjudicado"))
              .groupBy("proveedor").agg(F.sum("valor").alias("total"))
              .orderBy(F.desc("total")))

# ---------- Q5: participación de cada entidad en su departamento ----------
def q5_rdd(df):
    base = (df.select("departamento", "nombre_entidad", "valor").rdd
              .map(lambda r: ((_limpio(r[0]), r[1]), r[2])))
    por_ent = base.reduceByKey(lambda a, b: a + b)
    por_dep = por_ent.map(lambda x: (x[0][0], x[1])).reduceByKey(lambda a, b: a + b)
    return (por_ent.map(lambda x: (x[0][0], (x[0][1], x[1])))
                   .join(por_dep)
                   .map(lambda x: (x[0], x[1][0][0], x[1][0][1],
                                   x[1][0][1] / x[1][1] if x[1][1] else None)))

def _q5_desde_base(base):
    por_ent = base.groupBy("dep", "nombre_entidad").agg(F.sum("valor").alias("total_ent"))
    por_dep = por_ent.groupBy("dep").agg(F.sum("total_ent").alias("total_dep"))
    return (por_ent.join(por_dep, "dep")
                   .withColumn("participacion", F.col("total_ent") / F.col("total_dep"))
                   .select("dep", "nombre_entidad", "total_ent", "participacion"))

def q5_df(df):
    return _q5_desde_base(df.withColumn("dep", F.upper(F.trim("departamento"))))

def q5_sql(df):
    df.createOrReplaceTempView("secop")
    return df.sparkSession.sql("""
        WITH por_ent AS (
            SELECT upper(trim(departamento)) AS dep, nombre_entidad, sum(valor) AS total_ent
            FROM secop GROUP BY upper(trim(departamento)), nombre_entidad),
        por_dep AS (
            SELECT dep, sum(total_ent) AS total_dep FROM por_ent GROUP BY dep)
        SELECT e.dep, e.nombre_entidad, e.total_ent, e.total_ent / d.total_dep AS participacion
        FROM por_ent e JOIN por_dep d ON e.dep = d.dep""")

def q5_udf(df):
    return _q5_desde_base(df.withColumn("dep", normalizar("departamento")))

# ---------- Q6: contrato de mayor valor por entidad y año ----------
def gana(a, b):
    if a[1] > b[1] or (a[1] == b[1] and a[0] < b[0]):
        return a
    return b

def q6_rdd(df):
    return (df.select("nombre_entidad", "anio", "id_contrato", "valor").rdd
              .map(lambda r: ((_limpio(r[0]), r[1]), (r[2], r[3])))
              .reduceByKey(gana)
              .map(lambda x: (x[0][0], x[0][1], x[1][0], x[1][1])))

def _q6_desde_base(base):
    ventana = Window.partitionBy("entidad", "anio").orderBy(F.desc("valor"), F.asc("id_contrato"))
    return (base.withColumn("posicion", F.row_number().over(ventana))
                .filter(F.col("posicion") == 1)
                .select("entidad", "anio", "id_contrato", "valor"))

def q6_df(df):
    return _q6_desde_base(df.withColumn("entidad", F.upper(F.trim("nombre_entidad"))))

def q6_sql(df):
    df.createOrReplaceTempView("secop")
    return df.sparkSession.sql("""
        SELECT entidad, anio, id_contrato, valor FROM (
            SELECT upper(trim(nombre_entidad)) AS entidad, anio, id_contrato, valor,
                   ROW_NUMBER() OVER (PARTITION BY upper(trim(nombre_entidad)), anio
                                      ORDER BY valor DESC, id_contrato ASC) AS posicion
            FROM secop)
        WHERE posicion = 1""")

def q6_udf(df):
    return _q6_desde_base(df.withColumn("entidad", normalizar("nombre_entidad")))

# ---------- Catálogo de las 24 combinaciones ----------
CONSULTAS = {
    "Q1": {"rdd": q1_rdd, "df": q1_df, "sql": q1_sql, "udf": q1_udf},
    "Q2": {"rdd": q2_rdd, "df": q2_df, "sql": q2_sql, "udf": q2_udf},
    "Q3": {"rdd": q3_rdd, "df": q3_df, "sql": q3_sql, "udf": q3_udf},
    "Q4": {"rdd": q4_rdd, "df": q4_df, "sql": q4_sql, "udf": q4_udf},
    "Q5": {"rdd": q5_rdd, "df": q5_df, "sql": q5_sql, "udf": q5_udf},
    "Q6": {"rdd": q6_rdd, "df": q6_df, "sql": q6_sql, "udf": q6_udf},
}
