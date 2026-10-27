# monografia-spark-secop
Comparación de desempeño de RDD, DataFrame y Spark SQL en PySpark sobre datos SECOP II (monografía, Esp. Analítica y Ciencia de Datos, UdeA)

# monografia-spark-secop

Comparación de desempeño de las APIs RDD, DataFrame y Spark SQL de PySpark sobre los contratos de SECOP II (2023–2025). Repositorio de soporte de la monografía de la Especialización en Analítica y Ciencia de Datos, Universidad de Antioquia.

## Estructura

| Carpeta / archivo | Contenido |
| --- | --- |
| `notebooks/01_datos.ipynb` | Descarga, limpieza y conversión a Parquet |
| `notebooks/02_consultas.ipynb` | Consultas Q1, Q3, Q5 y Q6 en cuatro variantes |
| `notebooks/03_equivalencia.ipynb` | Verificación de que las variantes devuelven el mismo resultado |
| `notebooks/04_mediciones.ipynb` | Script de medición de tiempos |
| `notebooks/05_analisis.ipynb` | Tablas, figuras y planes de ejecución |
| `results/mediciones.csv` | Tiempos de cada corrida |
| `results/planes/` | Planes de ejecución (`explain`) |
| `results/figuras/` | Figuras de la monografía |
| `results/equipo.txt` | Hardware y versiones de software |

## Datos

Fuente: [SECOP II – Contratos Electrónicos](https://www.datos.gov.co/Gastos-Gubernamentales/SECOP-II-Contratos-Electr-nicos/jbjy-vk9h), Colombia Compra Eficiente, datos.gov.co.
Los datos no se incluyen en el repositorio por su tamaño. Para reproducir, descárguelos en `data/raw/` y ejecute `01_datos.ipynb`.

## Requisitos

- Python 3.x
- Java 17
- PySpark (versión: por completar)

## Reproducción

Ejecute los notebooks en orden, del 01 al 05.

## Licencia

MIT.
