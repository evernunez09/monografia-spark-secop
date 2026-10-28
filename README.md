# monografia-spark-secop

Comparación de desempeño de las APIs RDD, DataFrame y Spark SQL de PySpark, y de una variante DataFrame con una función definida por el usuario (UDF) de Python, sobre 2.734.292 contratos de SECOP II (2023–2025). Repositorio de soporte de la monografía de la Especialización en Analítica y Ciencia de Datos, Universidad de Antioquia.

## Estructura

| Carpeta / archivo | Contenido |
| --- | --- |
| `notebooks/01_datos.ipynb` | Descarga de SECOP II, limpieza y conversión a Parquet |
| `notebooks/02_consultas.ipynb` | Desarrollo de las seis consultas (Q1–Q6) en las cuatro variantes |
| `notebooks/consultas.py` | Versión final de las 24 implementaciones, usada en la validación y en las mediciones |
| `notebooks/03_equivalencia.ipynb` | Verificación de que las cuatro variantes producen el mismo resultado |
| `notebooks/04_mediciones.ipynb` | Medición de tiempos (incluye los diagnósticos del piloto en Windows) |
| `notebooks/05_analisis.ipynb` | Tablas, figuras y análisis de los planes de ejecución |
| `results/mediciones_wsl.csv` | Tiempos definitivos medidos en Linux (ronda 0 = piloto; rondas 1–3 = mediciones) |
| `results/mediciones_windows.csv` | Piloto medido en Windows (comparación del costo de los procesos de Python) |
| `results/equivalencia.csv` | Resultado de la prueba de equivalencia |
| `results/resumen_tiempos.csv`, `results/aceleracion_nucleos.csv` | Medianas, mínimos, máximos y aceleraciones |
| `results/indicadores_planes.csv`, `results/planes/` | Planes de ejecución de cada consulta y variante |
| `results/figuras/` | Figuras de la monografía |
| `results/tabla1_datos.txt` | Resumen de la preparación de los datos |
| `results/equipo.txt`, `results/equipo_wsl.txt` | Hardware y versiones de software (Windows y Linux) |

## Consultas

| Consulta | Qué calcula | Patrón |
| --- | --- | --- |
| Q1 | Contratos y valor total por departamento | Agregación simple |
| Q2 | Contratos de Antioquia por encima de 100 millones | Filtro y proyección |
| Q3 | Valor total por modalidad de contratación y mes | Agregación con dos llaves y fechas |
| Q4 | Ranking completo de proveedores por valor | Agregación y orden global |
| Q5 | Participación de cada entidad en el valor de su departamento | Unión (*join*) entre agregados |
| Q6 | Contrato de mayor valor por entidad y año | Función de ventana |

## Datos

Fuente: [SECOP II – Contratos Electrónicos](https://www.datos.gov.co/Gastos-Gubernamentales/SECOP-II-Contratos-Electr-nicos/jbjy-vk9h), Colombia Compra Eficiente, datos.gov.co. Solo se descargan 7 columnas, sin datos personales.

Los datos no se incluyen en el repositorio por su tamaño. Para reproducir, ejecute `01_datos.ipynb`, que los descarga en `data/raw/` y genera `data/secop.parquet`.

## Entorno

- Mediciones: Ubuntu 24.04 sobre WSL 2, con 12 GB de RAM y 16 procesadores lógicos asignados.
- Equipo: AMD Ryzen 7 4800H (8 núcleos físicos, 16 hilos), 16 GB de RAM.
- Python 3.11.9, Java 17 (OpenJDK 17.0.20.1), PySpark 3.5.9 en modo local.
- Configuración de Spark: memoria del controlador de 8 GB y regla de optimización `InferFiltersFromConstraints` desactivada para todas las variantes (ver la monografía).

## Reproducción

1. Ejecute `01_datos.ipynb` para descargar y preparar los datos.
2. Ejecute `03_equivalencia.ipynb`: su primera celda genera `consultas.py`.
3. En `04_mediciones.ipynb`, mida cada configuración de núcleos en un proceso de Python nuevo (reinicie el kernel entre `local[1]` y `local[*]`).
4. Ejecute `05_analisis.ipynb` para obtener las tablas, las figuras y los planes.

## Licencia

MIT.
