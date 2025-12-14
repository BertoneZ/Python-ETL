# Pipeline ETL de Ventas con Python y SQL

Este proyecto implementa un proceso **ETL (Extract, Transform, Load)** automatizado para la limpieza y migración de datos transaccionales.

## 📋 Descripción Técnica
El script toma datos crudos de ventas, los procesa para asegurar su calidad y los almacena en una base de datos relacional para su análisis posterior.

* **Extract:** Lectura de archivos planos (`.csv`).
* **Transform:**
    * Limpieza de datos (strings vacíos, espacios extra).
    * Validación de tipos de datos y manejo de errores (`try-except`) para evitar interrupciones.
    * Cálculo de métricas derivadas (Total de Venta).
* **Load:** Inserción masiva y eficiente en **SQLite** utilizando transacciones (`executemany`).
* **Reporting:** Generación automática de un archivo `.csv` con las ventas filtradas por criterio de negocio (Monto > $200).

## 🛠 Tecnologías
* **Lenguaje:** Python 3.x
* **Base de Datos:** SQLite
* **Librerías:** `csv`, `sqlite3` (Módulos estándar)

## 🚀 Cómo ejecutar
1.  Clonar el repositorio.
2.  Ejecutar el script principal:
    ```bash
    python main.py
    ```
3.  El sistema generará la base de datos `datawarehouse.db` y el reporte `reporte_ventas_altas.csv`.
