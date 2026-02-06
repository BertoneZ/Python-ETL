# Pipeline ETL de Ventas con Python y SQL

Este proyecto implementa un proceso **ETL (Extract, Transform, Load)** automatizado para la limpieza y migración de datos transaccionales.

## 📋 Descripción Técnica
El script toma datos crudos de ventas, los procesa para asegurar su calidad y los almacena en una base de datos relacional para su análisis posterior.
# Configuración del Data Warehouse y Schema SQL

<img width="455" height="319" alt="image" src="https://github.com/user-attachments/assets/f9df40a3-2211-4801-9184-83cfffafb5ad" />
## Descripción 
En esta etapa se establece la conexión con la base de datos SQLite y se automatiza la creación de la tabla. El uso de DROP TABLE IF EXISTS asegura que el entorno de datos se reinicie correctamente en cada ejecución, garantizando la integridad del esquema.
# Transformación y Calidad de Datos
<img width="649" height="472" alt="image" src="https://github.com/user-attachments/assets/ee692cbd-8557-4499-885f-f71bfa675344" />
## Descripción 
Aquí es donde ocurre la "magia" del ETL. El script recorre los datos crudos del CSV, elimina espacios innecesarios y valida que los tipos de datos sean correctos. Implementé un manejo de excepciones (try/except) para filtrar registros corruptos o incompletos, asegurando que solo información de calidad llegue al destino
* **Transform:**
    * Limpieza de datos (strings vacíos, espacios extra).
    * Validación de tipos de datos y manejo de errores (`try-except`) para evitar interrupciones.
 
#Carga Masiva y Reportabilidad
<img width="836" height="494" alt="image" src="https://github.com/user-attachments/assets/d7c2d6ea-022b-442a-8cf1-d98e57019db0" />
## Descripción 
Los datos ya limpios se insertan de forma masiva en SQL mediante executemany para optimizar el rendimiento. Finalmente, se realiza una consulta filtrada para extraer los registros de mayor valor y exportarlos automáticamente a un reporte CSV listo para el análisis de negocio.
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
