# Pipeline ETL de Ventas con Python y SQL

Este proyecto implementa un proceso **ETL (Extract, Transform, Load)** automatizado para la limpieza y migración de datos transaccionales.

## 📋 Descripción Técnica
El script toma datos crudos de ventas, los procesa para asegurar su calidad y los almacena en una base de datos relacional para su análisis posterior.
# Configuración del Data Warehouse y Schema SQL

<img width="463" height="339" alt="image" src="https://github.com/user-attachments/assets/3f24bbb8-26f4-43c0-9ed9-6cb487894a75" />


## Descripción 
En esta etapa se establece la conexión con la base de datos SQLite y se automatiza la creación de la tabla. El uso de DROP TABLE IF EXISTS asegura que el entorno de datos se reinicie correctamente en cada ejecución, garantizando la integridad del esquema.

# Transformación y Calidad de Datos

<img width="679" height="484" alt="image" src="https://github.com/user-attachments/assets/60c27d66-ff4e-4bce-9206-34a3cd3eb0f0" />

## Descripción 
Aquí es donde ocurre la "magia" del ETL. El script recorre los datos crudos del CSV, elimina espacios innecesarios y valida que los tipos de datos sean correctos. Implementé un manejo de excepciones (try/except) para filtrar registros corruptos o incompletos, asegurando que solo información de calidad llegue al destino

 
# Carga Masiva y Reportabilidad

<img width="883" height="502" alt="image" src="https://github.com/user-attachments/assets/6b25cef9-e136-4307-84c8-968586497cb1" />



## Descripción 

Los datos ya limpios se insertan de forma masiva en SQL mediante executemany para optimizar el rendimiento. Finalmente, se realiza una consulta filtrada para extraer los registros de mayor valor y exportarlos automáticamente a un reporte CSV listo para el análisis de negocio.

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
