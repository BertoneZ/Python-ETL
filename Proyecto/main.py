import csv
import sqlite3

def proceso_etl():
    print("--- INICIANDO PROCESO ETL ---")

    # 1. CONEXIÓN A BASE DE DATOS (DESTINO)
    conn = sqlite3.connect("datawarehouse.db")
    cursor = conn.cursor()
    
    # Reiniciamos la tabla para la prueba
    cursor.execute("DROP TABLE IF EXISTS ventas_limpias")
    cursor.execute("""
        CREATE TABLE ventas_limpias (
            id INTEGER PRIMARY KEY,
            producto TEXT,
            total_venta REAL
        )
    """)
    conn.commit()

    datos_limpios = []

    # 2. EXTRACT (Lectura del CSV)
    with open('ventas_raw.csv', 'r') as archivo:
        lector = csv.DictReader(archivo)
        
        # 3. TRANSFORM (Lógica de limpieza)
        for fila in lector:
            try:
                # A. Limpieza: Ignorar si no hay nombre de producto
                producto = fila['producto'].strip() # Quita espacios extra
                if not producto:
                    continue 

                # B. Validación: Si falta cantidad o precio, saltar
                if not fila['precio'] or not fila['cantidad']:
                    print(f"Dato incompleto en producto '{producto}', saltando...")
                    continue
                
                # C. Transformación: Calcular Total (Precio * Cantidad)
                precio = float(fila['precio'])
                cantidad = int(fila['cantidad'])
                total = precio * cantidad

                # Preparamos la tupla para guardar
                datos_limpios.append((producto, total))
                
            except ValueError:
                print("Error de tipo de dato en una fila, saltando...")
                continue

    # 4. LOAD (Carga masiva a SQL)
    if datos_limpios:
        cursor.executemany("INSERT INTO ventas_limpias (producto, total_venta) VALUES (?, ?)", datos_limpios)
        conn.commit()
        print(f"\n>> ÉXITO: Se cargaron {len(datos_limpios)} registros limpios a SQL.")
    else:
        print("No se procesaron datos.")

    print("\n--- PRODUCTOS MAYORES A $200 ---")
    cursor.execute("SELECT * FROM ventas_limpias WHERE total_venta > 200")
    resultados = cursor.fetchall()

    for producto in resultados:
        print(producto)

    # Exportamos a CSV
    nombre_reporte = 'reporte_ventas_altas.csv'
    with open(nombre_reporte, 'w', newline='') as archivo_reporte:
        escritor = csv.writer(archivo_reporte)
        
        # Escribimos encabezados
        escritor.writerow(['ID', 'Producto', 'Total Venta'])
        
        # Escribimos todas las filas de golpe
        escritor.writerows(resultados)
        
        print(f"\n>> Archivo '{nombre_reporte}' creado exitosamente.")
    
    conn.close()

    
if __name__ == "__main__":
    proceso_etl()