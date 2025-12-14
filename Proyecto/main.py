import csv
import sqlite3

def proceso_etl():
    print("--- INICIANDO PROCESO ETL ---")
    conn = sqlite3.connect("datawarehouse.db")
    cursor = conn.cursor()
    
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

    with open('ventas_raw.csv', 'r') as archivo:
        lector = csv.DictReader(archivo)
        
        for fila in lector:
            try:
                producto = fila['producto'].strip() 
                if not producto:
                    continue 

                if not fila['precio'] or not fila['cantidad']:
                    print(f"Dato incompleto en producto '{producto}', saltando...")
                    continue
                
                precio = float(fila['precio'])
                cantidad = int(fila['cantidad'])
                total = precio * cantidad

                datos_limpios.append((producto, total))
                
            except ValueError:
                print("Error de tipo de dato en una fila, saltando...")
                continue

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

    nombre_reporte = 'reporte_ventas_altas.csv'
    with open(nombre_reporte, 'w', newline='') as archivo_reporte:
        escritor = csv.writer(archivo_reporte)
        
        escritor.writerow(['ID', 'Producto', 'Total Venta'])
        
        escritor.writerows(resultados)
        
        print(f"\n>> Archivo '{nombre_reporte}' creado exitosamente.")
    
    conn.close()

    
if __name__ == "__main__":
    proceso_etl()