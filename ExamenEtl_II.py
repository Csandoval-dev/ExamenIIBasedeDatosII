import pandas as pd
import pyodbc

# 1. Leer archivo Excel (Extract)
def extract_excel(file_path, sheet_name):
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        print(f"Datos extraídos de {sheet_name}.")
        return df
    except Exception as e:
        print(f"Error leyendo el archivo Excel: {e}")
        return None

# 2. Transformar los datos (Transform)
def transform_data(df, table_name):
    try:
        # Ajuste para la tabla `Departamento` (evitar IDENTITY conflict)
        if table_name == 'dbo.Departamentos':  # Cambia según tu esquema
            df = df.drop(columns=['id_departamento'], errors='ignore')
        print("Datos transformados correctamente.")
        return df
    except Exception as e:
        print(f"Error transformando los datos: {e}")
        return None

# 3. Cargar datos a SQL Server (Load)
def load_to_sql(df, connection_string, table_name):
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Convertir el DataFrame en lista de tuplas
        data = [tuple(row) for row in df.to_numpy()]
        columns = ", ".join(df.columns)

        # Crear query SQL de inserción
        placeholders = ", ".join(["?"] * len(df.columns))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Insertar filas
        cursor.executemany(query, data)
        conn.commit()
        conn.close()
        print(f"Datos cargados exitosamente en la tabla {table_name}.")
    except Exception as e:
        print(f"Error cargando los datos a SQL Server: {e}")

# Uso del ETL
def etl_process(file_path, sheets_to_tables, connection_string):
    for sheet_name, table_name in sheets_to_tables.items():
        # Paso 1: Extract
        df = extract_excel(file_path, sheet_name)
        if df is None:
            continue

        # Paso 2: Transform
        df_transformed = transform_data(df, table_name)
        if df_transformed is None:
            continue

        # Paso 3: Load
        load_to_sql(df_transformed, connection_string, table_name)

# Ruta del archivo Excel
file_path = r'C:\Users\User\Desktop\Departamentos_Empleados.xlsx'

# Mapeo entre hojas de Excel y tablas de SQL Server
sheets_to_tables = {
    'Departamento': 'dbo.Departamento',
    'Empleado': 'dbo.Empleado'
}

# Cadena de conexión para SQL Server con autenticación de Windows
connection_string = (
    "DRIVER={SQL Server};"
    "SERVER=DESKTOP-RCPI344;"
    "DATABASE=BasedeDatosII;"
    "Trusted_Connection=yes;"
)

# Ejecutar el proceso ETL
etl_process(file_path, sheets_to_tables, connection_string)
