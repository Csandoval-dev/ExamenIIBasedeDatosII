import pandas as pd
import pyodbc

# Conexión a SQL Server
conexion = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=DESKTOP-RCPI344;"
    "DATABASE=TallerAutomotriz;"
    "Trusted_Connection=yes;"
)

cursor = conexion.cursor()

def importar_y_limpiar():
    try:
        # 1. Leer los datos
        carros = pd.read_csv("Carros.txt", sep=",", header=None, names=["Matricula", "Marca", "Modelo", "Color", "Precio", "Extras", "CodigoCliente"], encoding="latin1")
        clientes = pd.read_csv("Cliente.txt", sep=",", header=None, names=["CodigoCliente", "Nombre", "Apellidos", "Direccion", "Ciudad", "CodigoPostal", "Departamento", "Telefono", "FechaNacimiento"], encoding="latin1")
        revision = pd.read_csv("Revision.txt", sep=",", header=None, names=["NumRevision", "CambioAceite", "CambioFiltro", "RevisionFrenos", "Otros", "Matricula"], encoding="latin1")

        # 2. Limpieza de datos
        # Limpieza para `Carros`
        carros["Precio"] = pd.to_numeric(carros["Precio"], errors="coerce")  # Manejo de valores no numéricos en 'Precio'
        carros["Extras"] = carros["Extras"].fillna("N/A")  # Rellenar valores nulos
        carros["CodigoCliente"] = pd.to_numeric(carros["CodigoCliente"], errors="coerce").fillna(0).astype("int")  # Convertir a numérico y manejar nulos

        # Limpieza para `Clientes`
        clientes["FechaNacimiento"] = pd.to_datetime(clientes["FechaNacimiento"], dayfirst=True, errors="coerce")  # Formato de fecha
        clientes["Telefono"] = clientes["Telefono"].astype("str").str.replace(" ", "").str.replace("-", "")  # Limpiar el campo teléfono

        # Limpieza para `Revision`
        revision = revision.fillna("")  # Rellenar nulos con cadenas vacías
        revision[["CambioAceite", "CambioFiltro", "RevisionFrenos"]] = revision[["CambioAceite", "CambioFiltro", "RevisionFrenos"]].replace({"Si": 1, "No": 0})

        # 3. Inserción a SQL Server
        # Inserción para `Carros`
        for _, row in carros.iterrows():
            cursor.execute("""
                INSERT INTO Carros (Matricula, Marca, Modelo, Color, Precio, Extras, CodigoCliente)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, row.Matricula, row.Marca, row.Modelo, row.Color, row.Precio, row.Extras, row.CodigoCliente)

        # Inserción para `Clientes`
        for _, row in clientes.iterrows():
            cursor.execute("""
                INSERT INTO Clientes (CodigoCliente, Nombre, Apellidos, Direccion, Ciudad, CodigoPostal, Departamento, Telefono, FechaNacimiento)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row.CodigoCliente, row.Nombre, row.Apellidos, row.Direccion, row.Ciudad, row.CodigoPostal, row.Departamento, row.Telefono, row.FechaNacimiento)

        # Inserción para `Revision`
        for _, row in revision.iterrows():
            cursor.execute("""
                INSERT INTO Revision (NumRevision, CambioAceite, CambioFiltro, RevisionFrenos, Otros, Matricula)
                VALUES (?, ?, ?, ?, ?, ?)
            """, row.NumRevision, row.CambioAceite, row.CambioFiltro, row.RevisionFrenos, row.Otros, row.Matricula)

        # Confirmar cambios
        conexion.commit()
        print("Datos importados y limpiados exitosamente.")

    except Exception as e:
        print("Error durante el proceso ETL:", e)

    finally:
        cursor.close()
        conexion.close()

# Llamar a la función principal
importar_y_limpiar()
