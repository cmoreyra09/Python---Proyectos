import pandas as pd
import pyodbc
import time


class SQLServerManager:
    def __init__(self, server, database,username,password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.connection = None

    def conectar(self):
        try:
            connection_string = f"DRIVER={{SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password};"
            self.connection = pyodbc.connect(connection_string)
            print("Conexión exitosa a la base de datos SQL Server.")
        except Exception as e:
            print(f"Error al conectar a la base de datos: {e}")
            self.cerrar_conexion()  # Cerrar la conexión en caso de error

    def cerrar_conexion(self):
        if self.connection:
            try:
                self.connection.close()
                print("Conexión cerrada exitosamente.")
            except Exception as e:
                print(f"Error al cerrar la conexión: {e}")
        else:
            print("La conexión ya está cerrada.")

class TableManager:
    def __init__(self, connection, table_name, df):
        self.connection = connection
        self.table_name = table_name
        self.df = df

    def crear_tabla_si_no_existe(self):
        try:
            cursor = self.connection.cursor()

            check_table_query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{self.table_name}'"
            cursor.execute(check_table_query)
            table_exists = cursor.fetchone()[0] > 0

            if not table_exists:
                # Obtener la información de tipos de columna del DataFrame
                column_types = self.obtener_tipos_columnas()

                # Construir la definición de la tabla utilizando la información de tipos
                column_definitions = ', '.join([
                    f"[{col}] {col_type}" for col, col_type in column_types.items()] +
                    ["[FechaEjecucionInsert] DATETIME DEFAULT GETDATE()",
                     "[UsuarioEjecucionInsert] NVARCHAR(250) DEFAULT SYSTEM_USER",
                     "[PcEjecucionInsert] NVARCHAR(250) DEFAULT HOST_NAME()"])

                create_table_query = f"CREATE TABLE {self.table_name} ({column_definitions})"
                cursor.execute(create_table_query)
                self.connection.commit()
                print(f"Tabla '{self.table_name}' creada exitosamente.")
            else:
                print(f"La tabla '{self.table_name}' ya existe en la base de datos.")

            cursor.close()
        except Exception as e:
            print(f"Error al crear la tabla: {e}")
            self.cerrar_conexion()

    def obtener_tipos_columnas(self):
        # Obtener tipos de columna desde el DataFrame
        column_types = {}
        for col, dtype in zip(self.df.columns, self.df.dtypes):
            sql_type = self.mapear_tipo_pandas_a_sql(dtype)
            column_types[col] = sql_type

        return column_types

    def mapear_tipo_pandas_a_sql(self, dtype):
        # Mapear tipos de datos de pandas a tipos de datos de SQL Server
        if "object" in str(dtype):
            return "NVARCHAR(250)"
        elif "float" in str(dtype):
            return "FLOAT"
        elif "int" in str(dtype):
            return "FLOAT"  # Cambiado a FLOAT para evitar problemas con INT
        else:
            return "NVARCHAR(250)"  # Tipo predeterminado para otros casos

    def ingestar_datos(self):
        try:
            cursor = self.connection.cursor()

            # Rellenar valores NaN con cadena vacía
            self.df.fillna('', inplace=True)

            # Obtener tipos de columna desde el DataFrame
            column_types = self.obtener_tipos_columnas()

            # Construir la cadena de inserción con las columnas regulares y auditables
            column_names_regular = ', '.join([f"[{col}]" for col in self.df.columns])
            placeholders_regular = ', '.join(['?'] * len(self.df.columns))

            # No incluir las columnas de auditoría en las listas
            column_names_all = f"{column_names_regular}"
            placeholders_all = f"{placeholders_regular}"

            start_time = time.time()  # Tomar el tiempo de inicio

            # Ejecutar la consulta de inserción para cada fila del DataFrame
            for _, row in self.df.iterrows():
                try:
                    row_values = self.convertir_tipos(row, column_types)
                    row_values += (time.strftime('%Y-%m-%d %H:%M:%S'), 'SYSTEM_USER', 'HOST_NAME()')
                    cursor.execute(f"INSERT INTO {self.table_name} ({column_names_all}) VALUES ({placeholders_all})", row_values[:-3])
                except Exception as e:
                    print(f"Error al insertar fila: {e}")

            self.connection.commit()
            cursor.close()

            end_time = time.time()  # Tomar el tiempo de finalización
            elapsed_time = end_time - start_time  # Calcular el tiempo transcurrido

            print(f"Ingestión de datos completada en {elapsed_time:.2f} segundos.")
            print(f"Se insertaron {len(self.df)} filas en la tabla '{self.table_name}'.")
        except Exception as e:
            print(f"Error al insertar datos: {e}")
            self.cerrar_conexion()

    def convertir_tipos(self, row, column_types):
        # Convertir valores al tipo correspondiente antes de la inserción
        row_values = []
        for col in self.df.columns:
            value = row[col]
            sql_type = column_types[col]

            if sql_type == "NVARCHAR(250)":
                row_values.append(str(value)[:250])  # Limitar a 250 caracteres
            elif sql_type == "FLOAT":
                row_values.append(float(value))
            else:
                row_values.append(str(value))  # Tipo predeterminado

        return tuple(row_values)

class QueryManager:
    def __init__(self, connection):
        self.connection = connection

    def ejecutar_query(self, query):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                if query.strip().lower().startswith('select'):
                    # Solo intenta fetchear resultados si es una consulta SELECT
                    resultados = cursor.fetchall()
                    print("Consulta ejecutada exitosamente.")
                    return resultados
                else:
                    # Para INSERT, UPDATE, DELETE, etc., no hay resultados para fetchear
                    self.connection.commit()
                    print("Consulta de modificación ejecutada exitosamente.")
        except Exception as e:
            print(f"Error al ejecutar la consulta: {e}")

