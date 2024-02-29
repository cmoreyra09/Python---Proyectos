from sql_manager import SQLServerManager, TableManager, QueryManager
import pandas as pd 

# Configuración
server = '10.252.194.193'
database = 'BD_CARGAS'
table_name = 'T_Carga_Base_EC_Cobranzas_v1_py'

# Conectar a la base de datos
sql_manager = SQLServerManager(server, database)
sql_manager.conectar()

# Excel a Ingestar
df = pd.read_csv(r'D:\SSIS\ATENTO\MULTISECTOR\Carga_Bases_Gocontact_EC\3.CargaCM\Carga_Base_Cobranzas_v1.csv', delimiter=';').astype(str)
df_1 = df.replace('nan','')

# Gestionar la tabla
table_manager = TableManager(sql_manager.connection, table_name, df_1)
table_manager.crear_tabla_si_no_existe()
table_manager.ingestar_datos()

# Cerrar la conexión
sql_manager.cerrar_conexion()
