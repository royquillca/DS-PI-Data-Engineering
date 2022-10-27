#--------------------------#
# Importacion de Librerías #
#--------------------------#

import pandas as pd
import numpy as np

import mysql.connector as msql
from mysql.connector import Error

from sqlalchemy import create_engine

from decouple import config
import os

#-------------------------------------------------#
# Configuracion de la conexión a la base de datos #
#-------------------------------------------------#

host=config('MYSQL_HOST')
user=config('MYSQL_USER')
password=config('MYSQL_PASSWORD')
db_name=config('MYSQL_DB')
BASE_PATH = './Datasets/'

# Conexion a la base de datos MYSQL
my_conn = create_engine(f"mysql+mysqldb://{user}:{password}@{host}/{db_name}")


#-------------------#
# Limpieza de datos #
#-------------------#

def limpieza_producto(df):
    # Se elimina los duplicados
    if df.duplicated().sum().sum() > 0:
        print(f'Valores duplicados:\n{df.duplicated().sum()}')
        df = df.drop_duplicates()
    # Verificación de valores nulos
    elif df.isna().sum().sum() > 0:
        print(f'Valores faltantes/nulos por cada columna:\n{df.isna().sum()}\n')
        df = df.dropna(subset=['marca'])
    else:
        print('No hay valores nulos ni repetidos...')
    print('No hay valores nulos ni repetidos...')
    
    df = df.rename(columns={
        'id':'idProducto', 
        'marca':'marca', 
        'nombre':'nombre',
        'presentacion': 'presentacion', 
        'categoria1':'categoria1', 
        'categoria2':'categoria2',
       'categoria3':'categoria3'})
    return df

#-------------------------------#
# Creacion de la Tabla Producto #
#-------------------------------#
def crear_tabla_producto(tb_name):
    tb_name='producto'
    try:
        conn = msql.connect(
            host=host,
            user=user, 
            password=password)

        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
            cursor.execute("USE henrypi;")
            record = cursor.fetchone()
            #
            print("Estas conectado a la Base de Datos: ", db_name)
            #
            cursor.execute(f'DROP TABLE IF EXISTS `{tb_name}`;')
            print(f'Creando la tabla {tb_name}....')
            #
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{tb_name}` (
                `idProducto`		VARCHAR(20),
                `marca` 			VARCHAR(50),
                `nombre`			VARCHAR(150),
                `presentacion`		VARCHAR(150),
                `categoria1`		VARCHAR(50),
                `categoria2`		VARCHAR(50),
                `categoria3`		VARCHAR(50),
                PRIMARY KEY (`idProducto`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
            """)
            print("Tabla creada exitosamente....")
    except Error as e:
        print("Error al conectarse a MySQL", e)


def leer_archivo():
    file_path = input('Ingrese el nombre del archivo. Ejemplo archivo.extensio: ')    
    extension_archivo = os.path.basename(file_path).split('.')[1]
    data = pd.read_parquet(BASE_PATH + file_path)
    data_limpia = limpieza_producto(data)
    return data_limpia

def main():
    df = leer_archivo()
    tb_name = 'producto'
    try:
        conn = msql.connect(
            host=host,
            user=user, 
            password=password)

        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
            cursor.execute("USE henrypi;")
            record = cursor.fetchone()
            #
            print("Estas conectado a la Base de Datos: ", db_name)
            #
            cursor.execute('DROP TABLE IF EXISTS `producto`;')
            print('Creating table....')
            #
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS `producto` (
                    `idProducto`		VARCHAR(20),
                    `marca` 			VARCHAR(50),
                    `nombre`			VARCHAR(150),
                    `presentacion`		VARCHAR(150),
                    `categoria1`		VARCHAR(50),
                    `categoria2`		VARCHAR(50),
                    `categoria3`		VARCHAR(50),
                    PRIMARY KEY (`idProducto`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
            """)
            print("Tabla creada exitosamente....")
            df.to_sql(con=my_conn, name='producto', if_exists='append',index=False)
            print("Los registros se han cargado exitosamente....")
            
    except Error as e:
        print("Error al conectarse a MySQL", e)
    
if __name__ == '__main__':
    main()