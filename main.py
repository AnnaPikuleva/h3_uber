import h3pandas
import geopandas as gpd
from sqlalchemy import create_engine
import psycopg2
import h3
from tkinter import _flatten
from collections.abc import Iterable
import h3converter
import pandas as pd

from shapely.geometry import shape

def select_pg(sql):
    return pd.read_sql(sql, con_base)

# path = 'mahala.geojson'
# con_base = create_engine("postgresql://postgres:gfhjkm_1@192.168.2.104:5432/spatial001")
# sql = '''select *
# from addresses.mahalla_registry_union_geom'''
# df = pd.DataFrame(select_pg(sql))
# gdf = gpd.GeoDataFrame(df, geometry=df.geom)


size = 10
buffer_metres = 1000
#название схемы
schema = 'public'
#название таблицы
name_table = 'spb'


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

connection = create_connection("spatial001", "postgres", "gfhjkm_1", "192.168.2.104", "5432")
cursor = connection.cursor()

cursor.execute(f"DROP TABLE IF EXISTS {schema}.{name_table}")
# cursor.execute(f"CREATE UNLOGGED TABLE {schema}.{name_table} (id integer primary key, geom geometry, h3_index integer, h3_polyfill varchar(255), level_size integer)")
cursor.execute(f"CREATE UNLOGGED TABLE {schema}.{name_table} (geom geometry, h3_polyfill varchar(255), level_size integer)")
# cursor.execute(f"CREATE UNIQUE INDEX id_h3_index ON {schema}.{name_table} (id)")

gdf = gpd.read_file('spb.geojson')
print(gdf)
gdf['geometry'] = gdf['geometry'].to_crs(3857)
gdf['geometry'] = gdf['geometry'].to_crs(4326)
gex = gdf.h3.polyfill(size)
h3_polyfill = gex['h3_polyfill']

# print(h3_polyfill)

for i in h3_polyfill:

    for index in i:
        list = []
        hex = h3.h3_to_geo_boundary(index, geo_json=True)
        hex = str(hex)
        hex = hex.replace('(', '[')
        hex = hex.replace(')', ']')
        hex = '['+hex+ ']'
        index = index
        hex = '{'+'"type": "Polygon", "coordinates":'+ hex +'}'



        cursor.execute(f'''INSERT INTO {schema}.{name_table} 
        (geom, h3_polyfill, level_size) VALUES (st_transform(st_setsrid(st_geomfromgeojson('{hex}'),4326),3857), '{index}', '{size}');''')


        connection.commit()

cursor.close()
connection.close()

con_base = create_engine('postgresql+psycopg2://postgres:gfhjkm_1@192.168.2.104/spatial001')


sql = f"SELECT * FROM {schema}.{name_table}"
gdf = gpd.read_postgis(sql, con_base, crs='epsg:3857')
print(gdf)
gdf.to_file(f"{name_table}.geojson', driver='GeoJSON")











