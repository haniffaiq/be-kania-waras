import pandas as pd
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows
import psycopg2
from config import DATABASE_CONFIG
import pandas as pd
from tqdm import tqdm
import os
import datetime
import locale
from joblib import Parallel, delayed


# Membaca data supplier
# def pbfinser() :
#     def read_supplier_data(file_path, cabang):
#         df_supplier = pd.read_excel(file_path)
#         df_supplier = df_supplier.loc[:, ~df_supplier.columns.str.contains('^Unnamed')]
#         cols_to_check = ['Nama Barang', 'Satuan', 'Supplier Termurah', 'HPP Min', 'HPP Max', 'HPP Average', 'Harga Jual Min', 'Harga Jual Max']
#         df_supplier = df_supplier.dropna(subset=cols_to_check, how='all')
#         df_supplier['Nama Barang'] = df_supplier['Nama Barang'].str.lower()
#         df_supplier = df_supplier.sort_values(by='Supplier Termurah', key=lambda x: x.str[0])
#         df_supplier = df_supplier.fillna("PBF Tidak Ada")
#         df_supplier['Cabang'] = cabang
#         columns_to_drop = ['HPP Min', 'HPP Max', 'HPP Average', 'Harga Jual Min', 'Harga Jual Max', 'Status']
#         df_supplier = df_supplier.drop(columns=columns_to_drop)

#         # df_supplier = df_supplier.sort_values(by='PLU', ascending=True)

#         return df_supplier


#     pathDatabase_KWM = './Database PBF/Database PBF KWM.xlsx'
#     pathDatabase_KWB = './Database PBF/Database PBF KWB.xlsx'
#     pathDatabase_GEDA = './Database PBF/Database PBF GEDA.xlsx'
#     pathDatabase_MUT = './Database PBF/Database PBF MUT.xlsx'
#     pathDatabase_MUS = './Database PBF/Database PBF MUS.xlsx'
#     pathDatabase_KWU = './Database PBF/Database PBF KWU.xlsx'

#     df_supplier_KWM = read_supplier_data(pathDatabase_KWM, "KWM")
#     df_supplier_KWB = read_supplier_data(pathDatabase_KWB, "KWB")
#     df_supplier_GEDA = read_supplier_data(pathDatabase_GEDA, "GEDA")
#     df_supplier_MUT = read_supplier_data(pathDatabase_MUT, "MUT")
#     df_supplier_MUS = read_supplier_data(pathDatabase_MUS, "MUS")
#     df_supplier_KWU = read_supplier_data(pathDatabase_KWU, "KWU")



#     total_rows_1 = df_supplier_KWM.shape[0]
#     # total_rows_2 = df_supplier_KWB.shape[0]
#     # total_rows_3 = df_supplier_GEDA.shape[0]
#     # total_rows_4 = df_supplier_MUT.shape[0]
#     # total_rows_5 = df_supplier_MUS.shape[0]
#     # total_rows_6 = df_supplier_KWU.shape[0]

#     # sum_total = total_rows_1+total_rows_2+total_rows_3+total_rows_4+total_rows_5+total_rows_6
#     print(total_rows_1)


#     df_supplier_list = [df_supplier_KWM,df_supplier_KWB,df_supplier_GEDA,df_supplier_MUT,df_supplier_MUS,df_supplier_KWU]

#     # merged_df = pd.concat(df_supplier_list, ignore_index=True)
#     # print(merged_df.columns)


#     # conn = psycopg2.connect(**DATABASE_CONFIG)
#     # cursor = conn.cursor()

#     # # table_name = 'supplier'
#     # # merged_df.to_sql(table_name, conn, if_exists='replace', index=False)


#     # table_name = 'supplier'
#     # total_rows = len(merged_df)
#     # for index, row in tqdm(merged_df.iterrows(), total=total_rows, desc="Inserting data"):
#     #     sql = f"INSERT INTO {table_name} (plu, name, satuan,supplier,cabang) VALUES (%s, %s,%s, %s,%s)"
#     #     cursor.execute(sql, (row['PLU'], row['Nama Barang'], row['Satuan'], row['Supplier Termurah'], row['Cabang']))
#     # conn.commit()
#     # cursor.close()
#     # conn.close()

def read_obat_tidak_dijual_data(file_path, cabang):
    df_obat_tidak_dijual = pd.read_excel(file_path)
    df_obat_tidak_dijual = df_obat_tidak_dijual.fillna(0)
    df_obat_tidak_dijual = df_obat_tidak_dijual.loc[df_obat_tidak_dijual['Dijual'] == 1].copy()
    df_obat_tidak_dijual = df_obat_tidak_dijual.drop(columns=df_obat_tidak_dijual.columns[df_obat_tidak_dijual.columns.str.contains('Unnamed')])
    df_obat_tidak_dijual['Nama Produk'] = df_obat_tidak_dijual['Nama Produk'].str.lower()
    df_obat_tidak_dijual['Cabang'] = cabang
    columns_to_drop = ['Qty', 'Stok Min', 'Stok Max', 'Pesan']
    df_obat_tidak_dijual = df_obat_tidak_dijual.drop(columns=columns_to_drop)
    return df_obat_tidak_dijual


pathDataObatTidakDijual_KWM = './Data Obat Tidak Dijual/DataObatTidakDijual KWM.xlsx'
pathDataObatTidakDijual_KWB = './Data Obat Tidak Dijual/DataObatTidakDijual KWB.xlsx'
pathDataObatTidakDijual_GEDA = './Data Obat Tidak Dijual/DataObatTidakDijual GEDA.xlsx'
pathDataObatTidakDijual_MUT = './Data Obat Tidak Dijual/DataObatTidakDijual MUT.xlsx'
pathDataObatTidakDijual_MUS = './Data Obat Tidak Dijual/DataObatTidakDijual MUS.xlsx'
pathDataObatTidakDijual_KWU = './Data Obat Tidak Dijual/DataObatTidakDijual KWU.xlsx'

df_obat_tidak_dijual_KWM = read_obat_tidak_dijual_data(pathDataObatTidakDijual_KWM, "KWM")
df_obat_tidak_dijual_KWB = read_obat_tidak_dijual_data(pathDataObatTidakDijual_KWB, "KWB")
df_obat_tidak_dijual_GEDA = read_obat_tidak_dijual_data(pathDataObatTidakDijual_GEDA, "GEDA")
df_obat_tidak_dijual_MUT = read_obat_tidak_dijual_data(pathDataObatTidakDijual_MUT, "MUT")
df_obat_tidak_dijual_MUS = read_obat_tidak_dijual_data(pathDataObatTidakDijual_MUS, "MUS")
df_obat_tidak_dijual_KWU = read_obat_tidak_dijual_data(pathDataObatTidakDijual_KWU, "KWU")





df_supplier_list = [df_obat_tidak_dijual_KWM,df_obat_tidak_dijual_KWB,df_obat_tidak_dijual_GEDA,df_obat_tidak_dijual_MUT,df_obat_tidak_dijual_MUS,df_obat_tidak_dijual_KWU]
merged_df = pd.concat(df_supplier_list, ignore_index=True)
print(merged_df.columns)


# conn = psycopg2.connect(**DATABASE_CONFIG)
# cursor = conn.cursor()
# # table_name = 'supplier'
# # merged_df.to_sql(table_name, conn, if_exists='replace', index=False)


# table_name = 'not_selling'
# total_rows = len(merged_df)
# for index, row in tqdm(merged_df.iterrows(), total=total_rows, desc="Inserting data"):
#     sql = "INSERT INTO not_sell (plu, product_name, satuan, is_selling, cabang) VALUES (%s, %s, %s, %s, %s)"
#     cursor.execute(sql, (row['PLU'], row['Nama Produk'], row['Satuan'], bool(row['Dijual']), row['Cabang']))
# conn.commit()
# cursor.close()
# conn.close()

# Define a function to insert data

num_workers = 12

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(**DATABASE_CONFIG)

# Define the data insertion function
def insert_data(index):
    row = merged_df.iloc[index]
    conn = psycopg2.connect(**DATABASE_CONFIG)
    cursor = conn.cursor()
    sql = "INSERT INTO not_sell (plu, product_name, satuan, is_selling, cabang) VALUES (%s, %s, %s, %s, %s)"
    cursor.execute(sql, (row['PLU'], row['Nama Produk'], row['Satuan'], bool(row['Dijual']), row['Cabang']))
    conn.commit()
    cursor.close()

# Parallelize the data insertion using joblib
Parallel(n_jobs=-1)(delayed(insert_data)(index) for index in tqdm(range(len(merged_df)), desc="Inserting data"))

# Close the connection
conn.close()



