import pandas as pd
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows
import os
import datetime
import locale
from io import BytesIO
import base64


class DataProcess():

    def read_lapstok_data(file_path):
        df_lapstok = pd.read_excel(file_path, header=6)
        df_lapstok = df_lapstok.drop(df_lapstok.columns[df_lapstok.columns.str.contains('unnamed', case=False)], axis=1)
        cols_to_check_lapstok = ['PLU', 'Nama Produk', 'Satuan', 'Qty', 'Stok Min', 'Stok Max', 'Pesan']
        df_lapstok = df_lapstok.dropna(subset=cols_to_check_lapstok)
        df_lapstok['Nama Produk'] = df_lapstok['Nama Produk'].str.lower()
        return df_lapstok
    
    def find_matching_product(df_supplier, product_name):
        matching_rows = df_supplier[df_supplier['name'].str.contains(product_name, case=False, regex=False, na=False)]
        if not matching_rows.empty:
            tempat_order_list = matching_rows['supplier'].dropna().str.strip().tolist()
            return tempat_order_list
        else:
            return None
    def export_to_excel(df):
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Sheet1', index=False)

            # Mengakses objek workbook dan worksheet dari writer
            workbook  = writer.book
            worksheet = writer.sheets['Sheet1']

            # Mendapatkan lebar kolom untuk setiap kolom
            for i, col in enumerate(df.columns):
                column_len = max(df[col].astype(str).str.len().max(), len(col)) + 2  # Lebar kolom minimal = panjang nama kolom
                worksheet.set_column(i, i, column_len)  # Mengatur lebar kolom

        # Ambil bytes dari file Excel yang telah disimpan
        excel_data = output.getvalue()

        # Kodekan bytes ke dalam string base64
        base64_data = base64.b64encode(excel_data).decode('utf-8')
        return base64_data