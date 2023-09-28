from app import app
from flask import request, jsonify
import base64
import io
import pandas as pd
from .data_process import DataProcess
import psycopg2
from config import DATABASE_CONFIG

from flask import Blueprint, jsonify
from flask_cors import cross_origin  # Import cross_origin decorator untuk mengontrol CORS

api = Blueprint("api", __name__)
# Fungsi untuk menghubungkan ke database
def connect_to_database():
    conn = psycopg2.connect(**DATABASE_CONFIG)
    return conn

# Fungsi eksekusi query
def execute_query(query, params=()):
    conn = connect_to_database()
    cursor = conn.cursor()
    cursor.execute(query, params)
    column_names = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return pd.DataFrame(data, columns=column_names)

@app.route('/api/data', methods=['GET'])
# @cross_origin(origin='http://localhost:3000')  # Tambahkan decorator cross_origin ke rute ini
@cross_origin(origin='*')
def get_data():
    try:
        query = 'SELECT * FROM public.supplier'
        data = execute_query(query)
        return jsonify(data.to_dict(orient='records')), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/data', methods=['POST'])
# @cross_origin(origin='http://localhost:3000')  # Tambahkan decorator cross_origin ke rute ini
@cross_origin(origin='*')
def add_data():
    try:
        request_data = request.get_json()
        base64_data = request_data.get('file', '')
        binary_data = base64.b64decode(base64_data)
        data_buffer = io.BytesIO(binary_data)

        data_lapstok = DataProcess.read_lapstok_data(data_buffer)
        nama_cabang = request_data.get('cabang')
        message = f"Data dari cabang {nama_cabang} berhasil dibaca"

        data_supplier = execute_query('SELECT * FROM public.supplier WHERE public.supplier.cabang = %s', (nama_cabang,))
        data_not_selling = execute_query('SELECT * FROM public.not_sell WHERE public.not_sell.cabang = %s', (nama_cabang,))

        result_rows = []
        for index, row in data_lapstok.iterrows():
            nama_produk = row['Nama Produk']
            pesan = row['Pesan']
            satuan = row['Satuan']

            # Cari tempat order menggunakan fungsi find_matching_product
            supplier_match = DataProcess.find_matching_product(data_supplier, nama_produk)

            # Jika ada tempat order yang cocok
            if supplier_match:
                jumlah_pesan = pesan

                # Tambahkan baris ke list hasil
                result_rows.append({
                    'Nama Produk': nama_produk,
                    'Tempat Order': supplier_match,
                    'Jumlah Pesan': jumlah_pesan,
                    'Satuan': satuan
                })

        df_tempat_order = pd.DataFrame(result_rows, columns=['Nama Produk', 'Tempat Order', 'Jumlah Pesan', 'Satuan'])
        df_tempat_order = df_tempat_order[~df_tempat_order['Nama Produk'].isin(data_not_selling['product_name'])]
        df_tempat_order['Tempat Order'] = df_tempat_order['Tempat Order'].astype(str)

        df_grouped = df_tempat_order.groupby('Tempat Order').apply(lambda x: x.apply(lambda y: pd.Series(y[y.notnull()]), axis=1)).reset_index(drop=True)
        data_base64 = DataProcess.export_to_excel(df_grouped)

        return jsonify({'message': message, 'data': data_base64}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500
