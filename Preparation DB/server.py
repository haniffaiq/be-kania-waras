from flask import Flask, request, jsonify
import psycopg2
from config import DATABASE_CONFIG
import base64
import io
import pandas as pd
from data_process import DataProcess

app = Flask(__name__)

def connect_to_database():
    conn = psycopg2.connect(**DATABASE_CONFIG)
    return conn

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
def get_data():
    try:
        query = 'SELECT * FROM public.supplier'
        data = execute_query(query)
        return jsonify(data.to_dict(orient='records')), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/data', methods=['POST'])
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
                    'Satuan' : satuan
                })

        df_tempat_order = pd.DataFrame(result_rows, columns=['Nama Produk', 'Tempat Order', 'Jumlah Pesan', 'Satuan'])
        df_tempat_order = df_tempat_order[~df_tempat_order['Nama Produk'].isin(data_not_selling['product_name'])]
        df_tempat_order['Tempat Order'] = df_tempat_order['Tempat Order'].astype(str)

        df_grouped = df_tempat_order.groupby('Tempat Order').apply(lambda x: x.apply(lambda y: pd.Series(y[y.notnull()]), axis=1)).reset_index(drop=True)
        data_base64 = DataProcess.export_to_excel(df_grouped)

        return jsonify({'message': message, 'data': data_base64}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)





# from flask import Flask, request, jsonify
# import psycopg2
# from config import DATABASE_CONFIG
# import json
# import base64
# import io
# import pandas as pd
# from data_process import DataProcess

# app = Flask(__name__)

# # Fungsi untuk menghubungkan ke database
# def connect_to_database():
#     conn = psycopg2.connect(**DATABASE_CONFIG)
#     return conn

# # Endpoint untuk mengambil data dari database
# @app.route('/api/data', methods=['GET'])
# def get_data():
#     try:
#         conn = connect_to_database()
#         cursor = conn.cursor()


#         query1 = 'SELECT * FROM public.supplier'
#         cursor.execute(query1)
#         data = cursor.fetchall()
#         cursor.close()
#         conn.close()

#         return jsonify(data), 200
#     except Exception as e:
#         return jsonify({'error': str(e)}), 500

# # Endpoint untuk menambahkan dan memproses data
# @app.route('/api/data', methods=['POST'])
# def add_data():
#     try:
#         request_data = request.get_json()
#         base64_data = request_data.get('file', '')
#         binary_data = base64.b64decode(base64_data)

#         data_buffer = io.BytesIO(binary_data)

#         # Memanggil fungsi untuk memproses data (DataProcess.read_lapstok_data)
#         data_lapstok = DataProcess.read_lapstok_data(data_buffer)

#         print(data_lapstok.columns)



#         nama_cabang = request_data.get('cabang')
#         message = f"Data dari cabang {nama_cabang} berhasil dibaca"
        
#         conn = connect_to_database()
#         cursor = conn.cursor()
#         query1 = 'SELECT * FROM public.supplier WHERE public.supplier.cabang = %s'
#         cursor.execute(query1, (nama_cabang,))
#         column_names = [desc[0] for desc in cursor.description]
#         data = cursor.fetchall()
#         data_supplier = pd.DataFrame(data, columns=column_names)
#         cursor.close()
#         conn.close()
#         print(data_supplier.columns)

#         conn = connect_to_database()
#         cursor = conn.cursor()
#         query2 = 'SELECT * FROM public.not_sell WHERE public.not_sell.cabang = %s'
#         cursor.execute(query2, (nama_cabang,))
#         column_names = [desc[0] for desc in cursor.description]
#         data = cursor.fetchall()
#         data_not_selling = pd.DataFrame(data, columns=column_names)
#         cursor.close()
#         conn.close()
#         print(data_not_selling.columns)

#         result_rows = []
#         for index, row in data_lapstok.iterrows():
#             nama_produk = row['Nama Produk']
#             pesan = row['Pesan']
#             satuan = row['Satuan']

#             # Cari tempat order menggunakan fungsi find_matching_product
#             supplier_match = DataProcess.find_matching_product(data_supplier, nama_produk)

#             # Jika ada tempat order yang cocok
#             if supplier_match:
#                 jumlah_pesan = pesan

#                 # Tambahkan baris ke list hasil
#                 result_rows.append({
#                     'Nama Produk': nama_produk,
#                     'Tempat Order': supplier_match,
#                     'Jumlah Pesan': jumlah_pesan,
#                     'Satuan' : satuan
#                 })

#         df_tempat_order = pd.DataFrame(result_rows, columns=['Nama Produk', 'Tempat Order', 'Jumlah Pesan', 'Satuan'])

#         # Hapus produk yang tidak dijual
#         df_tempat_order = df_tempat_order[~df_tempat_order['Nama Produk'].isin(data_not_selling['product_name'])]
#         df_tempat_order['Tempat Order'] = df_tempat_order['Tempat Order'].astype(str)


#         df_grouped = df_tempat_order.groupby('Tempat Order').apply(lambda x: x.apply(lambda y: pd.Series(y[y.notnull()]), axis=1)).reset_index(drop=True)

#         # print(df_grouped)
#         data_base64 = DataProcess.export_to_excel(df_grouped)

        

#         return jsonify({
#             'message': message,
#             'data' : data_base64
#             }), 201
#     except Exception as e:
#         return jsonify({'error': str(e)}), 500

# if __name__ == '__main__':
#     app.run(debug=True)

