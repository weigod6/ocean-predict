import dateutil
from flask import Flask, request, send_file, jsonify
from flask_cors import CORS
import os
import datetime
from dateutil.relativedelta import relativedelta

app = Flask(__name__)
CORS(app)  # 允许所有来源的请求

# 假设所有的CSV文件都保存在一个目录中
CSV_DIR = 'output_csv'


def get_csv_filename_by_date(date_str):
    try:

        # 解析日期字符串
        date = datetime.datetime.strptime(date_str, '%Y-%m-%d')

        #后期要删掉
        date = date - dateutil.relativedelta.relativedelta(years=3)
        # 假设CSV文件名的格式是 YYYY-MM-DD.csv
        filename = date.strftime('%Y-%m-%d.csv')
        file_path = os.path.join(CSV_DIR, filename)
        return file_path if os.path.exists(file_path) else None
    except ValueError:
        return None


@app.route('/sst/<data>', methods=['GET'])
def get_csv(data):

    date_str = data
    file_path = get_csv_filename_by_date(date_str)

    if file_path:
        return send_file(file_path, as_attachment=True)
    else:
        return jsonify({'error': 'File not found'}), 404


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')  # 监听所有IP地址的请求
