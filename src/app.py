import pyspark
import subprocess
import os
import json

from flask import Flask, request, send_from_directory

op_map = {
    "eq": "=",
    "gt": ">",
    "lt": "<",
    "in": "in"
}

app = Flask(__name__)
sc = pyspark.SparkContext()
sql =pyspark.SQLContext(sc)

#   get columns datatype metadata
def get_columns_obj():
    tempdata = {}
    with open('temp.json', 'r') as f:
        tempdata = json.load(f)
    return tempdata

#   build sql query based on api parameters
def generate_sql(req, columns):
    where = ""
    limit = ""
    if 'q' in request.args:
        param = json.loads(request.args['q'])
        keys = param.keys()
        if len(keys)>0:
            # get column name
            col = list(keys)[0]
            col_lower = col.lower()

            # check if column name in query is valid
            columns_map = {c.lower():c for c in columns}
            if col_lower not in columns_map:
                raise ValueError('column {0} not exist'.format(col))

            # get operator
            op_keys = param[col].keys()
            op = list(op_keys)[0]
            
            if columns[col.upper()] =='string' or columns[col.upper()] == 'date':
                format_str = "'{0}'"
            else:
                format_str = "{0}"

            # get value
            val_data = param[col][op]
            val = ""
            if (op_map[op]=='in'):
                val = "({0})".format(','.join(format_str.format(x) for x in val_data))
            else:
                val = format_str.format(val_data)

            # build where clause
            where = " where {0} {1} {2}".format(columns_map[col_lower], op_map[op], val)

    if 'max' in request.args:
        limit = " limit {0}".format(request.args['max'])

    sql_str = "select * from irs " + where + limit
    return sql_str

#   retrieve columns and datatype information of loaded data
@app.route('/api/irs/column', methods=['GET'])
def get_column_def():
    data = get_columns_obj()
    if ('columns' in data):
        return json.dumps(data['columns']), 200
    else:
        return "{}", 200

#   load data from amazon public data
@app.route('/api/irs/ingest', methods=['GET'])
def ingest():
    try:
        # copy file
        subprocess.run(['aws s3 cp s3://irs-form-990/index_2011.csv ../testdata --no-sign-request --quiet'],shell=True)
        df = (sql.read.format("com.databricks.spark.csv").option("header", "true").load("../testdata/index_2011.csv"))
        df.registerTempTable("irs")
        # save columns datatype information
        with open('temp.json', 'r+') as f:
            tempdata={}
            tempdata["columns"] = dict(df.dtypes)
            json.dump(tempdata, f)
        return str(df.count()) + " records ingested", 200
    except Exception as e:
        return str(e), 500

#   retrieve sample data
@app.route('/api/irs/sample', methods=['GET'])
def get_sample():
    try:
        col_data = get_columns_obj()
        if not 'columns' in col_data:
            raise Exception('data not loaded')
        result = sql.sql("select * from irs limit 20")
        return json.dumps(result.rdd.collect()), 200
    except Exception as e:
        return str(e), 500

#   query data
@app.route('/api/irs/data', methods=['GET'])
def get_data():
    try:
        col_data = get_columns_obj()
        if not 'columns' in col_data:
            raise Exception('data not loaded')
        
        result = sql.sql(generate_sql(request.args, col_data["columns"]))
        return json.dumps(result.rdd.collect()), 200
    except ValueError as e:
        return str(e), 400
    except Exception as e:
        return str(e), 500

@app.route('/')
def home():
    return send_from_directory('./static', 'index.html')

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)