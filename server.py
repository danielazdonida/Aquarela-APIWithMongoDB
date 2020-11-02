from flask import Flask, Response
import pymongo
import pandas as pd
import json
import re

app = Flask(__name__)

mongo = pymongo.MongoClient(
    host="localhost",
    port=27017)

db = mongo.cereals_file

csvFile = pd.read_csv('cereal.csv')
csv = csvFile.to_json(orient="table")
parsed = json.loads(csv)


# Carrega o JSON proveniente do arquivo CSV 'Cereals' no banco de dados MongoDB
@app.route("/", methods=["POST"])
def create_json():
    db_response = db.cereals_info.insert_one(parsed)
    return Response(
        response=json.dumps(
            {"message": "file created",
             "id": f"{db_response}"
             }),
        status=200,
        mimetype="application/json"
    )


# API que lê e retorna o JSON completo que está no banco de dados
@app.route("/json", methods=["GET"])
def get_json():
    data = list(db.cereals_info.find())
    for user in data:
        user["_id"] = str(user["_id"])
    return Response(
        response=json.dumps(data),
        status=200,
        mimetype="application/json"
    )


# API retorna os N primeiros objetos do JSON
@app.route("/firstrows/<int:n>", methods=["GET"])
def api_firstrows(n):
    data = list(db.cereals_info.aggregate([
        {"$unwind": "$data"},
        {"$limit": n},
        {"$group": {"_id": "$_id", "data": {"$push": "$data"}}}
    ]
    ))
    for user in data:
        user["_id"] = str(user["_id"])
    return Response(
        response=json.dumps(data),
        status=200,
        mimetype="application/json"
    )


# API que filtra coluna e número de linhas do JSON
@app.route("/filterrows/<field>=<fieldname>&n=<int:n>", methods=["GET"])
def api_filterrows(field, fieldname, n):
    if (re.match('^\d+$', fieldname)):
        fieldname = int(fieldname)
    elif re.match('^\d+\.\d+$', fieldname):
        fieldname = float(fieldname)

    data = list(db.cereals_info.aggregate([
        {"$unwind": "$data"},
        {"$match": {"data." + field: fieldname}},
        {"$limit": n},
        {"$group": {"_id": "$_id", "data": {"$push": "$data"}}}
    ]
    ))
    for user in data:
        user["_id"] = str(user["_id"])
    return Response(
        response=json.dumps(data),
        status=200,
        mimetype="application/json"
    )


# API que retorna as possibilidades de preenchimentos dos campos do JSON
@app.route('/filter/options/<string:field>', methods=['GET'])
def api_filter_options(field):
    data = list(db.cereals_info.aggregate([
        {"$unwind": "$data"},
        {"$group": {"_id": "$_id", "data": {"$addToSet": "$data." + field}}}
    ]))

    for user in data:
        user["_id"] = str(user["_id"])
    return Response(
        response=json.dumps(data),
        status=200,
        mimetype="application/json"
    )


if __name__ == "__main__":
    app.run(port=80, debug=True)
