import json
import pandas as pd

csv_file = pd.read_csv('cereal.csv')


# Teste da API que retorna todo o arquivo JSON
def test_api_json(client):
    csv = csv_file.to_json(orient="table")
    parsed = json.loads(csv)

    res = client.get('/json', json=parsed)
    response = res.get_json()
    get_id = [d['_id'] for d in response]  # retorna o ID dentro de uma lista
    _id = get_id[0]
    _dict = {"_id": _id}
    _dict.update(parsed)

    assert res.status_code == 200

    assert ([_dict] == response)


# Teste unitário da API que retorna apenas 5 linhas do arquivo JSON
def test_api_first5rows(client):
    edited_csv = csv_file.head(5)
    csv = edited_csv.to_json(orient="table")
    parsed = json.loads(csv)
    res = client.get('/firstrows/5', json=parsed)
    response = res.get_json()

    parsed.pop("schema")  # retirar o campo "schema do dicionário

    get_id = [d['_id'] for d in response]  # retorna o ID que o banco de dados atribuiu ao documento
    _id = get_id[0]
    _dict = {"_id": _id}  # adiciona o ID do banco de dados ao documento utilizado para a comparação
    _dict.update(parsed)

    assert res.status_code == 200

    assert ([_dict] == response)


# Teste unitário da API que retorna apenas 20 linhas do arquivo JSON
def test_api_first20rows(client):
    edited_csv = csv_file.head(20)
    csv = edited_csv.to_json(orient="table")
    parsed = json.loads(csv)
    res = client.get('/firstrows/20', json=parsed)
    response = res.get_json()

    parsed.pop("schema")  # retirar o campo "schema do dicionário

    get_id = [d['_id'] for d in response]  # retorna o ID que o banco de dados atribuiu ao documento
    _id = get_id[0]
    _dict = {"_id": _id}  # adiciona o ID do banco de dados ao documento utilizado para a comparação
    _dict.update(parsed)

    assert res.status_code == 200

    assert ([_dict] == response)


# Teste unitário da API que retorna apenas 45 linhas do arquivo JSON
def test_api_first45rows(client):
    edited_csv = csv_file.head(45)
    csv = edited_csv.to_json(orient="table")
    parsed = json.loads(csv)
    res = client.get('/firstrows/45', json=parsed)
    response = res.get_json()

    parsed.pop("schema")  # retirar o campo "schema do dicionário

    get_id = [d['_id'] for d in response]  # retorna o ID que o banco de dados atribuiu ao documento
    _id = get_id[0]
    _dict = {"_id": _id}  # adiciona o ID do banco de dados ao documento utilizado para a comparação
    _dict.update(parsed)

    assert res.status_code == 200

    assert ([_dict] == response)


# Teste unitário da API que filtra coluna (mfr=K) e nº de linhas (n=20)
def test_api_filter_mfr_K_and_20_rows(client):
    csv_file_column_filter = csv_file.loc[csv_file['mfr'] == 'K']
    csv_file_row_filter = csv_file_column_filter.head(20)
    csv = csv_file_row_filter.to_json(orient="table")
    parsed = json.loads(csv)

    res = client.get('/filterrows/mfr=K&n=20', json=parsed)
    response = res.get_json()

    parsed.pop("schema")  # retirar o campo "schema do dicionário

    get_id = [d['_id'] for d in response]  # retorna o ID que o banco de dados atribuiu ao documento
    _id = get_id[0]
    _dict = {"_id": _id}  # adiciona o ID do banco de dados ao documento utilizado para a comparação
    _dict.update(parsed)

    assert res.status_code == 200

    assert ([_dict] == response)


# Teste unitário da API que filtra coluna (mfr=N) e nº de linhas (n=3)
def test_api_filter_mfr_N_and_3_rows(client):
    csv_file_column_filter = csv_file.loc[csv_file['mfr'] == 'N']
    csv_file_row_filter = csv_file_column_filter.head(3)
    csv = csv_file_row_filter.to_json(orient="table")
    parsed = json.loads(csv)

    res = client.get('/filterrows/mfr=N&n=3', json=parsed)
    response = res.get_json()

    parsed.pop("schema")

    get_id = [d['_id'] for d in response]
    _id = get_id[0]
    _dict = {"_id": _id}
    _dict.update(parsed)

    assert res.status_code == 200

    assert ([_dict] == response)


# Teste unitário da API que filtra coluna (type=C) e nº de linhas (n=10)
def test_api_filter_type_C_and_10_rows(client):
    csv_file_column_filter = csv_file.loc[csv_file['type'] == 'C']
    csv_file_row_filter = csv_file_column_filter.head(10)
    csv = csv_file_row_filter.to_json(orient="table")
    parsed = json.loads(csv)

    res = client.get('/filterrows/type=C&n=10', json=parsed)
    response = res.get_json()

    parsed.pop("schema")

    get_id = [d['_id'] for d in response]
    _id = get_id[0]
    _dict = {"_id": _id}
    _dict.update(parsed)

    assert res.status_code == 200

    assert ([_dict] == response)


# Teste unitário da API que retorna os possibilidades de resposta do campo mfr
def test_api_filter_options_mfr(client):
    column = csv_file['mfr']
    list(column)

    filtered_list = []
    for i in column:
        if i not in filtered_list:
            filtered_list.append(i)

    final_list = []
    for j in filtered_list:
        final_list.append(str(j))

    csv_file_column_filter = csv_file['mfr']
    csv = csv_file_column_filter.to_json(orient="table")
    parsed = json.loads(csv)

    res = client.get('/filter/options/mfr', json=parsed)
    response = res.get_json()
    _dict = response[0]
    _filter_dict = _dict.get('data')

    assert res.status_code == 200

    assert (set(_filter_dict) == set(final_list))  # set() permite comparar duas listas desordenadas


# Teste unitário da API que retorna os possibilidades de resposta do campo type
def test_api_filter_options_type(client):
    column = csv_file['type']
    list(column)

    filtered_list = []
    for i in column:
        if i not in filtered_list:
            filtered_list.append(i)

    final_list = []
    for j in filtered_list:
        final_list.append(str(j))

    csv_file_column_filter = csv_file['type']
    csv = csv_file_column_filter.to_json(orient="table")
    parsed = json.loads(csv)

    res = client.get('/filter/options/type', json=parsed)
    response = res.get_json()
    _dict = response[0]
    _filter_dict = _dict.get('data')

    assert res.status_code == 200

    assert (set(_filter_dict) == set(final_list))  # set() permite comparar duas listas desordenadas


# Teste unitário da API que retorna os possibilidades de resposta do campo calories
def test_api_filter_options_calories(client):
    column = csv_file['calories']
    list(column)

    final_list = []
    for i in column:
        if i not in final_list:
            final_list.append(i)

    csv_file_column_filter = csv_file['calories']
    csv = csv_file_column_filter.to_json(orient="table")
    parsed = json.loads(csv)

    res = client.get('/filter/options/calories', json=parsed)
    response = res.get_json()
    _dict = response[0]
    _filter_dict = _dict.get('data')

    assert res.status_code == 200

    assert (set(_filter_dict) == set(final_list))  # set() permite comparar se duas listas desordenadas possuem os mesmos dados
