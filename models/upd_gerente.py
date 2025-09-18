import requests
import mysql.connector
import json
import time

from utils.util import Util

db = Util.get_db_connection()

cursor = db.cursor()
url = Util.get_endpoints(0) # /usuarios/listar

headers = {"Accept": "application/json"}
api_key = Util.get_api_key()
quantidade = 50
pagina = 1

sql_query = """
    INSERT INTO gerente (codigo, nome) VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE nome = VALUES(nome);
"""

gerentes_processados = []

inicio = time.time()

while True:
    params = {
        "key": api_key,
        "pesquisa": json.dumps({
            "fields": [
                "Codigo",
                {"GerenteDoCorretor" : ["Nome"]}
            ],
            "paginacao": {"pagina": str(pagina), "quantidade": str(quantidade)}
        }),
        "showtotal": "1"
    }

    r = requests.get(url, params=params, headers=headers)
    data = r.json()

    if "total" not in data or not any(k.isdigit() for k in data.keys()):
        print("Nenhum dado retornado, finalizando.")
        break

    print(f"Inserindo página {pagina} / {data.get('paginas')}")

    for k, usuario in data.items():
        
        if not k.isdigit():
            continue

        gerentes = usuario.get("GerenteDoCorretor", [])

        try:
            for k_gerente, gerente in gerentes.items():
                nome_gerente = gerente.get("Nome")

                values = (k_gerente, nome_gerente)

                if values not in gerentes_processados:
                    gerentes_processados.append(values)
                    cursor.execute(sql_query, values)
                    print(f"Gerente: {nome_gerente}, Código: {k_gerente} foi processado.")
                
        except Exception as e:
            continue
    
    db.commit()
    print(f"Página {pagina} inserida com sucesso!")

    if pagina >= int(data.get("paginas", 0)):
        break

    pagina += 1
    time.sleep(0.2)

fim = time.time()
duracao = fim - inicio

cursor.close()
db.close()
print("Todos os gerentes foram atualizados com sucesso.")
print(f"Tempo de execução: {duracao:.4f} segundos")