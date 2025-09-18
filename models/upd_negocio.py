import requests
import mysql.connector
import json
import time
from utils.util import Util

db = Util.get_db_connection()
cursor = db.cursor()

cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")

print("Buscando IDs de negócios locais...")
try:
    cursor.execute("SELECT codigo FROM negocio")
    local_negocio_ids = {row[0] for row in cursor.fetchall()}
    print(f"Encontrados {len(local_negocio_ids)} negócios no banco de dados local.")
except mysql.connector.Error as err:
    print(f"Erro ao buscar negócios locais: {err}")
    local_negocio_ids = set()

url = Util.get_endpoints(2)
api_key = Util.get_api_key()
headers = {"Accept": "application/json"}
quantidade = 50
pagina = 1

sql_insert_update = """
    INSERT INTO negocio (
        codigo, nome_negocio, data_final, nome_cliente, veiculo_captacao, 
        valor_negocio, status, codigo_imovel, codigo_cliente
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        nome_negocio = VALUES(nome_negocio),
        data_final = VALUES(data_final),
        nome_cliente = VALUES(nome_cliente),
        veiculo_captacao = VALUES(veiculo_captacao),
        valor_negocio = VALUES(valor_negocio),
        status = VALUES(status),
        codigo_imovel = VALUES(codigo_imovel),
        codigo_cliente = VALUES(codigo_cliente)
"""

start_time = time.time()
print("\nIniciando sincronização de negócios com a API...")
api_negocio_ids = set()

while True:
    params = {
        "key": api_key,
        "codigo_pipe": 1,
        "pesquisa": json.dumps({
            "fields": [
                "Codigo", "NomeNegocio", "DataFinal", "NomeCliente",
                "VeiculoCaptacao", "ValorNegocio", "Status",
                "CodigoImovel", "CodigoCliente"
            ],
            "paginacao": {"pagina": str(pagina), "quantidade": str(quantidade)}
        }),
        "showtotal": "1"
    }

    r = requests.get(url, params=params, headers=headers)
    data = r.json()

    if "total" not in data or not any(k.isdigit() for k in data.keys()):
        print("Nenhum dado retornado ou fim da paginação.")
        break

    print(f"Processando página {pagina}/{data.get('paginas')}...")

    negocios_para_processar = []
    for k, negocio in data.items():
        if not k.isdigit():
            continue

        codigo = Util.checa_campo(negocio.get("Codigo"))
        if not codigo:
            continue
            
        api_negocio_ids.add(int(codigo))

        data_final = Util.trata_data(negocio.get("DataFinal"))

        negocios_para_processar.append((
            codigo,
            Util.checa_campo(negocio.get("NomeNegocio")),
            data_final,
            Util.checa_campo(negocio.get("NomeCliente")),
            Util.checa_campo(negocio.get("VeiculoCaptacao")),
            Util.checa_campo(negocio.get("ValorNegocio")),
            Util.checa_campo(negocio.get("Status")),
            Util.checa_campo(negocio.get("CodigoImovel")),
            Util.checa_campo(negocio.get("CodigoCliente"))
        ))

    if negocios_para_processar:
        try:
            cursor.executemany(sql_insert_update, negocios_para_processar)
            db.commit()
            print(f"Página {pagina} processada. {len(negocios_para_processar)} negócios inseridos/atualizados.")
        except mysql.connector.Error as err:
            print(f"\nERRO DE INTEGRIDADE NA PÁGINA {pagina}: {err}")
            print("Verifique se os scripts de 'imovel' e 'cliente' foram executados primeiro.")
            print("Cancelando a operação para evitar dados inconsistentes.")
            break

    if pagina >= int(data.get('paginas', 0)):
        break

    pagina += 1
    time.sleep(0.2)

ids_to_delete = local_negocio_ids - api_negocio_ids
if ids_to_delete:
    print(f"\nEncontrados {len(ids_to_delete)} negócios para excluir.")
    
    delete_tuples = [(negocio_id,) for negocio_id in ids_to_delete]
    sql_delete = "DELETE FROM negocio WHERE codigo = %s"
    
    cursor.executemany(sql_delete, delete_tuples)
    db.commit()
    print("Negócios antigos foram excluídos com sucesso.")
else:
    print("\nNenhum negócio para excluir.")

cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
cursor.close()
db.close()
print("-" * 50)
print("Sincronização de negócios concluída.")
print(f"Tempo total de execução: {time.time() - start_time:.2f} segundos.")