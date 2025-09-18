import requests
import mysql.connector
import json
import time
from utils.util import Util

# --- Conexão com o Banco de Dados ---
db = Util.get_db_connection()
cursor = db.cursor()

# 1. BUSCA LOCAL: Pega os códigos de todos os proprietários existentes.
print("Buscando IDs de proprietários locais...")
try:
    cursor.execute("SELECT codigo FROM proprietario")
    local_proprietario_ids = {row[0] for row in cursor.fetchall()}
    print(f"Encontrados {len(local_proprietario_ids)} proprietários no banco de dados local.")
except mysql.connector.Error as err:
    print(f"Erro ao buscar proprietários locais: {err}")
    local_proprietario_ids = set()

# --- Configurações da API ---
url = Util.get_endpoints(1) # /proprietarios/listar
api_key = Util.get_api_key()
headers = {"Accept": "application/json"}
quantidade = 50
pagina = 1

# --- Query SQL para Inserir ou Atualizar ---
sql_insert_update = """
    INSERT INTO proprietario (
        codigo, agencia, nome, cpfcnpj, credito_situacao, 
        credito_mensagem, codigo_credpago, corretor
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        agencia = VALUES(agencia),
        nome = VALUES(nome),
        cpfcnpj = VALUES(cpfcnpj),
        credito_situacao = VALUES(credito_situacao),
        credito_mensagem = VALUES(credito_mensagem),
        codigo_credpago = VALUES(codigo_credpago),
        corretor = VALUES(corretor)
"""

start_time = time.time()
print("\nIniciando sincronização de proprietários com a API...")
api_proprietario_ids = set() # Set para armazenar os códigos da API

while True:
    params = {
        "key": api_key,
        "pesquisa": json.dumps({
            "fields": [
                "Codigo", "Agencia", "Nome", "CPFCNPJ",
                "CreditoSituacao", "CreditoMensagem", "CODIGO_CREDPAGO",
                "Corretor"
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

    proprietarios_para_processar = []
    for k, proprietario in data.items():
        if not k.isdigit():
            continue

        codigo = Util.checa_campo(proprietario.get("Codigo"))
        if not codigo:
            continue
            
        api_proprietario_ids.add(int(codigo))

        proprietarios_para_processar.append((
            int(codigo),
            Util.checa_campo(proprietario.get("Agencia")),
            Util.checa_campo(proprietario.get("Nome")),
            Util.checa_campo(proprietario.get("CPFCNPJ")),
            Util.checa_campo(proprietario.get("CreditoSituacao")),
            Util.checa_campo(proprietario.get("CreditoMensagem")),
            Util.checa_campo(proprietario.get("CODIGO_CREDPAGO")),
            Util.checa_campo(proprietario.get("Corretor"))
        ))

    if proprietarios_para_processar:
        cursor.executemany(sql_insert_update, proprietarios_para_processar)
        db.commit()
        print(f"Página {pagina} processada. {len(proprietarios_para_processar)} proprietários inseridos/atualizados.")

    if pagina >= int(data.get('paginas', 0)):
        break

    pagina += 1
    time.sleep(0.2)

# --- Comparar e Deletar ---
ids_to_delete = local_proprietario_ids - api_proprietario_ids
if ids_to_delete:
    print(f"\nEncontrados {len(ids_to_delete)} proprietários para excluir.")
    
    delete_tuples = [(prop_id,) for prop_id in ids_to_delete]
    sql_delete = "DELETE FROM proprietario WHERE codigo = %s"
    
    cursor.executemany(sql_delete, delete_tuples)
    db.commit()
    print("Proprietários antigos foram excluídos com sucesso.")
else:
    print("\nNenhum proprietário para excluir.")

# --- Finalização ---
cursor.close()
db.close()
print("-" * 50)
print("Sincronização de proprietários concluída.")
print(f"Tempo total de execução: {time.time() - start_time:.2f} segundos.")
#a