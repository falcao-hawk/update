import requests
import json
import time
import mysql.connector
from utils.util import Util

db = Util.get_db_connection()
cursor = db.cursor()

print("Buscando IDs de usuários locais...")
cursor.execute("SELECT codigo FROM usuario")
local_user_ids = {row[0] for row in cursor.fetchall()}
print(f"Encontrados {len(local_user_ids)} usuários no banco de dados local.")


url = Util.get_endpoints(0) # /usuarios/listar
api_key = Util.get_api_key()
headers = {"Accept": "application/json"}
quantidade = 50
pagina = 1


sql_insert_update_usuario = """
    INSERT INTO usuario (
        codigo, data_cadastro, nome, nome_completo, inativo, 
        empresa, gerente_codigo, equipe_codigo
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        nome = VALUES(nome), nome_completo = VALUES(nome_completo),
        inativo = VALUES(inativo), empresa = VALUES(empresa),
        gerente_codigo = VALUES(gerente_codigo), equipe_codigo = VALUES(equipe_codigo)
"""

print("\nIniciando sincronização com a API...")
start_time = time.time()
api_user_ids = set() # Set para armazenar todos os códigos de usuários vindos da API

while True:
    params = {
        "key": api_key,
        "pesquisa": json.dumps({
            "fields": [
                "Empresa", "Datacadastro", "Nome", "Nomecompleto", "Inativo",
                {"GerenteDoCorretor": ["Nome"]}, {"Equipe": ["Nome"]}
            ],
            "paginacao": {"pagina": str(pagina), "quantidade": str(quantidade)}
        }),
        "showtotal": "1",
        "showSuspended": "1"
    }
    
    r = requests.get(url, params=params, headers=headers)
    data = r.json()

    if "total" not in data or not any(k.isdigit() for k in data.keys()):
        print("API não retornou dados de usuários. Finalizando loop.")
        break
        
    print(f"Processando página {pagina}/{data.get('paginas')}...")
    
    usuarios_para_processar = []

    for k, user in data.items():
        if not k.isdigit():
            continue

        codigo_usuario = int(user["Codigo"])
        api_user_ids.add(codigo_usuario) # Adiciona o código ao set da API

        gerente = user.get("GerenteDoCorretor")
        gerente_codigo = None
        if isinstance(gerente, dict) and gerente:
            gerente_codigo = int(list(gerente.keys())[0])
            gerente_nome = list(gerente.values())[0].get("Nome", f"Gerente {gerente_codigo}")[:45]
            cursor.execute("INSERT IGNORE INTO gerente (codigo, nome) VALUES (%s, %s)", (gerente_codigo, gerente_nome))

        equipe = user.get("Equipe")
        equipe_codigo = None
        if isinstance(equipe, dict) and equipe:
            equipe_codigo = int(list(equipe.keys())[0])
            equipe_nome = list(equipe.values())[0].get("Nome", f"Equipe {equipe_codigo}")[:45]
            cursor.execute("INSERT IGNORE INTO equipe (codigo, nome) VALUES (%s, %s)", (equipe_codigo, equipe_nome))
        
        data_cadastro = Util.trata_data(user.get("Datacadastro"))
        nome_completo = Util.checa_campo(user.get("Nomecompleto"))
        nome = Util.checa_campo(user.get("Nome"))
        inativo = Util.checa_campo(user.get("Inativo"))
        empresa = Util.checa_campo(user.get("Empresa"))

        usuarios_para_processar.append((
            codigo_usuario, data_cadastro, nome, nome_completo, inativo,
            empresa, gerente_codigo, equipe_codigo
        ))

    if usuarios_para_processar:
        cursor.executemany(sql_insert_update_usuario, usuarios_para_processar)
        db.commit()

    if pagina >= int(data.get("paginas", 0)):
        break

    pagina += 1
    time.sleep(0.1)

ids_to_delete = local_user_ids - api_user_ids

if ids_to_delete:
    print(f"\nEncontrados {len(ids_to_delete)} usuários para excluir.")
    
    delete_tuples = [(user_id,) for user_id in ids_to_delete]
    
    sql_delete = "DELETE FROM usuario WHERE codigo = %s"
    
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    cursor.executemany(sql_delete, delete_tuples)
    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
    db.commit()
    print("Usuários antigos foram excluídos com sucesso.")
else:
    print("\nNenhum usuário para excluir.")

cursor.close()
db.close()
print("-" * 50)
print("Sincronização de usuários concluída.")
print(f"Tempo total de execução: {time.time() - start_time:.2f} segundos.")