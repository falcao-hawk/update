import requests
import mysql.connector
import json
import time
from utils.util import Util

db = Util.get_db_connection()
cursor = db.cursor()

data_inicio = input("Digite a data inicial da atualização dos clientes (aaaa-mm-dd): ")
data_fim = input("Digite a data final da atualização dos clientes (aaaa-mm-dd): ")

url = Util.get_endpoints(5) # /clientes/listar
api_key = Util.get_api_key()
headers = {"Accept": "application/json"}
quantidade = 50
pagina = 1

start_time = time.time()
print(f"Buscando clientes atualizados entre {data_inicio} e {data_fim} para sincronizar vínculos...")

while True:
    params = {
        "key": api_key,
        "pesquisa": json.dumps({
            "fields": [
                "Codigo", 
                {"CorretorCliente" : ["Codigo"]},
                "DataAtualizacao"
            ],
            "paginacao": {"pagina": str(pagina), "quantidade": str(quantidade)},
            "filter": {
                "DataAtualizacao": [data_inicio, data_fim]
            }
        }),
        "showtotal": "1", 
        "showSuspended" : "1"
    }

    r = requests.get(url, params=params, headers=headers)
    data = r.json()

    if "total" not in data or not any(k.isdigit() for k in data.keys()):
        print("Nenhum cliente atualizado no período ou fim da paginação.")
        break

    print(f"Processando página {pagina}/{data.get('paginas')}...")

    clientes_para_sincronizar = []
    novos_vinculos = []

    for k, cliente in data.items():
        if not k.isdigit():
            continue

        cliente_codigo = Util.checa_campo(cliente.get("Codigo"))
        if not cliente_codigo:
            continue
        
        clientes_para_sincronizar.append(cliente_codigo)

        corretores = cliente.get("CorretorCliente", {})
        if isinstance(corretores, dict):
            for k_corretor, corretor in corretores.items():
                corretor_codigo = corretor.get("Codigo")
                if corretor_codigo:
                    novos_vinculos.append((corretor_codigo, cliente_codigo))

    if clientes_para_sincronizar:
        # 1. APAGA todos os vínculos antigos para os clientes desta página de uma só vez
        # O formato '%s' precisa ser gerado dinamicamente para a cláusula IN
        format_strings = ','.join(['%s'] * len(clientes_para_sincronizar))
        sql_delete = f"DELETE FROM usuario_has_cliente WHERE cliente_codigo IN ({format_strings})"
        cursor.execute(sql_delete, tuple(clientes_para_sincronizar))
        print(f"  - {cursor.rowcount} vínculos antigos removidos para {len(clientes_para_sincronizar)} clientes.")

    if novos_vinculos:
        # 2. INSERE todos os novos vínculos para os clientes desta página de uma só vez
        sql_insert = """
            INSERT INTO usuario_has_cliente (usuario_codigo, cliente_codigo)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE usuario_codigo=VALUES(usuario_codigo)
        """
        cursor.executemany(sql_insert, novos_vinculos)
        print(f"  - {cursor.rowcount} novos vínculos inseridos.")

    db.commit()

    if pagina >= int(data.get("paginas", 0)):
        break

    pagina += 1
    time.sleep(0.2)

cursor.close()
db.close()
print("-" * 50)
print(f"Sincronização de vínculos concluída.")
print(f"Tempo total: {time.time() - start_time:.2f} segundos.")