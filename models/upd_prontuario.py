import requests
import mysql.connector
import json
import time
from utils.util import Util

db = Util.get_db_connection()
cursor = db.cursor()

data_inicio = input("Digite a data inicial para a busca de imóveis atualizados (aaaa-mm-dd): ")
data_fim = input("Digite a data final para a busca de imóveis atualizados (aaaa-mm-dd): ")

url_imovel_lista = Util.get_endpoints(3) # /imoveis/listar
url_imovel_detalhes = Util.get_endpoints(4) # /imoveis/detalhes
api_key = Util.get_api_key()
headers = {"Accept": "application/json"}
quantidade_imoveis = 50
pagina = 1

start_time = time.time()
print(f"Buscando imóveis atualizados entre {data_inicio} e {data_fim} para sincronizar prontuários...")

imoveis_atualizados_codigos = []

# --- PARTE 1: Coletar todos os imóveis que foram atualizados no período ---
while True:
    params_lista = {
        "key": api_key,
        "pesquisa": json.dumps({
            "fields": ["Codigo"],
            "paginacao": {"pagina": str(pagina), "quantidade": str(quantidade_imoveis)},
            "filter": {
                "DataAtualizacao": [data_inicio, data_fim]
            }
        }),
        "showtotal": "1"
    }

    r_lista = requests.get(url_imovel_lista, params=params_lista, headers=headers)
    data_lista = r_lista.json()

    if "total" not in data_lista or not any(k.isdigit() for k in data_lista.keys()):
        break

    for k, imovel in data_lista.items():
        if k.isdigit() and imovel.get("Codigo"):
            imoveis_atualizados_codigos.append(imovel["Codigo"])
    
    if pagina >= int(data_lista.get('paginas', 0)):
        break
    pagina += 1

print(f"Encontrados {len(imoveis_atualizados_codigos)} imóveis atualizados. Sincronizando prontuários...")

# --- PARTE 2: Sincronizar os prontuários para os imóveis encontrados ---
if imoveis_atualizados_codigos:
    # 2a. Apaga todos os prontuários antigos para os imóveis que serão atualizados
    format_strings = ','.join(['%s'] * len(imoveis_atualizados_codigos))
    sql_delete = f"DELETE FROM prontuario WHERE imovel_codigo IN ({format_strings})"
    cursor.execute(sql_delete, tuple(imoveis_atualizados_codigos))
    db.commit()
    print(f"{cursor.rowcount} prontuários antigos foram removidos.")

    novos_prontuarios = []
    # 2b. Busca os detalhes de cada imóvel atualizado para pegar os novos prontuários
    for codigo_imovel in imoveis_atualizados_codigos:
        params_detalhes = {
            "key": api_key,
            "imovel": codigo_imovel,
            "pesquisa": json.dumps({
                "fields": [{"prontuarios": ["Cliente", "PROPOSTA", "ValorProposta", "CodigoCorretor", "Data", "Hora"]}]
            })
        }
        try:
            r_detalhes = requests.get(url_imovel_detalhes, params=params_detalhes, headers=headers)
            r_detalhes.raise_for_status()
            data_detalhes = r_detalhes.json()
            
            prontuarios_api = data_detalhes.get("prontuarios", {})
            if isinstance(prontuarios_api, dict):
                for prontuario_id, prontuario_info in prontuarios_api.items():
                    novos_prontuarios.append((
                        Util.checa_campo(prontuario_info.get("PROPOSTA")),
                        Util.checa_campo(prontuario_info.get("ValorProposta")),
                        Util.trata_data(prontuario_info.get("Data")),
                        Util.checa_campo(prontuario_info.get("Hora")),
                        Util.checa_campo(prontuario_info.get("Cliente")),
                        Util.checa_campo(prontuario_info.get("CodigoCorretor")),
                        codigo_imovel
                    ))
        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar detalhes do imóvel {codigo_imovel}: {e}")
            continue
        
        time.sleep(0.1)

    if novos_prontuarios:
        sql_insert = """
            INSERT IGNORE INTO prontuario 
            (proposta, valor_proposta, data, hora, cliente_codigo, usuario_codigo, imovel_codigo)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(sql_insert, novos_prontuarios)
        db.commit()
        print(f"{cursor.rowcount} novos prontuários foram inseridos.")

cursor.close()
db.close()
print("-" * 50)
print(f"Sincronização de prontuários concluída.")
print(f"Tempo total de execução: {time.time() - start_time:.2f} segundos.")