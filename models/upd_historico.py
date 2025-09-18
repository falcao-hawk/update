
import html
import json
import time
import requests
import mysql.connector
from bs4 import BeautifulSoup
from utils.util import Util

def limpa_html(texto):
    """Método para tratar o texto que vem com tags HTML da API."""
    if not texto:
        return None

    soup = BeautifulSoup(texto, "html.parser")
    texto_limpo = soup.get_text(separator=" ")
    texto_limpo = html.unescape(texto_limpo)
    texto_limpo = " ".join(texto_limpo.split())
    return texto_limpo


db = Util.get_db_connection()
cursor = db.cursor()

data_inicio = input("Digite a data inicial da atualização dos clientes (aaaa-mm-dd): ")
data_fim = input("Digite a data final da atualização dos clientes (aaaa-mm-dd): ")

url = Util.get_endpoints(5)  # /clientes/listar
url_imoveis = Util.get_endpoints(3)  # /imoveis/listar
api_key = Util.get_api_key()
headers = {"Accept": "application/json"}
quantidade = 50
pagina = 1

start_time = time.time()
print(f"Buscando clientes atualizados entre {data_inicio} e {data_fim} para sincronizar históricos...")

while True:
    params = {
        "key": api_key,
        "pesquisa": json.dumps({
            "fields": [
                "Codigo",
                "DataAtualizacao",  # Importante para o filtro funcionar
                {"historicos": ["Assunto", "Texto", "Data", "Statusvisita", "Codigoimovel", "MotivoLost", "Hora"]}
            ],
            "paginacao": {"pagina": str(pagina), "quantidade": str(quantidade)},
            "filter": {
                "DataAtualizacao": [data_inicio, data_fim]
            }
        }),
        "showtotal": "1"
    }

    r = requests.get(url, params=params, headers=headers)
    data = r.json()

    if "total" not in data or not any(k.isdigit() for k in data.keys()):
        print("Nenhum cliente atualizado no período ou fim da paginação.")
        break

    print(f"Processando página {pagina}/{data.get('paginas')}...")

    clientes_para_sincronizar_ids = []
    novos_historicos = []

    for k, cliente in data.items():
        if not k.isdigit():
            continue

        cliente_codigo = Util.checa_campo(cliente.get("Codigo"))
        if not cliente_codigo:
            continue

        clientes_para_sincronizar_ids.append(cliente_codigo)

        historicos_api = cliente.get("historicos", {})
        if isinstance(historicos_api, dict):
            for k_hist, historico_info in historicos_api.items():
                novos_historicos.append((
                    k_hist,  # codigo do historico
                    Util.checa_campo(historico_info.get("Assunto")),
                    limpa_html(historico_info.get("Texto")),
                    Util.trata_data(historico_info.get("Data")),
                    Util.checa_campo(historico_info.get("Hora")),
                    Util.checa_campo(historico_info.get("Statusvisita")),
                    Util.checa_campo(historico_info.get("Codigoimovel")),
                    cliente_codigo,
                    Util.checa_campo(historico_info.get("MotivoLost"))
                ))

    if clientes_para_sincronizar_ids:
        # 1. APAGA os históricos antigos para os clientes desta página
        format_strings = ','.join(['%s'] * len(clientes_para_sincronizar_ids))
        sql_delete = f"DELETE FROM historico WHERE cliente_codigo IN ({format_strings})"
        cursor.execute(sql_delete, tuple(clientes_para_sincronizar_ids))
        print(f"  - {cursor.rowcount} históricos antigos removidos para {len(clientes_para_sincronizar_ids)} clientes.")

    if novos_historicos:
        # 2. INSERE os novos históricos para os clientes desta página
        sql_insert = """
            INSERT INTO historico (
                codigo, assunto, texto, data, hora, status_visita, imovel, cliente_codigo, motivo_perda
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                assunto = VALUES(assunto), texto = VALUES(texto), data = VALUES(data),
                hora = VALUES(hora), status_visita = VALUES(status_visita), imovel = VALUES(imovel),
                motivo_perda = VALUES(motivo_perda)
        """

        try:
            cursor.executemany(sql_insert, novos_historicos)
            print(f"  - {cursor.rowcount} novos históricos inseridos/atualizados.")
        except Exception as e:
            # The 'except' block is executed if `executemany` fails due to the foreign key constraint.
            print("⚠️ Deu erro no código do imóvel, tentando resolver pelo ImoCodigo...")

            novos_historicos_corrigidos = []
            for hist in novos_historicos:
                codigo_hist, assunto, texto, data_hist, hora, status_visita, imovel, cliente_codigo, motivo_perda = hist
                
                # Only try to correct the imovel code if one exists
                if imovel:
                    # consulta API de imóveis tentando resolver pelo ImoCodigo
                    params2 = {
                        "key": api_key,
                        "pesquisa": json.dumps({
                            "fields": ["Codigo"],
                            "paginacao": {"pagina": "1", "quantidade": "1"},
                            "filter": {
                                "ImoCodigo": [imovel]
                            }
                        }),
                        "showtotal": "1",
                        "showSuspended": "1"
                    }

                    r2 = requests.get(url_imoveis, params=params2, headers=headers)
                    data2 = r2.json()

                    codigo_real = None
                    # IMPORTANT FIX: Check if the response has a total greater than 0
                    if data2 and data2.get("total", 0) > 0:
                        for _, imovel_data in data2.items():
                            # Ensure the value is a dictionary before trying to use .get()
                            if isinstance(imovel_data, dict):
                                print("Codigo ImoCodigo: ", imovel)
                                codigo_real = imovel_data.get("Codigo")
                                print("Codigo verdadeiro: ", codigo_real)
                                break  # Pega o primeiro resultado válido

                    if codigo_real:
                        imovel = codigo_real  # substitui pelo Codigo correto
                    else:
                        # If no real code is found, set imovel to None to avoid foreign key errors
                        imovel = None

                novos_historicos_corrigidos.append((
                    codigo_hist, assunto, texto, data_hist, hora,
                    status_visita, imovel, cliente_codigo, motivo_perda
                ))

            # The code from here on is outside the for loop, so it's only executed once per page.
            try:
                cursor.execute("SET FOREIGN_KEY_CHECKS=0")
                cursor.executemany(sql_insert, novos_historicos_corrigidos)
                print(f"  - {cursor.rowcount} históricos inseridos/atualizados após correção.")
                cursor.execute("SET FOREIGN_KEY_CHECKS=1")
            except Exception as e:
                # Log the specific error if the corrected list still fails
                print(f"❌ Falha ao inserir históricos corrigidos. Erro: {e}")
                # You might want to break here or continue to the next page
                pass

    db.commit()

    if pagina >= int(data.get("paginas", 0)):
        break

    pagina += 1
    time.sleep(0.2)

cursor.close()
db.close()
print("-" * 50)
print(f"Sincronização de históricos concluída.")
print(f"Tempo total de execução: {time.time() - start_time:.2f} segundos.")