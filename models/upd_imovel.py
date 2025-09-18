import requests
import mysql.connector
import json
import time
from utils.util import Util

def contar_fotos(codigo, api_key):
    """Faz uma chamada de API separada para contar as fotos de um imóvel."""
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    url = (
        "http://cli15060-rest.vistahost.com.br/imoveis/detalhes"
        f"?key={api_key}"
        f"&imovel={codigo}"
        "&pesquisa={\"fields\":[{\"Foto\": [\"Ordem\"]}]}"
    )
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        resultado = resp.json()

        foto_raw = resultado.get("Foto")
        if not foto_raw or not isinstance(foto_raw, dict):
            return 0
        
        contador = sum(1 for v in foto_raw.values() if v is not None)
        print(f"Imóvel {codigo} tem {contador} fotos.")
        return contador
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar fotos para o imóvel {codigo}: {e}")
        return 0
    except json.JSONDecodeError:
        print(f"Erro ao decodificar JSON das fotos do imóvel {codigo}.")
        return 0

db = Util.get_db_connection()
cursor = db.cursor()

data_inicio = input("Digite a data inicial para a atualização de imóveis (aaaa-mm-dd): ")
data_fim = input("Digite a data final para a atualização de imóveis (aaaa-mm-dd): ")

url = Util.get_endpoints(3)
api_key = Util.get_api_key()
headers = {"Accept": "application/json"}
quantidade = 25
pagina = 1

sql_insert_update = """
    INSERT INTO imovel (
        codigo, agenciador, valor_venda, data_cadastro, status, lancamento, 
        empreendimento, dormitorios, area_privativa, latitude, longitude, vagas, 
        incorporadora, construtora, area_total, cidade, bairro, data_entrega, 
        ano_construcao, categoria, data_atualizacao, suites, total_banheiros, 
        valor_condominio, valor_iptu, imovel_dwv, quantidade_fotos
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        agenciador = VALUES(agenciador), valor_venda = VALUES(valor_venda), 
        data_cadastro = VALUES(data_cadastro), status = VALUES(status), 
        lancamento = VALUES(lancamento), empreendimento = VALUES(empreendimento), 
        dormitorios = VALUES(dormitorios), area_privativa = VALUES(area_privativa), 
        latitude = VALUES(latitude), longitude = VALUES(longitude), vagas = VALUES(vagas), 
        incorporadora = VALUES(incorporadora), construtora = VALUES(construtora), 
        area_total = VALUES(area_total), cidade = VALUES(cidade), bairro = VALUES(bairro), 
        data_entrega = VALUES(data_entrega), ano_construcao = VALUES(ano_construcao), 
        categoria = VALUES(categoria), data_atualizacao = VALUES(data_atualizacao), 
        suites = VALUES(suites), total_banheiros = VALUES(total_banheiros), 
        valor_condominio = VALUES(valor_condominio), valor_iptu = VALUES(valor_iptu), 
        imovel_dwv = VALUES(imovel_dwv), quantidade_fotos = VALUES(quantidade_fotos)
"""

start_time = time.time()
print(f"Buscando imóveis atualizados entre {data_inicio} e {data_fim}...")

while True:
    params = {
        "key": api_key,
        "pesquisa": json.dumps({
            "fields": [
                "Codigo", "Empreendimento", "Incorporadora", "Construtora",
                "ValorVenda", "AreaPrivativa", "AreaTotal", "Cidade", "Bairro", "Latitude",
                "Longitude", "Lancamento", "DataEntrega", "AnoConstrucao", "Categoria",
                "Status", "DataCadastro", "DataAtualizacao", "Agenciador", "ImovelDWV",
                "Dormitorios", "Suites", "Vagas", "TotalBanheiros", "ValorCondominio",
                "ValorIptu"
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
        print("Nenhum imóvel encontrado no período ou fim da paginação.")
        break
        
    print(f"Processando página {pagina}/{data.get('paginas')}...")
    
    imoveis_para_processar = []
    for k, imovel in data.items():
        if not k.isdigit():
            continue

        codigo = Util.checa_campo(imovel.get("Codigo"))
        quantidade_fotos = contar_fotos(codigo=codigo, api_key=api_key)

        imoveis_para_processar.append((
            codigo,
            Util.checa_campo(imovel.get("Agenciador")),
            Util.checa_campo(imovel.get("ValorVenda")),
            Util.trata_data(imovel.get("DataCadastro")),
            Util.checa_campo(imovel.get("Status")),
            Util.checa_campo(imovel.get("Lancamento")),
            Util.checa_campo(imovel.get("Empreendimento")),
            Util.checa_campo(imovel.get("Dormitorios")),
            Util.checa_campo(imovel.get("AreaPrivativa")),
            Util.checa_campo(imovel.get("Latitude")),
            Util.checa_campo(imovel.get("Longitude")),
            Util.checa_campo(imovel.get("Vagas")),
            Util.checa_campo(imovel.get("Incorporadora")),
            Util.checa_campo(imovel.get("Construtora")),
            Util.checa_campo(imovel.get("AreaTotal")),
            Util.checa_campo(imovel.get("Cidade")),
            Util.checa_campo(imovel.get("Bairro")),
            Util.trata_data(imovel.get("DataEntrega")),
            Util.checa_campo(imovel.get("AnoConstrucao")),
            Util.checa_campo(imovel.get("Categoria")),
            Util.trata_data(imovel.get("DataAtualizacao")),
            Util.checa_campo(imovel.get("Suites")),
            Util.checa_campo(imovel.get("TotalBanheiros")),
            Util.checa_campo(imovel.get("ValorCondominio")),
            Util.checa_campo(imovel.get("ValorIptu")),
            Util.checa_campo(imovel.get("ImovelDWV")),
            quantidade_fotos
        ))

    if imoveis_para_processar:
        cursor.executemany(sql_insert_update, imoveis_para_processar)
        db.commit()
        print(f"Página {pagina} processada. {len(imoveis_para_processar)} imóveis inseridos/atualizados.")

    if pagina >= int(data.get("paginas", 0)):
        break

    pagina += 1
    time.sleep(0.2)

cursor.close()
db.close()
print("-" * 50)
print(f"Atualização de imóveis do período de {data_inicio} a {data_fim} concluída.")
print(f"Tempo total de execução: {time.time() - start_time:.2f} segundos.")