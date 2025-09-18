import html
import json
import time
import math
import logging
from typing import Optional, List, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import mysql.connector
from bs4 import BeautifulSoup
from utils.util import Util

sql_insert = """
    INSERT INTO cliente (
        codigo, nome, fone_principal, celular, email_residencial,
        profissao, veiculo_captacao, data_nascimento, sexo, endereco,
        bairro_residencial, cidade_residencial, uf_residencial, cep_residencial,
        estado_civil, potencial_compra, data_atualizacao, status, data_cadastro
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        nome = VALUES(nome),
        fone_principal = VALUES(fone_principal),
        celular = VALUES(celular),
        email_residencial = VALUES(email_residencial),
        profissao = VALUES(profissao),
        veiculo_captacao = VALUES(veiculo_captacao),
        data_nascimento = VALUES(data_nascimento),
        sexo = VALUES(sexo),
        endereco = VALUES(endereco),
        bairro_residencial = VALUES(bairro_residencial),
        cidade_residencial = VALUES(cidade_residencial),
        uf_residencial = VALUES(uf_residencial),
        cep_residencial = VALUES(cep_residencial),
        estado_civil = VALUES(estado_civil),
        potencial_compra = VALUES(potencial_compra),
        data_atualizacao = VALUES(data_atualizacao),
        status = VALUES(status),
        data_cadastro = VALUES(data_cadastro)
"""

def trata_endereco(cliente: dict) -> Optional[str]:
    """Normaliza e concatena o endereço retornado pela API.

    Retorna None se não houver informação útil.
    """
    endereco_residencial = cliente.get("EnderecoResidencial")
    endereco_numero = cliente.get("EnderecoNumero")
    endereco_tipo = cliente.get("EnderecoTipo")

    parts: List[str] = []

    if endereco_tipo and endereco_tipo.strip():
        parts.append(endereco_tipo.strip())
    if endereco_residencial and endereco_residencial.strip():
        parts.append(endereco_residencial.strip())
    if endereco_numero and endereco_numero.strip() and endereco_numero != "0":
        parts.append(endereco_numero.strip())

    endereco_completo = " ".join(parts).strip()
    return endereco_completo if endereco_completo else None

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def _create_session(retries: int = 3, backoff_factor: float = 0.3) -> requests.Session:
    session = requests.Session()
    retry = Retry(total=retries, backoff_factor=backoff_factor, status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET", "POST"))
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def run():
    db = Util.get_db_connection()
    cursor = db.cursor()

    data_inicio = input("Digite a data inicial (aaaa-mm-dd): ")
    data_fim = input("Digite a data final (aaaa-mm-dd): ")

    url = Util.get_endpoints(5)  # clientes/listar
    api_key = Util.get_api_key()
    headers = {"Accept": "application/json"}
    quantidade = 50
    pagina = 1

    start = time.time()
    session = _create_session()

    try:
        while True:
            params = {
                "key": api_key,
                "pesquisa": json.dumps({
                    "fields": [
                        "Codigo", "Nome", "FonePrincipal", "Celular",
                        "EmailResidencial", "Profissao", "VeiculoCaptacao",
                        "DataNascimento", "Sexo", "EnderecoResidencial",
                        "EstadoCivil", "Potencial", "DataAtualizacao", "Status",
                        "EnderecoNumero", "EnderecoTipo",
                        "BairroResidencial", "CidadeResidencial", "UFResidencial", "CEPResidencial",
                        "DataCadastro"
                    ],
                    "paginacao": {"pagina": str(pagina), "quantidade": str(quantidade)},
                    "filter": {
                        "DataAtualizacao": [data_inicio, data_fim]  # <-- intervalo de datas
                    }
                }),
                "showtotal": "1"
            }

            logger.info("Solicitando página %s", pagina)
            try:
                r = session.get(url, params=params, headers=headers, timeout=30)
                r.raise_for_status()
            except requests.RequestException as exc:
                logger.exception("Erro na requisição da API: %s", exc)
                break

            try:
                data = r.json()
            except ValueError:
                logger.exception("Resposta inválida JSON na página %s", pagina)
                break

            total_pages = int(data.get("paginas", 0)) if data.get("paginas") else 0
            registros = [(k, v) for k, v in data.items() if k.isdigit()]

            if not registros:
                logger.info("Nenhum dado retornado na página %s, finalizando.", pagina)
                break

            logger.info("Página %s/%s - preparando batch com %s registros...", pagina, total_pages or "?", len(registros))

            valores: List[Tuple] = []
            for k, cliente in registros:
                codigo = Util.checa_campo(cliente.get("Codigo"))
                if codigo is None or str(codigo).strip() == "":
                    continue

                try:
                    codigo_int = int(codigo)
                except (TypeError, ValueError):
                    logger.warning("Codigo inválido para registro %s: %r", k, codigo)
                    continue

                nome = Util.checa_campo(cliente.get("Nome"))
                fone_principal = Util.checa_campo(cliente.get("FonePrincipal"))
                celular = Util.checa_campo(cliente.get("Celular"))
                email_residencial = Util.checa_campo(cliente.get("EmailResidencial"))
                profissao = Util.checa_campo(cliente.get("Profissao"))
                veiculo_captacao = Util.checa_campo(cliente.get("VeiculoCaptacao"))
                sexo = Util.checa_campo(cliente.get("Sexo"))
                endereco_completo = trata_endereco(cliente)
                bairro_residencial = Util.checa_campo(cliente.get("BairroResidencial"))
                cidade_residencial = Util.checa_campo(cliente.get("CidadeResidencial"))
                uf_residencial = Util.checa_campo(cliente.get("UFResidencial"))
                cep_residencial = Util.checa_campo(cliente.get("CEPResidencial"))
                estado_civil = Util.checa_campo(cliente.get("EstadoCivil"))
                potencial_compra = Util.checa_campo(cliente.get("Potencial"))
                data_atualizacao_cliente = Util.trata_data(cliente.get("DataAtualizacao"))
                status = Util.checa_campo(cliente.get("Status"))
                data_nascimento = Util.trata_data(cliente.get("DataNascimento"))
                data_cadastro = Util.trata_data(cliente.get("DataCadastro"))

                valores.append((
                    codigo_int,
                    nome,
                    fone_principal,
                    celular,
                    email_residencial,
                    profissao,
                    veiculo_captacao,
                    data_nascimento,
                    sexo,
                    endereco_completo,
                    bairro_residencial,
                    cidade_residencial,
                    uf_residencial,
                    cep_residencial,
                    estado_civil,
                    potencial_compra,
                    data_atualizacao_cliente,
                    status,
                    data_cadastro
                ))

            if not valores:
                logger.info("Nenhum valor válido para inserir na página %s", pagina)
            else:
                chunk_size = 1000
                for i in range(0, len(valores), chunk_size):
                    chunk = valores[i:i + chunk_size]
                    try:
                        cursor.executemany(sql_insert, chunk)
                        db.commit()
                        logger.info("Inseridos %s registros (chunk %s..%s)", len(chunk), i + 1, i + len(chunk))
                    except mysql.connector.Error:
                        db.rollback()
                        logger.exception("Erro ao inserir chunk, realizando rollback")

            if total_pages and pagina >= total_pages:
                break

            pagina += 1
            time.sleep(0.5)

    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            db.close()
        except Exception:
            pass

    logger.info("Clientes do intervalo %s a %s inseridos/atualizados com sucesso.", data_inicio, data_fim)
    logger.info("Tempo total: %.2f segundos", time.time() - start)


if __name__ == "__main__":
    run()
