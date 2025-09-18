import mysql

class Util:
    def __init__(self):
        pass
    
    # Usuários, Proprietários, Negócios, Imóveis, Imóveis Detalhes, Clientes [0, 1, 2, 3, 4, 5]
    @staticmethod
    def get_endpoints(indice):

        endpoints = ["http://cli15060-rest.vistahost.com.br/usuarios/listar",
                      "http://cli15060-rest.vistahost.com.br/proprietarios/listar",
                        "http://cli15060-rest.vistahost.com.br/negocios/listar",
                          "http://cli15060-rest.vistahost.com.br/imoveis/listar",
                           "http://cli15060-rest.vistahost.com.br/imoveis/detalhes",
                            "http://cli15060-rest.vistahost.com.br/clientes/listar"
                        ]
        
        if indice not in range(len(endpoints)):
            print("Indíce inválido. Escolha um índice de 0 até " + len(endpoints) - 1)
            return None
        
        return endpoints[indice]

    @staticmethod
    def get_api_key():
        api_key = None
        with open('update/models/utils/key.txt', 'r') as arq:
            api_key = arq.readline()
        return api_key

    @staticmethod
    def get_db_connection():
        return mysql.connector.connect(
            host="193.203.175.220",
            port="3306",
            user="u305840337_admin",
            password="#TbhL^J87",
            database="u305840337_oa"
        )
    
    @staticmethod
    def checa_campo(campo):
        if campo is None:
            return None

        if isinstance(campo, str):
            campo = campo.strip("")

        if campo == "" or campo == "0":
            return None

        elif campo == 0:
            return None
        
        return campo
    
    @staticmethod
    def trata_data(data_final):
        if data_final in ("0000-00-00 00:00:00", "0000-00-00", "", None):
            data_final = None

        return data_final