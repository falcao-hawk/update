import subprocess
import sys
import time
from datetime import datetime, timedelta

def run_update_script(script_path, date_input_str=None):
 
    print(f"Iniciando execução de: {script_path}")
    
    try:
        run_args = {
            "check": True,
            "text": True,
            "encoding": 'utf-8'
        }
        if date_input_str:
            run_args["input"] = date_input_str

        subprocess.run([sys.executable, script_path], **run_args)
        
        print(f"✅ Execução de {script_path} concluída com sucesso.\n")
        return True
    except FileNotFoundError:
        print(f"❌ ERRO: O arquivo de script não foi encontrado: {script_path}\n")
        return False
    except subprocess.CalledProcessError:
        print(f"❌ ERRO: O script {script_path} falhou durante a execução.\n")
        return False
    except Exception as e:
        print(f"❌ Ocorreu um erro inesperado ao executar {script_path}: {e}\n")
        return False

if __name__ == '__main__':
    print("======================================================")
    print(f"INICIANDO ROTINA DE ATUALIZAÇÃO DO BANCO DE DADOS")
    print(f"Data e Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("======================================================")
    
    data_fim = datetime.now().date()
    data_inicio = data_fim - timedelta(days=7)

    print(f"Data inicial calculada: {data_inicio.strftime('%Y-%m-%d')}")
    print(f"Data final calculada:   {data_fim.strftime('%Y-%m-%d')}")
    print("-" * 50)
    
    date_input_for_scripts = f"{data_inicio.strftime('%Y-%m-%d')}\n{data_fim.strftime('%Y-%m-%d')}\n"

    start_time = time.time()

    full_sync_scripts = [
        "models/upd_gerente.py",
        "models/upd_usuarios.py",
        "models/upd_proprietario.py",
        "models/upd_negocio.py",
    ]

    date_based_scripts = [
        "models/upd_clientes.py",
        "models/upd_imovel.py",
        "models/upd_usuario_has_cliente.py",
        "models/upd_historico.py",
        "models/upd_prontuario.py"
    ]
    
    execution_order = [
        "models/upd_gerente.py",
        "models/upd_clientes.py",
        "models/upd_usuarios.py",
        "models/upd_imovel.py",
        "models/upd_proprietario.py",
        "models/upd_usuario_has_cliente.py",
        "models/upd_negocio.py",
        "models/upd_historico.py",
        "models/upd_prontuario.py"
    ]

    for script in execution_order:
        if script in date_based_scripts:
            success = run_update_script(script, date_input_str=date_input_for_scripts)
        else:
            success = run_update_script(script)
        
        if not success:
            print("======================================================")
            print("🛑 PROCESSO DE ATUALIZAÇÃO INTERROMPIDO DEVIDO A UM ERRO.")
            print("======================================================")
            sys.exit(1)

    end_time = time.time()
    total_time = end_time - start_time
    
    print("======================================================")
    print("🎉 TODOS OS SCRIPTS FORAM EXECUTADOS COM SUCESSO!")
    print(f"Tempo total de execução: {total_time:.2f} segundos.")
    print("======================================================")
