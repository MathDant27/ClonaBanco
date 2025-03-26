import jaydebeapi
import jpype
import jpype.imports
from jpype.types import *
import os
import logging
import hashlib
from datetime import datetime

# Diretório para salvar os logs
log_directory = "DIRETORIO_LOGS"
os.makedirs(log_directory, exist_ok=True)

# Data atual para o nome do arquivo (formato brasileiro: dia-mês-ano)
current_date = datetime.now().strftime("%d-%m-%Y")
log_filename = os.path.join(log_directory, f'Log_{current_date}.txt')

# Configuração de logging - diretamente para o arquivo de relatório
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuração do JVM e driver JDBC
jvm_path = "CAMINHO_JVM"
jdbc_driver_path = "CAMINHO_DRIVER_JDBC"
bson_driver_path = "CAMINHO_DRIVER_BSON"
driver_class = "com.informix.jdbc.IfxDriver"

import warnings
warnings.filterwarnings("ignore", message="No type mapping for JDBC type")

if not jpype.isJVMStarted():
    jpype.startJVM(jvm_path, f"-Djava.class.path={jdbc_driver_path}{os.pathsep}{bson_driver_path}", convertStrings=True)

# Parâmetros de conexão do servidor de produção
prod_config = {
    'servidor': "IP_SERVIDOR_PROD", 'porta': "PORTA_PROD", 'banco': "NOME_BANCO_PROD",
    'usuario': "USUARIO_PROD", 'senha': "SENHA_PROD", 'informixserver': "SERVIDOR_PROD"
}

# Parâmetros de conexão do servidor de homologação
homolog_config = {
    'servidor': "IP_SERVIDOR_HOMOLOG", 'porta': "PORTA_HOMOLOG", 'banco': "NOME_BANCO_HOMOLOG",
    'usuario': "USUARIO_HOMOLOG", 'senha': "SENHA_HOMOLOG", 'informixserver': "SERVIDOR_HOMOLOG"
}

# Lista de tabelas para clonar - adicione ou remova tabelas conforme necessário
TABELAS_PARA_CLONAR = ['TABELA1', 'TABELA2', 'TABELA3']

# Estatísticas para o relatório final
estatisticas = {
    'inicio': datetime.now(), 'fim': None, 'tabelas_processadas': 0,
    'tabelas_com_erro': 0, 'registros_copiados': 0, 'registros_ignorados': 0, 'erros': []
}

def criar_url_conexao(config):
    return f"jdbc:informix-sqli://{config['servidor']}:{config['porta']}/{config['banco']}:INFORMIXSERVER={config['informixserver']}"

def conectar_db(config):
    """Estabelece conexão com o banco de dados Informix"""
    jdbc_url = criar_url_conexao(config)
    logging.info(f"Conectando ao servidor: {config['servidor']}")
    try:
        conn = jaydebeapi.connect(driver_class, jdbc_url, [config['usuario'], config['senha']], jdbc_driver_path)
        logging.info(f"Conexão estabelecida com sucesso ao servidor {config['servidor']}")
        return conn
    except Exception as e:
        logging.error(f"Erro ao conectar ao servidor {config['servidor']}: {str(e)}")
        estatisticas['erros'].append(f"Erro ao conectar ao servidor {config['servidor']}: {str(e)}")
        raise

def obter_tabelas_para_clonar(conn_prod, conn_homolog):
    """Verifica quais tabelas da lista existem em ambos os ambientes"""
    cursor_prod = conn_prod.cursor()
    cursor_homolog = conn_homolog.cursor()
    
    try:
        # Verifica quais tabelas da lista existem na produção
        placeholders = ','.join([f"'{tabela}'" for tabela in TABELAS_PARA_CLONAR])
        cursor_prod.execute(f"SELECT tabname FROM systables WHERE tabtype = 'T' AND owner = 'informix' AND tabname IN ({placeholders})")
        tabelas_prod = [row[0] for row in cursor_prod.fetchall()]
        
        if not tabelas_prod:
            logging.warning("Nenhuma das tabelas especificadas foi encontrada no ambiente de produção!")
            return []
        
        # Verifica quais dessas tabelas existem na homologação
        placeholders = ','.join([f"'{tabela}'" for tabela in tabelas_prod])
        cursor_homolog.execute(f"SELECT tabname FROM systables WHERE tabtype = 'T' AND owner = 'informix' AND tabname IN ({placeholders})")
        tabelas_homolog = [row[0] for row in cursor_homolog.fetchall()]
        
        return tabelas_homolog
    except Exception as e:
        logging.error(f"Erro ao obter tabelas para clonar: {str(e)}")
        estatisticas['erros'].append(f"Erro ao obter tabelas para clonar: {str(e)}")
        raise
    finally:
        cursor_prod.close()
        cursor_homolog.close()

def gerar_hash_registro(registro):
    """Gera um hash único para um registro baseado em seus valores"""
    valores_str = '|'.join(str(valor if valor is not None else 'NULL') for valor in registro)
    return hashlib.md5(valores_str.encode()).hexdigest()

def obter_ultimos_registros(conn, tabela, limite=100):
    """Obtém os últimos registros da tabela especificada"""
    cursor = conn.cursor()
    try:
        # Conta o total de registros na tabela
        cursor.execute(f"SELECT COUNT(*) FROM {tabela}")
        total = cursor.fetchone()[0]
        
        # Calcula o offset para pegar os últimos registros
        offset = max(0, total - limite)
        
        # Obtém os registros usando SKIP/LIMIT
        cursor.execute(f"SELECT * FROM {tabela} SKIP {offset} LIMIT {limite}")
        dados = cursor.fetchall()
        colunas = [desc[0] for desc in cursor.description]
        
        logging.info(f"Obtidos {len(dados)} registros da tabela {tabela}")
        return dados, colunas
    except Exception as e:
        logging.error(f"Erro ao obter registros da tabela {tabela}: {str(e)}")
        estatisticas['erros'].append(f"Erro ao obter registros da tabela {tabela}: {str(e)}")
        return [], []
    finally:
        cursor.close()

def verificar_registros_existentes(conn, tabela, dados, colunas):
    """Verifica quais registros já existem na tabela de destino para evitar duplicações"""
    if not dados:
        return []
    
    cursor = conn.cursor()
    try:
        # Obtém todos os registros da tabela de destino
        cursor.execute(f"SELECT * FROM {tabela}")
        registros_destino = cursor.fetchall()
        
        # Gera hashes para os registros da origem e destino
        hashes_origem = [gerar_hash_registro(registro) for registro in dados]
        hashes_destino = [gerar_hash_registro(registro) for registro in registros_destino]
        
        # Filtra apenas os registros que não existem no destino
        novos_dados = [dados[i] for i, hash_valor in enumerate(hashes_origem) if hash_valor not in hashes_destino]
        
        estatisticas['registros_ignorados'] += (len(dados) - len(novos_dados))
        return novos_dados
    except Exception as e:
        logging.error(f"Erro ao verificar registros existentes na tabela {tabela}: {str(e)}")
        estatisticas['erros'].append(f"Erro ao verificar registros existentes na tabela {tabela}: {str(e)}")
        return dados  # Em caso de erro, tenta inserir todos
    finally:
        cursor.close()

def inserir_dados(conn, tabela, dados, colunas):
    """Insere os dados na tabela de destino usando transações"""
    if not dados:
        logging.info(f"Nenhum novo registro para inserir na tabela {tabela}")
        return 0
    
    cursor = conn.cursor()
    try:
        conn.jconn.setAutoCommit(False)
        cursor.execute("BEGIN WORK")
        
        # Prepara a instrução de inserção
        placeholders = ','.join(['?'] * len(colunas))
        insert_sql = f"INSERT INTO {tabela} ({','.join(colunas)}) VALUES ({placeholders})"
        
        # Insere cada registro individualmente para melhor controle de erros
        registros_inseridos = 0
        for registro in dados:
            try:
                cursor.execute(insert_sql, registro)
                registros_inseridos += 1
            except Exception as e:
                logging.warning(f"Erro ao inserir um registro: {str(e)}")
                # Continua com o próximo registro
        
        # Confirma a transação
        cursor.execute("COMMIT WORK")
        
        logging.info(f"Inseridos {registros_inseridos} registros na tabela {tabela}")
        estatisticas['registros_copiados'] += registros_inseridos
        return registros_inseridos
    except Exception as e:
        # Em caso de erro, faz rollback da transação
        try:
            cursor.execute("ROLLBACK WORK")
        except:
            pass
        logging.error(f"Erro ao inserir dados na tabela {tabela}: {str(e)}")
        estatisticas['erros'].append(f"Erro ao inserir dados na tabela {tabela}: {str(e)}")
        estatisticas['tabelas_com_erro'] += 1
        return 0
    finally:
        try:
            conn.jconn.setAutoCommit(True)
        except:
            pass
        cursor.close()

def copiar_tabela(tabela, conn_prod, conn_homolog):
    """Copia os últimos registros da tabela de produção para homologação"""
    try:
        print(f"Copiando tabela: {tabela}...")
        
        # Obtém os últimos registros da tabela de origem
        dados, colunas = obter_ultimos_registros(conn_prod, tabela, 100)
        
        if not dados:
            logging.info(f"Nenhum dado encontrado na tabela {tabela}")
            return 0
        
        # Verifica quais registros já existem no destino
        novos_dados = verificar_registros_existentes(conn_homolog, tabela, dados, colunas)
        
        # Insere os novos dados na tabela de destino
        registros_inseridos = inserir_dados(conn_homolog, tabela, novos_dados, colunas)
        
        print(f"  - {registros_inseridos} registros inseridos")
        return registros_inseridos
    except Exception as e:
        logging.error(f"Erro ao processar tabela {tabela}: {str(e)}")
        estatisticas['erros'].append(f"Erro ao processar tabela {tabela}: {str(e)}")
        estatisticas['tabelas_com_erro'] += 1
        print(f"  - Erro: {str(e)}")
        return 0

def gerar_relatorio():
    """Gera o relatório final com estatísticas da operação"""
    estatisticas['fim'] = datetime.now()
    duracao = estatisticas['fim'] - estatisticas['inicio']
    
    relatorio = f"""
=== RELATÓRIO DE CÓPIA DE DADOS ===
Data e hora de início: {estatisticas['inicio'].strftime('%Y-%m-%d %H:%M:%S')}
Data e hora de término: {estatisticas['fim'].strftime('%Y-%m-%d %H:%M:%S')}
Duração total: {duracao}

Tabelas processadas: {estatisticas['tabelas_processadas']}
Tabelas com erro: {estatisticas['tabelas_com_erro']}
Registros copiados: {estatisticas['registros_copiados']}
Registros ignorados (já existentes): {estatisticas['registros_ignorados']}

Erros encontrados: {len(estatisticas['erros'])}
"""
    
    if estatisticas['erros']:
        relatorio += "\nDetalhes dos erros:\n"
        for i, erro in enumerate(estatisticas['erros'], 1):
            relatorio += f"{i}. {erro}\n"
    
    logging.info(relatorio)
    
    print("\nResumo da operação:")
    print(f"Tabelas processadas: {estatisticas['tabelas_processadas']}")
    print(f"Registros copiados: {estatisticas['registros_copiados']}")
    print(f"Erros encontrados: {len(estatisticas['erros'])}")
    print(f"Relatório completo salvo em: {log_filename}")

def main():
    print(f"Iniciando cópia de dados - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Tabelas selecionadas: {', '.join(TABELAS_PARA_CLONAR)}")
    
    try:
        # Conecta aos bancos de dados de produção e homologação
        conn_prod = conectar_db(prod_config)
        conn_homolog = conectar_db(homolog_config)
        
        # Obtém as tabelas que existem em ambos os ambientes
        tabelas_para_clonar = obter_tabelas_para_clonar(conn_prod, conn_homolog)
        estatisticas['tabelas_processadas'] = len(tabelas_para_clonar)
        
        if not tabelas_para_clonar:
            print("Nenhuma tabela disponível para clonar. Verifique os nomes das tabelas.")
        else:
            # Processa cada tabela encontrada
            for tabela in tabelas_para_clonar:
                copiar_tabela(tabela, conn_prod, conn_homolog)
            
        # Fecha as conexões com os bancos de dados
        conn_prod.close()
        conn_homolog.close()
        
    except Exception as e:
        estatisticas['erros'].append(f"Erro crítico: {str(e)}")
        print(f"Erro durante o processo: {str(e)}")
    finally:
        gerar_relatorio()

if __name__ == "__main__":
    main()
