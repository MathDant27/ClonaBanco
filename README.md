# Projeto de Sincronização de Dados Informix

## Descrição
Este projeto é uma ferramenta de sincronização de dados entre bancos de dados Informix. Ele copia os registros mais recentes de tabelas específicas do banco de cópia para o banco de destino, evitando duplicações, e também permite a exclusão de registros antigos do banco de destino.

## Funcionalidades Principais
- Conexão com bancos de dados Informix usando JDBC
- Cópia seletiva de dados entre ambientes
- Verificação de registros existentes para evitar duplicações
- Exclusão de registros com mais de 6 meses no banco de destino (com confirmação via console)
- Geração de logs detalhados e relatórios de execução

## Tecnologias Utilizadas
- Python
- jaydebeapi (para conexão JDBC)
- jpype
- Informix JDBC Driver

## Configuração
1. Instale as dependências Python necessárias:
pip install jaydebeapi jpype1

2. Configure os caminhos do JVM e dos drivers JDBC no script.

3. Configure os parâmetros de conexão para os ambientes:
copia_config = {...}
destino_config = {...}

4. Defina as tabelas a serem sincronizadas:
TABELAS_PARA_CLONAR = ['TABELAS A SEREM COPIADAS']

## Uso
O script irá:
1. Conectar-se aos bancos de dados de cópia e destino
2. Identificar as tabelas disponíveis para cópia
3. Copiar os dados mais recentes de cada tabela
4. Solicitar confirmação para exclusão de dados antigos (mais de 6 meses) do banco de destino
5. Gerar um relatório detalhado da operação

## Estrutura do Projeto
- `nome_do_script.py`: Script principal contendo toda a lógica de sincronização
- Logs: Salvos no diretório especificado em `log_directory`

## Considerações de Segurança
- As credenciais de banco de dados estão diretamente no código. Para um ambiente de produção, considere usar variáveis de ambiente ou um arquivo de configuração seguro.
- A exclusão de dados antigos requer confirmação manual para evitar perda acidental de dados.

## Contribuições
Contribuições são bem-vindas. Por favor, abra uma issue para discutir mudanças propostas antes de submeter um pull request.

Última atualização: 4 de abril de 2025
