# Extrator de Riscos por funcionarios - SOC
- Esta automação tem como objetivo consultar a API do SOC para 
obter uma lista de funcionarios por empresa e os riscos ocupacionais de cada funcionario ,filtra as colunas requeridas e consolida as informações em um arquivo XLSX. 

# Funcionalidades 
- Consulta funcionarios por empresa a API do SOC
- Consulta riscos ocupacionais de cada funcionario
- Exporta os dados unificados em um arquivo Excel 

# Linguagem / biblioteca
- Python 3.13
- requests
- openpyxl
- pandas

# Estrutura de arquivos 
- Extrair.py : script principal para execucao e extracao
- Base_empresas.xlsx : Arquivo de entrada contendo a lista de empresas 
- saida_soc_unificadas.xlsx : Arquivo de saida 

