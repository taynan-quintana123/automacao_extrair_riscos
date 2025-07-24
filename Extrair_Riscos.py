import requests
import json
import pandas as pd
import time

# === CONFIGURAÇÕES ===
CAMINHO_EMPRESAS = r"C:\Users\Home\Desktop\Codigos_CAZA\Base_empresas.xlsx"
ARQUIVO_SAIDA = "saida_soc_unificada.xlsx"

# === PARÂMETROS API FUNCIONÁRIOS ===
CODIGO_FUNCIONARIOS = '157817'
CHAVE_FUNCIONARIOS = ''

# === PARÂMETROS API RISCOS ===
CODIGO_RISCOS = '206777'
CHAVE_RISCOS = ''

# === ETAPA 1: Consultar Funcionários ===
print("📥 Consultando funcionários por empresa...")

try:
    df_empresas = pd.read_excel(CAMINHO_EMPRESAS)
    empresas = df_empresas['empresaTrabalho'].dropna().astype(str).str.strip()
except Exception as erro:
    print(f"Erro ao ler a base: {erro}")
    exit()

todos_funcionarios = []

for empresa_id in empresas:
    print(f"🔄 Empresa {empresa_id}...")

    parametros = {
        'empresa': empresa_id,
        'codigo': CODIGO_FUNCIONARIOS,
        'chave': CHAVE_FUNCIONARIOS,
        'tipoSaida': 'json',
        'ativo': 'sim',
        'inativo': 'nao',
        'afastado': 'sim',
        'pendente': 'sim',
        'ferias': 'sim'
    }

    try:
        resposta = requests.get(
            "https://ws1.soc.com.br/WebSoc/exportadados",
            params={"parametro": json.dumps(parametros)},
            timeout=20
        )

        if resposta.status_code == 200 and resposta.text.strip() and resposta.text.strip() != "[]":
            dados = resposta.json()
            if isinstance(dados, list):
                for item in dados:
                    item["empresaTrabalho"] = empresa_id
                    todos_funcionarios.append(item)
                print(f"{len(dados)} funcionários adicionados.")
            else:
                print(f"Nenhum dado da empresa {empresa_id}.")
        else:
            print(f"Erro ou resposta vazia da empresa {empresa_id}")
    except Exception as e:
        print(f"Erro na empresa {empresa_id}: {e}")

    time.sleep(0.5)

df_funcionarios = pd.DataFrame(todos_funcionarios)

# Colunas obrigatórias
colunas_func = [
    "empresaTrabalho", "CODIGO", "CODIGOUNIDADE", "NOMEUNIDADE",
    "CODIGOCARGO", "NOMECARGO", "CODIGOSETOR", "NOMESETOR"
]

for col in colunas_func:
    if col not in df_funcionarios.columns:
        df_funcionarios[col] = ""

df_funcionarios = df_funcionarios[colunas_func]

# === ETAPA 2: Consultar Riscos ===
print("\n📥 Consultando riscos por funcionário...")

dados_riscos = []

for _, row in df_funcionarios.iterrows():
    empresa = str(row["empresaTrabalho"]).strip()
    funcionario = str(row["CODIGO"]).strip()

    print(f"Riscos -> empresa {empresa} - funcionario {funcionario}")

    parametros = {
        "empresa": "1028815",
        "codigo": CODIGO_RISCOS,
        "chave": CHAVE_RISCOS,
        "tipoSaida": "json",
        "empresaTrabalho": empresa,
        "funcionario": funcionario
    }

    try:
        resposta = requests.get("https://ws1.soc.com.br/WebSoc/exportadados", params={"parametro": json.dumps(parametros)}, timeout=15)
        conteudo = resposta.text.strip()

        if resposta.status_code == 200 and conteudo and conteudo != "[]":
            dados = resposta.json()
            if isinstance(dados, list):
                for item in dados:
                    item["empresaTrabalho"] = empresa
                    item["CODIGOFUNCIONARIO"] = funcionario
                    dados_riscos.append(item)
            else:
                dados_riscos.append({
                    "empresaTrabalho": empresa,
                    "CODIGOFUNCIONARIO": funcionario,
                    "RISCO": "Sem risco"
                })
        else:
            dados_riscos.append({
                "empresaTrabalho": empresa,
                "CODIGOFUNCIONARIO": funcionario,
                "RISCO": "Sem risco"
            })

    except Exception:
        dados_riscos.append({
            "empresaTrabalho": empresa,
            "CODIGOFUNCIONARIO": funcionario,
            "RISCO": "Sem risco"
        })

    time.sleep(0.5)

df_riscos = pd.DataFrame(dados_riscos)

# Padroniza colunas de risco
colunas_risco = ["empresaTrabalho", "CODRISCO", "RISCO", "FUNCIONARIO", "CODIGOFUNCIONARIO"]
for col in colunas_risco:
    if col not in df_riscos.columns:
        df_riscos[col] = ""

df_riscos = df_riscos[colunas_risco]

# === ETAPA FINAL: Mesclar Funcionários com Riscos ===
df_unificado = pd.merge(df_funcionarios, df_riscos, how="left", left_on=["empresaTrabalho", "CODIGO"], right_on=["empresaTrabalho", "CODIGOFUNCIONARIO"])

# Reorganiza colunas para exportação
colunas_finais = [
    "empresaTrabalho", "CODIGOFUNCIONARIO", "CODIGOUNIDADE", "NOMEUNIDADE",
    "CODIGOCARGO", "NOMECARGO", "CODIGOSETOR", "NOMESETOR",
    "CODRISCO", "RISCO", "FUNCIONARIO"
]

for col in colunas_finais:
    if col not in df_unificado.columns:
        df_unificado[col] = ""

df_unificado = df_unificado[colunas_finais]

# === EXPORTAR ===
df_unificado.to_excel(ARQUIVO_SAIDA, index=False)
print(f"\nArquivo '{ARQUIVO_SAIDA}' salvo com sucesso!")
