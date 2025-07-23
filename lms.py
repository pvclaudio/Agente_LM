# -*- coding: utf-8 -*-
"""
Created on Thu Jun  5 07:43:47 2025

@author: cvieira
"""

import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import calendar
import time
import io
import teradatasql
import os
import json
import re
import openai
import urllib3
from dotenv import load_dotenv
import numpy as np
import ssl
from datetime import datetime
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Alignment, Font
from openpyxl.styles.numbers import BUILTIN_FORMATS
from sklearn.preprocessing import MinMaxScaler
from io import BytesIO
import logging

st.set_page_config(layout = 'wide')

load_dotenv()
API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = API_KEY

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

st.title('AGENTE DE PAGAMENTOS 📊')

def formata_numero(valor, prefixo=''):
    for unidade in ['', 'mil', 'milhões', 'bilhões']:
        if valor < 1000:
            return f'{prefixo} {valor:.2f} {unidade}'.strip()
        valor /= 1000
    return f'{prefixo} {valor:.2f} trilhões'

def converte_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Dados')  # Exportar para Excel sem índice
    return output.getvalue()

def gerar_excel_formatado(df_export):
    output = io.BytesIO()
    wb = Workbook()
    ws = wb.active
    ws.title = "Pedidos_Agente"

    # Adiciona o cabeçalho
    for r_idx, row in enumerate(dataframe_to_rows(df_export, index=False, header=True)):
        ws.append(row)
        if r_idx == 0:
            for cell in ws[1]:
                cell.font = Font(bold=True)
                cell.alignment = Alignment(horizontal="center", vertical="center")

    # Formata colunas numéricas e datas no padrão brasileiro
    for col in ws.columns:
        max_length = 0
        col_letter = col[0].column_letter
        for cell in col:
            try:
                if isinstance(cell.value, float):
                    cell.number_format = '#.##0,00'
                elif isinstance(cell.value, int):
                    cell.number_format = '0'
                elif isinstance(cell.value, str) and '/' in cell.value and len(cell.value) == 10:
                    cell.number_format = 'DD/MM/YYYY'
            except:
                pass
            # Autoajuste de largura
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))
        ws.column_dimensions[col_letter].width = max_length + 2

    wb.save(output)
    return output.getvalue()

# Credenciais de conexão
    
load_dotenv()

HOST = os.getenv("DB_HOST")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
SCHEMA = "AA_PRD_DDM"
SCHEMA2 = "AA_PRD_WRK"

@st.cache_data(show_spinner="🔍 Trazendo dados do Teradata...")
def base_teradata():
    try:
        with teradatasql.connect(host=HOST, user=USER, password=PASSWORD) as conn:
                    print("Conexão bem-sucedida!")
                    
                    with conn.cursor() as cur:
                        
                        query_journal= f"""
                            SELECT
                                "CompanyCode",
                                "CompanyCodeName",
                                "FiscalYear",
                                "AccountingDocument",
                                "LedgerGLLineItem",
                                "ReferenceDocument",
                                "ReversalReferenceDocument",
                                "GLAccount",
                                "GLAccountLongName",
                                "CostCenter",
                                "CostCenterName",
                                "BalanceTransactionCurrency",
                                "AmountInTransactionCurrency",
                                "GlobalCurrency",
                                "AmountInGlobalCurrency",
                                "FreeDefinedCurrency1",
                                "AmountInFreeDefinedCurrency1",
                                "PostingDate",
                                "DocumentDate",
                                "AccountingDocumentType",
                                "AccountingDocumentTypeName",
                                "AccountingDocCreatedByUser",
                                "DocumentItemText",
                                "OffsettingAccount",
                                "OffsettingAccountName",
                                "ClearingAccountingDocument",
                                "ClearingDate",
                                "PurchasingDocument"
                            FROM {SCHEMA2}.I_JournalEntryItemCube
                            WHERE (TRIM("ReversalReferenceDocument") = '' OR "ReversalReferenceDocument" IS NULL)
                        """
                        cur.execute(query_journal)
                        columns_journal = [desc[0] for desc in cur.description]
                        df = pd.DataFrame(cur.fetchall(), columns=columns_journal)
                        
                        query_estorno= f"""
                            SELECT
                                "CompanyCode",
                                "FiscalYear",
                                "ReversalReferenceDocument"
                            FROM {SCHEMA2}.I_JournalEntryItemCube
                            WHERE (TRIM("ReversalReferenceDocument") <> '' OR "ReversalReferenceDocument" IS NOT NULL)             
                        """
                        cur.execute(query_estorno)
                        columns_estorno = [desc[0] for desc in cur.description]
                        df_estorno = pd.DataFrame(cur.fetchall(), columns=columns_estorno)
                        
    except Exception as e:
        print(f'Identificamos o erro: {e}')
        
    return df, df_estorno

df, df_estorno = base_teradata()

# Criação do Agente com o Chat GPT --------------------------------------------------------------------------------------------------------------------
@st.cache_data(show_spinner="🔍 Analisando os lançamentos com IA...")
def executar_auditoria(df_agente_filtrado):
    # Função para verificar JSON válido
    def extract_json_objects_from_response(response_content):
        logging.basicConfig(level=logging.INFO)
        json_objects = re.findall(r'\{.*?\}', response_content, re.DOTALL)
        parsed = []

        for obj in json_objects:
            try:
                parsed.append(json.loads(obj))
            except json.JSONDecodeError:
                logging.warning(f"Erro ao decodificar JSON: {obj[:100]}")
        
        return parsed
    
    def is_valid_json(response_content):
        try:
            json.loads(response_content)
            return True
        except ValueError:
            return False

    # Função para limpar delimitadores de código da resposta da API
    def clean_json_response(text):
        if text.startswith("```json") and text.endswith("```"):
            return text[7:-3].strip()
        return text.strip()

    # Função para realizar chamadas à API OpenAI
    def invoke_openai(prompt):
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": "gpt-4o",
            "messages": [
                {"role": "system", "content": "Você é um investigador sênior e experiente que identifica fraudes financeiras."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.5
        }

        try:
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=payload,
                verify=False  # ambiente com SSL desabilitado
            )
            response.raise_for_status()
            result = response.json()
            content = result.get('choices', [{}])[0].get('message', {}).get('content', '')
            return clean_json_response(content)
        except Exception as e:
            st.error(f"Erro na chamada da API OpenAI: {e}")
            return ""

    # Função para processar um chunk de dados
    def process_chunk(chunk):
        # Agrupar por "Documento Contábil" e consolidar dados antes de enviar ao modelo
        grouped_chunk = chunk.groupby("Doc Contabil", as_index=False).agg({
            "Nome Conta": "first",
            "Centro de Custo": "first",
            "Valor BRL": "first",
            "Nome Tipo Doc": "first",
            "Nome Contrapartida": "first",
            "Data Registro": "first",
            "Empresa": "first"
        }).reset_index()

        prompt_template = """
        #Objetivos
        
        Seu objetivo é identificar possíveis fraudes e erros na base de dados de pedidos de lançamentos manuais no SAP.
        O futuro da empresa e o emprego de seus funcionários dependem da precisão de suas análises.
        
        #Contexto
        
        Como investigador experiente, considere os seguintes cenários:
        - Lançamentos em contas contábeis mais sensíveis como patrocínios, doações e consultorias.
        - Lançamentos muito semelhantes, considerando o Nome Conta, Centro de Custo, Nome Contrapartida e Valor BRL.
        
        #Saída
        Retorne um JSON com:
        [
          {{
            "Doc Contabil": "123456",
            "Empresa": "Empresa X",
            "Nome Contrapartida": "Fulano",
            "Valor BRL": 123456.78,
            "Data do Registro": "2025-06-01",
            "Motivo": "Duplicidade com outros lançamentos em junho para a mesma contrapartida."
          }},
          ...
        ]
        
        #Fonte da busca
        Aqui estão os dados: {dados}
        """

        dados_json = grouped_chunk.to_json(orient='records')
        prompt = prompt_template.format(dados=dados_json)

        response = invoke_openai(prompt)
        if response:
            try:
                # Extrair apenas os objetos JSON da resposta
                red_flags = extract_json_objects_from_response(response)
                if isinstance(red_flags, list) and red_flags:
                    print(f"✅ JSONs válidos extraídos: {len(red_flags)} itens.")
                    return red_flags
                else:
                    print("⚠️ Nenhum JSON válido encontrado na resposta.")
                    return []
            except json.JSONDecodeError:
                print("❌ Resposta da API não é um JSON válido.")
                return []
        else:
            print("❌ Resposta inválida ou vazia do modelo.")
            return []

    # Função principal do agente de auditoria
    def auditor(file_path):
        
        required_columns = [
            "Nome Conta",
            "Centro de Custo",
            "Valor BRL",
            "Nome Tipo Doc",
            "Nome Contrapartida",
            "Data Registro",
            "Empresa"
        ]
        
        df_entrada = file_path
        df_entrada = df_entrada[required_columns]
        if df_entrada is None or df_entrada.empty:
            return None

        # Processar dados em chunks
        chunk_size = 100
        red_flags = []
        for start in range(0, len(df_entrada), chunk_size):
            chunk = df_entrada.iloc[start:start + chunk_size]
            red_flags.extend(process_chunk(chunk))

        # Gerar DataFrame com os resultados
        df_flags = pd.DataFrame(red_flags)
        if df_flags.empty:
            st.info("Nenhuma suspeita encontrada.")
            st.success(f"🔍 {len(df_flags)} casos suspeitos identificados.")
            return None

        # Retornar resultados
        return df_flags
    
    # Remove o bloco `if __name__ == "__main__":`
    file_path = df_agente_filtrado
    df_flags = auditor(file_path)

    if df_flags is not None and not df_flags.empty:
        print("Fraudes identificadas:")
        print(df_flags.to_string(index=False))
    else:
        print("Nenhuma suspeita encontrada.")

    # Retorna a variável para uso fora da função
    
    converte_excel(df_flags)
    
    return df_flags

@st.cache_data
def consultar_openai(prompt):
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "gpt-4o",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0
    }

    try:
        response = requests.post(url, headers=headers, json=payload, verify=False)
        response.raise_for_status()
        data = response.json()
        return data['choices'][0]['message']['content']
    except Exception as e:
        print("❌ Erro ao consultar a OpenAI:", e)
        return ""

def extrair_resposta(texto):
    try:
        return json.loads(texto)
    except json.JSONDecodeError:
        json_matches = re.findall(r'\{[^}]+\}', texto)
        parsed = []
        for match in json_matches:
            try:
                item = json.loads(match)
                parsed.append(item)
            except:
                continue
        return parsed
    
@st.cache_data(show_spinner="🔍 Analisando os lançamentos com IA...")
def verificar_fraude_por_po(df_flags, df_agente_filtrado, chunk_size=5):
   
    if df_flags is None or df_flags.empty:
        st.info("⚠️ Nenhuma flag identificada para revisão.")
        return pd.DataFrame()

    resultados = []

    for i in range(0, len(df_flags), chunk_size):
        chunk = df_flags.iloc[i:i + chunk_size]
        dados_completos = []

        for _, row in chunk.iterrows():
            numero_doc = row['Doc Contabil']
            motivo_flag = row['Motivo']

            dados_doc = df_agente_filtrado[df_agente_filtrado["Doc Contabil"] == numero_doc].to_dict(orient='records')
            if dados_doc:
                dados_completos.append({
                    "Doc Contabil": numero_doc,
                    "Motivo Flag": motivo_flag,
                    "Dados Documento": json.loads(json.dumps(dados_doc[0], default=str))
                })

        if not dados_completos:
            continue

        prompt = f"""
Você é um auditor especialista em fraudes financeiras em pagamentos.

Você recebeu os seguintes casos suspeitos. Cada caso tem:
- Um motivo levantado pelo primeiro agente (`Motivo Flag`),
- E os dados completos do lançamento (`Dados Documento`).

Para cada caso:
- Indique se o motivo da flag é **procedente** com base nos dados do lançamento.
- Se sim, escreva um **Parecer Revisor** curto, claro e técnico.
- Se não, diga que **não há evidência suficiente** ou que o motivo não é procedente.

Responda **somente com JSON válido**, com estrutura como esta:
[
  {{
    "Doc Contabil": "123456",
    "Procedente": true,
    "Parecer Revisor": "Concordo. O lançamento possui semelhanças que indicam possível duplicidade com os documentos 456789."
  }},
  {{
    "Doc Contabil": "456789",
    "Procedente": false,
    "Parecer Revisor": "Discordo. Os lançamentos identificados como possíveis duplicidades possuem diferenças significativas nos campos XYZ."
  }},
  ...
]

Casos:
{json.dumps(dados_completos, ensure_ascii=False)}
        """

        print(f"🔹 Enviando arquivos {i // chunk_size + 1} com {len(dados_completos)} casos")
        resposta = consultar_openai(prompt)
        print("📩 Resposta da IA:")
        print(resposta[:500])

        pareceres = extrair_resposta(resposta)
        for p in pareceres:
            if "Doc Contabil" in p and "Procedente" in p:
                p["Doc Contabil"] = str(p["Doc Contabil"])
                p["Parecer Revisor"] = p.get("Parecer Revisor", "")
                resultados.append(p)

    df_resultados = pd.DataFrame(resultados)
    df_resultados.columns = [str(c).strip().replace("_", " ") for c in df_resultados.columns]

    if "Doc Contabil" not in df_resultados.columns:
        print("⚠️ Nenhum parecer válido foi retornado pela IA.")
        return pd.DataFrame()

    df_flags["Doc Contabil"] = df_flags["Doc Contabil"].astype(str)
    df_resultados["Doc Contabil"] = df_resultados["Doc Contabil"].astype(str)

    df_agente = df_flags.merge(df_resultados.drop_duplicates(), on="Doc Contabil", how="left")

    return df_agente

df["chave_estorno"] = df["AccountingDocument"] + df["FiscalYear"] + df["CompanyCode"]
df_estorno["chave_estorno"] = df_estorno["ReversalReferenceDocument"] + df_estorno["FiscalYear"] + df_estorno["CompanyCode"]
df_estorno = df_estorno[df_estorno["ReversalReferenceDocument"] != '']

lista_estorno = list(df_estorno["chave_estorno"].unique())

df = df[~df["chave_estorno"].isin(lista_estorno)]
df = df.drop(columns=["ReversalReferenceDocument","chave_estorno"])

df["CostCenter"] = df["CostCenter"].fillna("Não Informado")
df["CostCenterName"] = df["CostCenterName"].fillna("Não Informado")
df["DocumentItemText"] = df["DocumentItemText"].fillna("Não Informado")
df["ClearingAccountingDocument"] = df["ClearingAccountingDocument"].fillna("Não Compensado")
df["ClearingDate"] = df["ClearingDate"].fillna("Não Compensado")

df["CostCenter"] = df["CostCenter"].replace({"":"Não Informado"})
df["CostCenterName"] = df["CostCenterName"].replace({"":"Não Informado"})
df["DocumentItemText"] = df["DocumentItemText"].replace({"":"Não Informado"})
df["ClearingAccountingDocument"] = df["ClearingAccountingDocument"].replace({"":"Não Compensado"})
df["ClearingDate"] = df["ClearingDate"].replace({"":"Não Compensado"})
df["DocumentItemText"] = df["DocumentItemText"].replace({"":"Não Informado"})
df["OffsettingAccountName"] = df["OffsettingAccountName"].replace({"":"Não Informado"})
df["PurchasingDocument"] = df["PurchasingDocument"].replace({"":"Não Informado"})
df["PurchasingDocument"] = df["PurchasingDocument"].replace({"0":"Não Informado"})

df["PostingDate"] = pd.to_datetime(df["PostingDate"], errors="coerce").dt.strftime('%d/%m/%Y')
df["DocumentDate"] = pd.to_datetime(df["DocumentDate"], errors="coerce").dt.strftime('%d/%m/%Y')

df = df[~df["DocumentDate"].isnull()]

df["AmountInTransactionCurrency"] = df["AmountInTransactionCurrency"].astype("float")
df["AmountInGlobalCurrency"] = df["AmountInGlobalCurrency"].astype("float")
df["AmountInFreeDefinedCurrency1"] = df["AmountInFreeDefinedCurrency1"].astype("float")

lista_gla = sorted(df['GLAccountLongName'].unique().tolist())

lista_risco = ["CONSULTORIAS","PATROCINIO","MATERIAIS PROMOCIONAIS / INSTITUCIONAIS","PROPAGANDA E PUBLICIDADE",
               "DOACOES NAO DEDUTIVEIS","FEIRAS E CONGRESSOS","BRINDES"]

df_risco = df[df["GLAccountLongName"].isin(lista_risco)]



lista_estoque_materiais = ["EMPRESTIMO DE MATERIAIS", "EMPRESTIMO DE MATERIAIS COMODATO", "ESTOQUE EM PODER DE TERCEIROS",
                           "ESTOQUE DE TERCEIROS", "MATERIAL EM CONSIGNACAO", "MATERIAL EM ALMOXARIFADO CONTA MANUAL",
                           "MATERIA PRIMA GÁS NÃO PROCESSADO"]

lista_estoque_oleo = ["EMPRESTIMO DE OLEO", "EMPRÉSTIMO GÁS MUTUO", "ESTOQUE DE GAS PROCESSADO", "ESTOQUE DE GÁS MUTUO",
                      "ESTOQUE DE OLEO", "ESTOQUE DE GAS PROCESSADO", "ESTOQUE DE OLEO - CARGA",
                      "ESTOQUE DE OLEO - PARCELA IFRS 16", "ESTOQUE DE OLEO - REMESSA FORMAÇÃO DE LOTE"]

df_materiais = df[df["GLAccountLongName"].isin(lista_estoque_materiais)]

df_oleo = df[df["GLAccountLongName"].isin(lista_estoque_oleo)]

# -----------------------------
# Aplicativo
# -----------------------------

st.sidebar.image("PRIO_SEM_POLVO_PRIO_PANTONE_LOGOTIPO_Azul.png")

df_lm = df.copy()
df_lm = df_lm[df_lm["AccountingDocument"].str.startswith("19", na=False)]
df_lm = df_lm[df_lm["GLAccount"].str.startswith("3", na=False)]
df_lm = df_lm[df_lm["OffsettingAccount"].str.startswith("10", na=False)]

lista_conta_exclusao = ["VARIACAO CAMBIAL PASSIVA REALIZADA",
                        "VARIACAO CAMBIAL ATIVA REALIZADA",
                        "CUSTOS DOS PRODUTOS VENDIDOS"]

lista_contrapartida_exclusao = [
    "15 OFICIO DE NOTAS DA COMARCA",
    "2 OFICIO DO REGISTRO DE PROTESTO",
    "ABRASCA - ASS. BRAS. DAS CIAS ABERT",
    "ACE SEGURADORA SA",
    "AIG SEGUROS BRASIL S.A.",
    "ABEP - ASSOCIACAO BRASILEIRA DE EMPRESAS",
    "AGENCIA NACIONAL DO PETROLEO GAS NA E BIOCOMBUSTIVEIS",
    "AMERICAN CHAMBER OF COMMERCE FOR BRAZIL",
    "AUSTRAL SEGURADORA S.A.",
    "ASSOCIACAO BRASILEIRA DOS PRODUTORES INDEPENDENTES DE",
    "ASSOCIACAO DE COMERCIO EXTERIOR DO BRASIL AEB",
    "ASSOCIACAO DOS REGISTRADORES DE TITULOS E",
    "ASSURANCEFORENINGEN SKULD",
    "ASA ASSESSORIA DE COMERCIO EXTERIOR LTDA",
    "B3 S.A. - BRASIL, BOLSA, BALCAO",
    "B3 SA BRASIL BOLSA BALCAO",
    "BANCO ITAUCARD S.A",
    "BANCO ITAUCARD S.A.",
    "BANCO CENTRAL DO BRASIL",
    "BANCO DAYCOVAL S.A.",
    "BRADESCO SAUDE S/A",
    "BTG PACTUAL INVESTMENT BANKING LTDA",
    "CAIXA ECONOMICA FEDERAL",
    "CENTRO DE INTEGRACAO EMPRESA ESCOLA E RIO DE JANEIRO",
    "CITIGROUP GOLBAL MARKETS LIMITED",
    "ERNST & YOUNG ASSESSORIA EMPRESARIAL LTDA",
    "SHELL WESTEN SUPPLY AND TRADING LIMITED",
    "EZZE SEGUROS S.A.",
    "FLASH TECNOLOGIA E PAGAMENTOS LTDA",
    "GOOGLE BRASIL INTERNET LTDA.",
    "INSTITUTO BRASILEIRO DE PETROLEO, GAS E",
    "ITAU BBA INTERNATIONAL PLC",
    "ITAU CORRETORA DE VALORES S/A",
    "J S ASSESSORIA ADUANEIRA LTDA",
    "JUNTA COMERCIAL DO ESTADO DO RIO DE JANEIRO",
    "JUNTO SEGUROS S.A.",
    "KOVR SEGURADORA S A",
    "LUFTHANSA AIRPLUS SERVICEKARTEN GMB",
    "MINISTERIO DA ECONOMIA",
    "MINISTERIO DA FAZENDA",
    "MINISTERIO DA PREVIDENCIA SOCIAL",
    "MUNICIPIO DA SERRA PREFEITURA MUNICIPAL DA SERRA",
    "MUNICIPIO DE MACAE",
    "MUNICIPIO DE MANAUS",
    "MUNICIPIO DE RIO DAS OSTRAS",
    "MUNICIPIO DE RIO DE JANEIRO",
    "MUNICIPIO DE SALVADOR",
    "MUNICIPIO DE SAO GONCALO",
    "MUNICIPIO DE SAO JOAO DA BARRA",
    "MS LOGISTICA INTERNACIONAL LTDA",
    "ODONTOPREV S.A.",
    "PETRÓLEO BRASILEIRO S.A.",
    "PETROBRAS UTGCAB",
    "PLUXEE LUXEMBOURG",
    "Pluxee Austria GmbH",
    "POTTENCIAL SEGURADORA S.A.",
    "PREFEITURA MUNIC. DE SÃO JOÃO DA BA",
    "PREFEITURA MUNICIPAL DE VILA VELHA",
    "PRUDENTIAL DO BRASIL VIDA EM GRUPO",
    "PROSAFE PRODUCTION BV",
    "SECRETARIA DE ESTADO DE FAZENDA - S",
    "SECRETARIA DO TESOURO NACIONAL",
    "SECRET DE EST DE FAZENDA - RJ",
    "STADT WIEN BUCHHALTUNGSABTEILUNG 33",
    "SUPREMO TRIBUNAL FEDERAL",
    "TRIBUNAL DE JUSTICA DO ESTADO DO",
    "TRIBUNAL REGIONAL DO TRABALHO DA 1 REGIA"
]


df_lm = df_lm[~df_lm["GLAccountLongName"].isin(lista_conta_exclusao)]
df_lm = df_lm[~df_lm["OffsettingAccountName"].isin(lista_contrapartida_exclusao)]

df_app = df_lm.copy()
df_app.info()

df_app = df_app.rename(columns={
    "CompanyCode":"Numero Empresa",
    "CompanyCodeName":"Empresa",
    "FiscalYear":"Ano",
    "AccountingDocument":"Doc Contabil",
    "LedgerGLLineItem":"Item",
    "ReferenceDocument":"Doc Referencia",
    "GLAccount":"Numero Conta",
    "GLAccountLongName":"Nome Conta",
    "CostCenter":"Numero CC",
    "CostCenterName":"Centro de Custo",
    "BalanceTransactionCurrency":"Moeda Transacao",
    "AmountInTransactionCurrency":"Valor Transacao",
    "GlobalCurrency":"Moeda Global",
    "AmountInGlobalCurrency":"Valor USD",
    "FreeDefinedCurrency1":"Moeda BRL",
    "AmountInFreeDefinedCurrency1":"Valor BRL",
    "PostingDate":"Data Registro",
    "DocumentDate":"Data Documento",
    "AccountingDocumentType":"Tipo de Doc",
    "AccountingDocumentTypeName":"Nome Tipo Doc",
    "AccountingDocCreatedByUser":"Criador",
    "DocumentItemText":"Texto",
    "OffsettingAccount":"Numero Contrapartida",
    "OffsettingAccountName":"Nome Contrapartida",
    "ClearingAccountingDocument":"Doc Compensacao",
    "ClearingDate":"Data Compensacao",
    "PurchasingDocument":"Pedido"
    })

df_app["Data2"] = pd.to_datetime(df_app["Data Registro"], errors="coerce")
df_app = df_app.sort_values(by= "Data2", ascending = False)
df_app["Ano"] = df_app["Data2"].dt.year.fillna(df_app["Data Documento"].str[-4:]).astype("int").astype(str)
df_app["Mes"] = df_app["Data2"].dt.month.fillna(df_app["Data Documento"].str[-7:-5]).astype("int").astype(str)
df_app["Ano"] = df_app["Ano"].replace({"2202":"2022"})

lista_pedidos = ["Todos"] + sorted(list(df_app["Pedido"].str.strip().unique()))
lista_contas = ["Todas"] + sorted(list(df_app["Nome Conta"].str.strip().unique()))
lista_areas = ["Todas"] + sorted(list(df_app["Centro de Custo"].str.strip().unique()))
lista_anos = ["Todos"] + sorted(df_app["Ano"].dropna().unique())
lista_meses = ["Todos"] + sorted(df_app["Mes"].dropna().unique(), key=lambda x: int(x))
lista_aprovadores = ["Todos"] + sorted(list(df_app["Criador"].unique()))
lista_empresas = ["Todas"] + sorted(df_app["Empresa"].unique().astype(str).tolist())
lista_contrapartida = ["Todos"] + sorted(df_app["Nome Contrapartida"].unique().astype(str).tolist())

with st.sidebar.expander("Pedidos"):
    pedidos_colados = st.text_area("Cole aqui a lista de pedidos: ", height=100)
    
    lista_pedidos_colados = []
    if pedidos_colados:
        lista_pedidos_colados = re.split(r"[,\n;\s]+", pedidos_colados)
        lista_pedidos_colados = [p.strip() for p in lista_pedidos_colados if p.strip() in lista_pedidos]
    
    pedidos_select = st.multiselect(
        "Ou selecione manualmente:",
        options=lista_pedidos,
        default=lista_pedidos_colados if lista_pedidos_colados else "Todos"
    )

with st.sidebar.expander("Conta Contabil"):
    contas_colados = st.text_area("Cole aqui a lista de contas: ", height=100)
    
    lista_contas_colados = []
    if contas_colados:
        lista_contas_colados = re.split(r"[,\n;\s]+", contas_colados)
        lista_contas_colados = [p.strip() for p in lista_contas_colados if p.strip() in lista_contas]
    
    contas_select = st.multiselect(
        "Ou selecione manualmente:",
        options=lista_contas,
        default=lista_contas_colados if lista_contas_colados else "Todas"
    )

with st.sidebar.expander("Empresas"):
    empresas_select = st.multiselect("Selecione:",lista_empresas,default="Todas")
    
with st.sidebar.expander("Area"):
    areas_select = st.multiselect("Selecione:",lista_areas,default="Todas")
    
with st.sidebar.expander("Beneficiário"):
    contrapartida_select = st.multiselect("Selecione:",lista_contrapartida,default="Todos")
    
with st.sidebar.expander("Aprovador"):
    aprovador_select = st.multiselect("Selecione:",lista_aprovadores,default="Todos")
    
with st.sidebar.expander("Ano"):
    anos_select = st.multiselect("Selecione:",lista_anos,default="2025")

with st.sidebar.expander("Mes"):
    meses_select = st.multiselect("Selecione:",lista_meses,default="Todos")

check_materiais = st.sidebar.toggle("Verificar Materiais")
check_oleo = st.sidebar.toggle("Verificar Óleo")
check_risco = st.sidebar.toggle("Maiores Riscos")

if "Todos" not in pedidos_select:
    df_app = df_app[df_app["Pedido"].isin(pedidos_select)]
    
if "Todas" not in contas_select:
    df_app = df_app[df_app["Nome Conta"].isin(contas_select)]

if "Todas" not in empresas_select:
    df_app = df_app[df_app["Empresa"].isin(empresas_select)]
    
if "Todas" not in areas_select:
    df_app = df_app[df_app["Centro de Custo"].isin(areas_select)]
    
if "Todos" not in contrapartida_select:
    df_app = df_app[df_app["Nome Contrapartida"].isin(contrapartida_select)]

if "Todos" not in aprovador_select:
    df_app = df_app[df_app["Aprovador"].isin(aprovador_select)]
    
if "Todos" not in anos_select:
    df_app = df_app[df_app["Ano"].isin(anos_select)]
    
if "Todos" not in meses_select:
    df_app = df_app[df_app["Mes"].isin(meses_select)]
    
if check_materiais:
    df_app = df_app[df_app['Nome Conta'].isin(lista_estoque_materiais)]
    
if check_oleo:
    df_app = df_app[df_app['Nome Conta'].isin(lista_estoque_oleo)]
    
if check_risco:
    df_app = df_app[df_app['Nome Conta'].isin(lista_risco)]


aba1, aba2, aba3 = st.tabs(['Base LMs','Gráficos','Agente'])

with aba1:
    valor = df_app['Valor BRL'].sum()
    valor_formatado = f"R$ {valor:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
    st.metric("Valor Total - BRL", valor_formatado)
    st.dataframe(df_app.drop(['Data2','Mes'], axis=1))
    st.markdown(f'A tabela possui **{df_app.shape[0]}** linhas e **{df_app.shape[1]}** colunas.')
    
    st.download_button(
        label="📥 Baixar relatório de lançamentos",
        data=gerar_excel_formatado(df_app.drop(['Data2','Mes'], axis=1)),
        file_name="base_lms.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

with aba2:
    numero_areas = st.number_input('Informe o número de áreas que deseja visualizar', min_value=1, value=5)
    df_app_areas = (
        df_app.groupby('Centro de Custo')['Valor BRL']
        .sum()
        .reset_index()
        .nlargest(numero_areas, 'Valor BRL')
    )
    df_app_areas["Valor Formatado"] = df_app_areas["Valor BRL"].apply(lambda x: formata_numero(x,"R$"))
    fig = px.bar(
                df_app_areas,
                x="Centro de Custo",
                y="Valor BRL",
                text = "Valor Formatado",
                title=f'Distribuição pelo top {numero_areas} áreas'
            )
    st.plotly_chart(fig, use_container_width=True)
    
    numero_beneficiarios = st.number_input('Informe o número de beneficiários que deseja visualizar', min_value=1, value=5)
    df_app_beneficiarios = (
        df_app.groupby('Nome Contrapartida')['Valor BRL']
        .sum()
        .reset_index()
        .nlargest(numero_beneficiarios, 'Valor BRL')
    )
    df_app_beneficiarios["Valor Formatado"] = df_app_beneficiarios["Valor BRL"].apply(lambda x: formata_numero(x,"R$"))
    fig = px.bar(
                df_app_beneficiarios,
                x="Nome Contrapartida",
                y="Valor BRL",
                text = "Valor Formatado",
                title=f'Distribuição pelo top {numero_beneficiarios} bebeficiários'
            )
    st.plotly_chart(fig, use_container_width=True)
    
with aba3:
    st.title("Análise dos Agentes")
    
    df_agente = df_app[["Nome Conta", "Centro de Custo", "Valor BRL", "Nome Tipo Doc", "Nome Contrapartida",
                        "Data Registro", "Empresa", "Ano", "Mes"]]
    
    # 1. Seletor de ano e mês com base no df_agente original
    anos_disponiveis = sorted(df_agente["Ano"].unique()) + ['Todos']
    meses_disponiveis = sorted(df_agente["Mes"].unique()) + ['Todos']
    areas_disponiveis = sorted(df_agente["Centro de Custo"].unique()) + ['Todas']
    
    ano = st.multiselect("Selecione o Ano", anos_disponiveis, default = 'Todos')
    mes = st.multiselect("Selecione o Mês", meses_disponiveis, default = 'Todos')
    area = st.multiselect("Selecione a área", areas_disponiveis, default = 'Todas')
    valor_minimo = st.number_input(
        "Filtrar valor dos pedidos (BRL)", 
        min_value=0, 
        value=100000, 
        step=10000
    )

    df_agente_filtrado = df_agente[df_agente["Valor BRL"] > valor_minimo].copy()
    
    if 'Todos' not in  ano:
        df_agente_filtrado = df_agente_filtrado[df_agente_filtrado['Ano'].isin(ano)]
    
    if 'Todos' not in mes:
        df_agente_filtrado = df_agente_filtrado[df_agente_filtrado['Mes'].isin(mes)]
        
    if 'Todas' not in area:
        df_agente_filtrado = df_agente_filtrado[df_agente_filtrado['Centro de Custo'].isin(area)]
        
    # 3. Inicializar estado de controle
    if "executar_analise" not in st.session_state:
        st.session_state.executar_analise = False
    if "executar_agora" not in st.session_state:
        st.session_state.executar_agora = False

    # 4. Botões de controle
    with st.container():
        col_executar, col_resetar = st.columns(2)

        with col_executar:
            if st.button("▶️ Executar Análise do Agente", key="btn_executar"):
                st.session_state.executar_agora = True

        with col_resetar:
            if st.button("🔁 Resetar Análise", key="btn_resetar"):
                st.session_state.executar_agora = False
                st.session_state.executar_analise = False
                st.success("Análise resetada.")

    # 5. Execução somente ao clicar no botão
    if st.session_state.executar_agora and not st.session_state.executar_analise:
        with st.spinner("🔄 Gerando arquivos e analisando..."):

            df_flags = executar_auditoria(df_agente_filtrado)
            df_agente_resultado = verificar_fraude_por_po(df_flags, df_agente_filtrado)

            if df_flags.empty:
                st.info("✅ Nenhum caso com 'Flag: Sim' foi identificado.")
            else:
                st.success(f"Análise concluída. {df_flags.shape[0]} casos com red flags.")
                st.dataframe(df_flags)
                st.download_button(
                    label="📥 Baixar resultados em CSV",
                    data=df_flags.to_csv(index=False, encoding="utf-8-sig"),
                    file_name=f"df_flags_{ano}_{mes}.csv",
                    mime="text/csv"
                )

        # Atualiza flags de estado após execução
        st.session_state.executar_analise = True
        st.session_state.executar_agora = False
        
    # 6. Bases Agente
    st.title("Base de Dados do Agente")
    st.dataframe(df_agente_filtrado)
    st.markdown(f'A tabela possui **{df_agente_filtrado.shape[0]}** linhas e **{df_agente_filtrado.shape[1]}** colunas')
        
    st.download_button(
        label="📥 Baixar relatório do Agente",
        data=gerar_excel_formatado(df_agente_filtrado),
        file_name="base_agente.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
