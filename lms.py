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
from io import BytesIO
import teradatasql
import os
import json
import re
import openai
import urllib3
from dotenv import load_dotenv
import numpy as np

st.set_page_config(layout = 'wide')

st.title('AGENTE DE PAGAMENTOS 📊')

def formata_numero(valor, prefixo=''):
    for unidade in ['', 'mil', 'milhões', 'bilhões']:
        if valor < 1000:
            return f'{prefixo} {valor:.2f} {unidade}'.strip()
        valor /= 1000
    return f'{prefixo} {valor:.2f} trilhões'

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
               "DOACOES NAO DEDUTIVEIS","FEIRAS E CONGRESSOS"]

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

lista_lm_acc = ["DESPESAS COM ALIMENTACAO","DESPESAS COM TRANSPORTE","CONDUCOES",
            "OUTRAS DESPESAS","FEIRAS E CONGRESSOS","SUPORTE TECNICO OPERACIONAL",
            "OUTRAS DESPESAS DE VIAGENS","MATERIAIS DE LIMPEZA, COPA E HIGIENE","EVENTOS E CONFRATERNIZACOES",
            "ALIMENTACAO E REFEICAO","DIARIAS EM VIAGENS","DESPESAS COM TRANSPORTE EM VIAGENS",
            "OUTROS MATERIAIS DE USO E CONSUMO","BRINDES","HOSPEDAGENS","OUTROS SERVICOS DE TERCEIROS",
            "MANUTENCAO DE IMOVEIS","AJUDA DE CUSTO","MATERIAIS PROMOCIONAIS / INSTITUCIONAIS",
            "DESENVOLVIMENTO (CURSOS E ESTUDOS)","SUPORTE E MANUTENCAO DE INFORMATICA",
            "OUTRAS DESPESAS GERAIS E ADMINISTRATIVAS","PROPAGANDA E PUBLICIDADE","PASSAGENS AEREAS",
            "TREINAMENTOS TECNICOS","MATERIAIS DE MANUTENCAO PREDIAL","CONSULTORIAS","SALARIOS",
            "DESPESAS COM ORNAMENTACAO","SERVICOS DE ENGENHARIA","PATROCINIO","ALUGUEL DE VEICULOS EM VIAGENS",
            "DOACOES NAO DEDUTIVEIS","FEIRAS E CONGRESSOS"]

lista_lm_benef = ["FLASH TECNOLOGIA E PAGAMENTOS LTDA","J S ASSESSORIA ADUANEIRA LTDA","BANCO ITAUCARD S.A",
                  "MINISTERIO DA ECONOMIA","LUFTHANSA AIRPLUS SERVICEKARTEN GMB","Pluxee Austria GmbH",
                  "ERNST & YOUNG ASSESSORIA EMPRESARIAL LTDA","GOOGLE BRASIL INTERNET LTDA.",
                  "SECRET DE EST DE FAZENDA - RJ","INSTITUTO BRASILEIRO DE PETROLEO, GAS E","PETRÓLEO BRASILEIRO S.A.",
                  "ITAU CORRETORA DE VALORES S/A","B3 S.A. - BRASIL, BOLSA, BALCAO","CAIXA ECONOMICA FEDERAL",
                  "PREFEITURA MUNIC. DE SÃO JOÃO DA BA","ITAU BBA INTERNATIONAL PLC","B3 SA BRASIL BOLSA BALCAO",
                  "CENTRO DE INTEGRACAO EMPRESA ESCOLA E RIO DE JANEIRO","15 OFICIO DE NOTAS DA COMARCA",
                  "BTG PACTUAL INVESTMENT BANKING LTDA","BANCO ITAUCARD S.A.", "CITIGROUP GOLBAL MARKETS LIMITED"]

df_lm = df_lm[df_lm["GLAccountLongName"].isin(lista_lm_acc)]
df_lm = df_lm[~df_lm["OffsettingAccountName"].isin(lista_lm_benef)]

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
    
with st.sidebar.expander("Aprovador"):
    aprovador_select = st.multiselect("Selecione:",lista_aprovadores,default="Todos")
    
with st.sidebar.expander("Ano"):
    anos_select = st.multiselect("Selecione:",lista_anos,default="2025")

with st.sidebar.expander("Mes"):
    meses_select = st.multiselect("Selecione:",lista_meses,default="Todos")

check_materiais = st.sidebar.toggle("Verificar Materiais")
check_oleo = st.sidebar.toggle("Verificar Oleo")
check_risco = st.sidebar.toggle("Maiores Riscos")

if "Todos" not in pedidos_select:
    df_app = df_app[df_app["Pedido"].isin(pedidos_select)]
    
if "Todas" not in contas_select:
    df_app = df_app[df_app["Nome Conta"].isin(contas_select)]

if "Todas" not in areas_select:
    df_app = df_app[df_app["Empresa"].isin(empresas_select)]
    
if "Todas" not in areas_select:
    df_app = df_app[df_app["Centro de Custo"].isin(areas_select)]

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
