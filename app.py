# app.py
# Streamlit pipeline (Bronze ➜ Silver ➜ Gold) para planilhas do Cortex
# - Faz upload de CSV/XLSX (Bronze)
# - Aplica tratamentos configuráveis para gerar Silver
# - Oferece várias opções de "Gold" e espaço para SQL (DuckDB)
# - Gera gráficos com Plotly e permite exportar CSV/Parquet
#
# Observação: marque os pontos "TODO" para inserir suas regras/SQL específicas.

# =======================
# Imports 
# =======================
import io
import unicodedata
from typing import List, Optional

import duckdb
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

# ----------------------
# Configurações gerais
# ----------------------
st.set_page_config(page_title="Cortex → Bronze/Silver/Gold", layout="wide")

st.title("Cortex ETL – Bronze ➜ Silver ➜ Gold")
st.caption(
    "Faça upload da planilha do Cortex (CSV/XLSX), normalize campos (incluindo datas em string), "
    "enriqueça com SQL opcional e gere tabelas Gold. Exporte e visualize com Plotly."
)

# ----------------------
# Helpers
# ----------------------
def clean_column_name(col: str, keep_accents: bool = False) -> str:
    c = col.strip()
    if not keep_accents:
        c = ''.join(ch for ch in unicodedata.normalize('NFKD', c) if not unicodedata.combining(ch))
    c = c.lower().replace(' ', '_')
    c = ''.join(ch for ch in c if ch.isalnum() or ch in ['_', '-'])
    return c

def clean_columns(df: pd.DataFrame, keep_accents: bool = False) -> pd.DataFrame:
    df = df.copy()
    df.columns = [clean_column_name(c, keep_accents=keep_accents) for c in df.columns]
    return df

def parse_date_series(s: pd.Series, fmt: Optional[str], dayfirst: bool, infer: bool) -> pd.Series:
    if fmt:
        return pd.to_datetime(s, format=fmt, errors="coerce")
    return pd.to_datetime(s, errors="coerce", dayfirst=dayfirst, infer_datetime_format=infer)

def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")

def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    df.to_parquet(bio, index=False)
    return bio.getvalue()

def register_duckdb_tables(con: duckdb.DuckDBPyConnection, **dfs):
    for name, df in dfs.items():
        if df is not None:
            con.register(name, df)

# ----------------------
# Sidebar – Upload & Leitura (BRONZE)
# ----------------------
with st.sidebar:
    st.header("1) Upload – Bronze")
    f = st.file_uploader("Carregue a planilha do Cortex (CSV ou XLSX)", type=["csv", "xlsx"])

    read_opts = {}
    filetype = None
    if f is not None:
        filetype = "xlsx" if f.name.lower().endswith(".xlsx") else "csv"

    if filetype == "csv":
        sep = st.selectbox("Separador CSV", [",", ";", "|", "\t"], index=1)
        enc = st.selectbox("Encoding", ["utf-8", "latin-1", "utf-16"], index=0)
        read_opts.update({"sep": sep, "encoding": enc})
    elif filetype == "xlsx":
        sheet = st.text_input("Nome da aba (opcional, deixe vazio para a 1ª)", "")
        read_opts.update({"sheet_name": sheet if sheet.strip() else 0})

    st.markdown("---")
    st.header("2) Opções – Silver (limpeza)")
    keep_accents = st.checkbox("Manter acentos nos nomes de colunas?", value=False)
    strip_strings = st.checkbox("Remover espaços extras em campos de texto", value=True)
    na_values = st.text_input("Valores a considerar como NA (separados por vírgula)", "NULL, N/A, -")
    na_list = [v.strip() for v in na_values.split(",")] if na_values.strip() else None

    st.subheader("Datas")
    date_col = st.text_input("Nome da coluna de data (ex.: event_time)", "")
    date_fmt = st.text_input("Formato da data (ex.: %d/%m/%Y %H:%M:%S). Deixe vazio para auto-detecção", "")
    dayfirst = st.checkbox("Dia primeiro?", value=True)
    infer_dt = st.checkbox("Tentar inferir o formato automaticamente", value=True)

    st.markdown("---")
    st.header("3) Opções – Gold")
    gold_choose_cols = st.text_input("Gold: escolher colunas (separadas por vírgula). Vazio = todas", "")
    gold_drop_dupes = st.checkbox("Gold: remover duplicados", value=True)
    gold_dropna_subset = st.text_input("Gold: descartar linhas com NA nestas colunas (vírgula)", "")
    gold_date_filter_on = st.checkbox("Gold: filtrar por faixa de datas usando a coluna de data acima", value=False)
    gold_date_start = st.text_input("Data inicial (YYYY-MM-DD)", "")
    gold_date_end = st.text_input("Data final (YYYY-MM-DD)", "")

# Leitura Bronze
bronze_df = None
if f is not None:
    try:
        if filetype == "csv":
            bronze_df = pd.read_csv(f, **read_opts, na_values=na_list, keep_default_na=True)
        else:
            bronze_df = pd.read_excel(f, **read_opts, na_values=na_list)
    except Exception as e:
        st.error(f"Erro ao ler arquivo: {e}")

# ----------------------
# Conteúdo principal
# ----------------------
tab_bronze, tab_silver, tab_gold, tab_sql, tab_viz = st.tabs(["Bronze", "Silver", "Gold", "SQL (opcional)", "Gráficos"])

with tab_bronze:
    st.subheader("Bronze: dados crus")
    if bronze_df is None:
        st.info("Faça o upload para visualizar a Bronze.")
    else:
        st.write("Dimensões:", bronze_df.shape)
        st.dataframe(bronze_df.head(500))

with tab_silver:
    st.subheader("Silver: tratamentos padronizados + espaço para regras")
    if bronze_df is None:
        st.info("Faça o upload primeiro.")
    else:
        # Copia para evitar editar Bronze in-place
        silver = bronze_df.copy()

        # 1) Padroniza nomes de colunas
        silver = clean_columns(silver, keep_accents=keep_accents)

        # 2) Opcional: strip em strings
        if strip_strings:
            for c in silver.select_dtypes(include=["object", "string"]).columns:
                silver[c] = silver[c].astype("string").str.strip()

        # 3) Datas: se o usuário especificou a coluna, tenta converter
        if date_col.strip() and date_col.strip() in silver.columns:
            try:
                silver[date_col.strip()] = parse_date_series(
                    silver[date_col.strip()], fmt=date_fmt.strip() or None, dayfirst=dayfirst, infer=infer_dt
                )
            except Exception as e:
                st.warning(f"Falha ao converter data em '{date_col}': {e}")

        # 4) TODO: Regras específicas de normalização do Cortex
        #    - Ex.: mapear níveis de severidade, normalizar hostname, separar domínio/usuário,
        #           padronizar campos booleanos ("Yes/No" -> True/False), etc.
        #    Insira abaixo suas regras; mantenha o retorno do DataFrame 'silver'.

        # EXEMPLO (comente/remova quando não for necessário):
        # if 'severity' in silver.columns:
        #     map_sev = {'Informational': 0, 'Low': 1, 'Medium': 2, 'High': 3, 'Critical': 4}
        #     silver['severity_level'] = silver['severity'].map(map_sev).fillna(-1).astype(int)

        st.write("Dimensões:", silver.shape)
        st.dataframe(silver.head(500))

        # Guarda no estado
        st.session_state["silver_df"] = silver

with tab_gold:
    st.subheader("Gold: derivar visões/aplicações")
    silver = st.session_state.get("silver_df")
    if silver is None:
        st.info("Gere a Silver primeiro.")
    else:
        gold = silver.copy()

        # 1) Seleção de colunas
        if gold_choose_cols.strip():
            keep_cols = [c.strip() for c in gold_choose_cols.split(",") if c.strip()]
            keep_cols = [c for c in keep_cols if c in gold.columns]
            if keep_cols:
                gold = gold[keep_cols]

        # 2) Remoção de duplicados
        if gold_drop_dupes:
            gold = gold.drop_duplicates()

        # 3) Descartar NAs para subset
        if gold_dropna_subset.strip():
            subset_cols = [c.strip() for c in gold_dropna_subset.split(",") if c.strip()]
            subset_cols = [c for c in subset_cols if c in gold.columns]
            if subset_cols:
                gold = gold.dropna(subset=subset_cols)

        # 4) Filtro por data
        if gold_date_filter_on and date_col.strip() and date_col.strip() in gold.columns:
            try:
                if gold[date_col].dtype == "object":
                    gold[date_col] = pd.to_datetime(gold[date_col], errors="coerce")
                if gold_date_start.strip():
                    gold = gold[gold[date_col] >= pd.to_datetime(gold_date_start.strip(), errors="coerce")]
                if gold_date_end.strip():
                    gold = gold[gold[date_col] <= pd.to_datetime(gold_date_end.strip(), errors="coerce")]
            except Exception as e:
                st.warning(f"Falha no filtro de data: {e}")

        # 5) TODO: Outras regras de negócio para a(s) Gold
        #    - Ex.: agregações, enrichment, junções com tabelas de referência, etc.
        #    Insira abaixo suas regras (ou use a aba SQL).

        st.write("Dimensões:", gold.shape)
        st.dataframe(gold.head(500))

        # Export
        c1, c2 = st.columns(2)
        with c1:
            st.download_button("⬇️ Baixar Gold (CSV)", data=to_csv_bytes(gold), file_name="gold.csv", mime="text/csv")
        with c2:
            st.download_button("⬇️ Baixar Gold (Parquet)", data=to_parquet_bytes(gold), file_name="gold.parquet", mime="application/octet-stream")

        # Guarda no estado
        st.session_state["gold_df"] = gold

with tab_sql:
    st.subheader("SQL opcional (DuckDB in-memory)")
    st.caption("Você pode consultar **bronze**, **silver** e **gold** por SQL e criar novas visões.")
    silver = st.session_state.get("silver_df")
    gold = st.session_state.get("gold_df")
    # Nota: bronze_df pode ser pesado; use com parcimônia.
    con = duckdb.connect(database=":memory:")
    register_duckdb_tables(con, bronze=bronze_df, silver=silver, gold=gold)

    default_sql = """-- TODO: escreva aqui seu SQL. Exemplos:
-- SELECT * FROM silver LIMIT 100;
-- SELECT date_trunc('day', event_time) AS d, COUNT(*) AS n FROM silver GROUP BY 1 ORDER BY 1;
-- CREATE OR REPLACE TABLE gold_view AS SELECT * FROM silver WHERE severity_level >= 3;
"""
    sql_text = st.text_area("Escreva seu SQL", value=default_sql, height=220)
    run_sql = st.button("▶️ Executar SQL")

    if run_sql and sql_text.strip():
        try:
            res = con.execute(sql_text).fetchdf()
            st.success("Consulta executada.")
            st.dataframe(res.head(1000))
            st.download_button("⬇️ Baixar resultado (CSV)", data=to_csv_bytes(res), file_name="sql_result.csv", mime="text/csv")
        except Exception as e:
            st.error(f"Erro no SQL: {e}")

with tab_viz:
    st.subheader("Gráficos (Plotly)")
    st.caption("Escolha a tabela e o tipo de gráfico. Dica: defina a Silver primeiro para ter colunas padronizadas.")
    df_choice = st.selectbox("Tabela", ["bronze", "silver", "gold"])
    df_map = {
        "bronze": bronze_df,
        "silver": st.session_state.get("silver_df"),
        "gold": st.session_state.get("gold_df"),
    }
    viz_df = df_map[df_choice]

    if viz_df is None or len(viz_df) == 0:
        st.info("Nenhum dado disponível para plotar.")
    else:
        cols = viz_df.columns.tolist()
        kind = st.selectbox("Tipo de gráfico", ["Histograma", "Série temporal", "Barras (contagem por categoria)"])

        if kind == "Histograma":
            num_cols = [c for c in cols if pd.api.types.is_numeric_dtype(viz_df[c])]
            x = st.selectbox("Coluna numérica", num_cols if num_cols else cols)
            nb = st.slider("Bins", min_value=5, max_value=100, value=30)
            fig = px.histogram(viz_df, x=x, nbins=nb)
            st.plotly_chart(fig, use_container_width=True)

        elif kind == "Série temporal":
            dt_cols = [c for c in cols if pd.api.types.is_datetime64_any_dtype(viz_df[c])]
            if not dt_cols:
                st.warning("Nenhuma coluna datetime detectada. Converta na Silver (campo de data).")
            else:
                tcol = st.selectbox("Coluna de data/hora", dt_cols)
                y_candidates = [c for c in cols if pd.api.types.is_numeric_dtype(viz_df[c])]
                y = st.selectbox("Métrica (numérica) para somar por dia", y_candidates) if y_candidates else None
                if y:
                    tmp = viz_df.copy()
                    tmp["_d"] = tmp[tcol].dt.date
                    agg = tmp.groupby("_d")[y].sum().reset_index()
                    fig = px.line(agg, x="_d", y=y, markers=True)
                    st.plotly_chart(fig, use_container_width=True)

        elif kind == "Barras (contagem por categoria)":
            cat_cols = [c for c in cols if pd.api.types.is_string_dtype(viz_df[c]) or viz_df[c].dtype.name == "category"]
            x = st.selectbox("Coluna categórica", cat_cols if cat_cols else cols)
            topn = st.slider("Top N categorias", min_value=5, max_value=50, value=15)
            counts = viz_df[x].value_counts(dropna=False).head(topn).reset_index()
            counts.columns = [x, "count"]
            fig = px.bar(counts, x=x, y="count")
            st.plotly_chart(fig, use_container_width=True)

st.markdown("---")
with st.expander("📌 Como usar / Dicas"):
    st.markdown(
        """
1. Faça upload do arquivo do Cortex (CSV/XLSX) na **sidebar** (Bronze).
2. Configure as opções de **Silver**: padronize colunas, escolha a coluna e formato da **data** (string → datetime).
3. Em **Gold**, selecione colunas, remova duplicados, filtre por data e aplique as **regras de negócio** (há espaço para *TODOs*).
4. Use a aba **SQL** para escrever queries com DuckDB sobre `bronze`, `silver` e `gold`.
5. Na aba **Gráficos**, gere visualizações rápidas com Plotly.
6. Exporte resultados em CSV/Parquet.
        """
    )










































# --..                        ++..--..              ::::      
# ..                      --MM@@@@MMMMMMMM..      ::::::      
#   ..                  mmmmmmMM@@MMMM@@@@@@++    ::::..      
# ..--                MM++------------::++@@@@    --::        
#   --              MM::--......----------::MM++..::--        
#   --            --MM........----------::--::::              
#   --            mm++......------------::::::::              
#   --            mm::--------------------::::--              
#   --            ++::--------------------------              
#   --            ++::--..--------------------::            --
#   --            ++----mm@@####MM++::::::::::--          --::
#   ..            mm--::MMmmmmMMMMMM++++@@####MM        --::::
#   ..            ++--++@@@@##@@MM++::mm@@@@mmMM      --::::::
#               mm::----::mmmmmmMM----MM@@####mm    ..::::++++
#     ..        ++::------::++++------++mmmmmm++  ..--::++++++
#     ..        --::::::::::----------++++++++::..::::++++mmmm
#     ..    ..    ::::::::----::::----::::::++++::::++++mmmmmm
#     ..          ::::::::::::::::mm++++++::++++++++++mmmmMMMM
#     ..          ::::::::::::::++++mmmm++++++++++mmmmmmMM@@@@
#     --            ::::::::::::::::++++++++++@@mmmmMMMM@@@@@@
#   ..::          ..++::::::::++++mmmmMM++++++@@mmMMMM@@@@@@@@
# ....::      ::++::++++++::++mmmmmmmmmmmmmmmm@@MMMM@@@@@@@@@@
#   ..::  --::mmmmmm++++++mmmm++mmmmmmmmmmmmmm@@MMMM@@@@@@@@@@
#     ..--++++##mm@@++mmmmmmmm++::::++mmmm@@##@@mmmmmm##@@MMmm
#   ::::MMMMmm######::++MMMMMM++++mmmmmm@@######@@MM@@##MM    
# ++::::++mm@@######MM++mmMMMM@@@@@@@@@@##########@@@@@@++    
# ++++MM##MM@@########++++mm@@@@@@MMMM##################@@MM::
# MMmm++MM@@MM##########mmmmmmMM@@MM##########################
# mmmm@@mmMMMMMM##########mmmmmmmmmm##########################
# ##@@MM@@@@@@MM############mmmmmm############################