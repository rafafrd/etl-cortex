# -*- coding: utf-8 -*-
"""
Unificação de planilhas do Cortex (XDR) no Streamlit
Funcionalidades:
- Upload de 1..N arquivos .xlsx exportados do Cortex;
- Detecção automática da linha de cabeçalho;
- Padronização de nomes de colunas (snake_case);
- Normalização de datas e status;
- Extração do primeiro IPv4/IPv6 quando vierem múltiplos na mesma célula;
- Deduplicação mantendo o registro mais recente (por last_seen / last_upgrade_status_time);
- Resumos por endpoint_status e operating_system;
- Exportação Excel com múltiplas abas (Base_Limpa, Resumo_Status, Resumo_OS, Falhas_Upgrade)
  com formatação amigável (freeze panes, largura auto, num_format).

Dependências: streamlit, pandas, openpyxl (leitura), XlsxWriter (escrita recomendada).
"""

from __future__ import annotations

import io
import re
from datetime import datetime
from typing import Dict, Tuple

import numpy as np
import pandas as pd
import streamlit as st
from pandas.api.types import is_float_dtype, is_integer_dtype


# ==============================
# Helpers de Exportação (XLSX)
# ==============================

def df_to_xlsx_bytes(
    sheets: Dict[str, pd.DataFrame],
    sample_for_width: int = 1000,
    float_format: str = "#,##0.00",
    int_format: str = "#,##0",
) -> bytes:
    """
    Gera um .xlsx em memória, com múltiplas abas:
      - Congela a 1ª linha;
      - Ajusta largura automaticamente (amostra até sample_for_width linhas);
      - Formata floats/inteiros com os formatos fornecidos.

    Tenta usar XlsxWriter (melhor para col formats). Se não disponível, usa openpyxl.
    """
    output = io.BytesIO()

    # 1ª tentativa: XlsxWriter
    try:
        with pd.ExcelWriter(output, engine="xlsxwriter", datetime_format="yyyy-mm-dd HH:MM:SS") as writer:
            for name, df in sheets.items():
                sheet = (name or "Sheet1")[:31]
                _df = df.copy()

                # Achata MultiIndex em colunas (ex.: ("Métrica","Status","Base"))
                if isinstance(_df.columns, pd.MultiIndex):
                    _df.columns = [" - ".join(map(str, tup)).strip() for tup in _df.columns]

                # Se tiver index nomeado, leva para colunas
                if _df.index.names is not None and any(n is not None for n in _df.index.names):
                    _df = _df.reset_index()

                _df.to_excel(writer, sheet_name=sheet, index=False)

                workbook = writer.book
                worksheet = writer.sheets[sheet]

                # Congela a 1ª linha (cabeçalho)
                worksheet.freeze_panes(1, 0)

                # Formatos de número
                fmt_float = workbook.add_format({"num_format": float_format})
                fmt_int = workbook.add_format({"num_format": int_format})

                # Largura automática por coluna
                n = min(len(_df), sample_for_width)
                sample_df = _df.head(n)

                for col_idx, col_name in enumerate(_df.columns):
                    series = sample_df[col_name]
                    header_len = len(str(col_name))

                    # Aplicar formatação e estimar largura
                    if is_float_dtype(_df[col_name]):
                        worksheet.set_column(col_idx, col_idx, None, fmt_float)
                        formatted = series.dropna().map(lambda x: f"{x:,.2f}")
                        data_len = formatted.map(len).max() if not formatted.empty else 0
                    elif is_integer_dtype(_df[col_name]):
                        worksheet.set_column(col_idx, col_idx, None, fmt_int)
                        formatted = series.dropna().map(lambda x: f"{int(x):,d}")
                        data_len = formatted.map(len).max() if not formatted.empty else 0
                    else:
                        data_len = series.astype(str).map(len).max() if not series.empty else 0

                    best = min(max(header_len, int(data_len or 0)) + 2, 60)
                    worksheet.set_column(col_idx, col_idx, best)

        return output.getvalue()

    except Exception:
        # Fallback: openpyxl
        from openpyxl.utils import get_column_letter
        with pd.ExcelWriter(output, engine="openpyxl", datetime_format="yyyy-mm-dd HH:MM:SS") as writer:
            for name, df in sheets.items():
                sheet = (name or "Sheet1")[:31]
                _df = df.copy()

                if isinstance(_df.columns, pd.MultiIndex):
                    _df.columns = [" - ".join(map(str, tup)).strip() for tup in _df.columns]

                if _df.index.names is not None and any(n is not None for n in _df.index.names):
                    _df = _df.reset_index()

                _df.to_excel(writer, sheet_name=sheet, index=False)
                ws = writer.sheets[sheet]

                # Congela a 1ª linha
                ws.freeze_panes = "A2"

                # Largura automática (amostra)
                n = min(len(_df), sample_for_width)
                sample_df = _df.head(n)

                for col_idx, col_name in enumerate(_df.columns, start=1):
                    header_len = len(str(col_name))
                    series = sample_df[col_name]
                    data_len = series.astype(str).map(len).max() if not series.empty else 0
                    best = min(max(header_len, int(data_len or 0)) + 2, 60)
                    ws.column_dimensions[get_column_letter(col_idx)].width = best

        return output.getvalue()


# ==============================
# Parsing / Limpeza Cortex
# ==============================

_CANDIDATE_COLS = {
    "Endpoint Name",
    "Endpoint Type",
    "Operating System",
    "Agent Version",
}

def _norm_col(c: str) -> str:
    c = str(c).strip()
    c = re.sub(r"[^0-9A-Za-z]+", "_", c)
    c = re.sub(r"_+", "_", c).strip("_").lower()
    return c

def detect_header_index(raw: pd.DataFrame) -> int:
    """
    Detecta a linha do header pelo conjunto de colunas típicas do export do Cortex.
    Se não achar, retorna 1 (fallback comum quando há um título na primeira linha).
    """
    for i, row in raw.iterrows():
        vals = set(str(x).strip() for x in row.dropna().tolist())
        if _CANDIDATE_COLS.issubset(vals):
            return i
    return 1

def parse_cortex_excel(file_like) -> Tuple[pd.DataFrame, int]:
    """
    Lê um XLSX do Cortex, detecta header, padroniza colunas/tipos e retorna (df, header_row_idx).
    """
    # Lê sem header para poder detectar
    raw = pd.read_excel(file_like, sheet_name=0, header=None, engine="openpyxl")

    header_row_idx = detect_header_index(raw)
    hdr = raw.iloc[header_row_idx].tolist()
    df = raw.iloc[header_row_idx + 1:].copy()
    df.columns = hdr

    # Remove colunas e linhas totalmente vazias
    df = df.dropna(axis=1, how="all").dropna(how="all")

    # Padroniza nomes
    df.columns = [_norm_col(c) for c in df.columns]

    # Converte datas
    for col in ("last_seen", "last_upgrade_status_time"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Normaliza status (title case)
    if "endpoint_status" in df.columns:
        df["endpoint_status"] = (
            df["endpoint_status"].astype(str).str.strip().str.title()
        )

    # Extrai 1º IPv4 e 1º IPv6 se houverem múltiplos por célula
    ipv4_re = re.compile(r"\b(\d{1,3}(?:\.\d{1,3}){3})\b")

    if "ip_address" in df.columns:
        def _first_ipv4(x):
            if pd.isna(x):
                return np.nan
            m = ipv4_re.search(str(x))
            return m.group(1) if m else np.nan
        df["ipv4"] = df["ip_address"].apply(_first_ipv4)

    if "ipv6_address" in df.columns:
        def _first_ipv6(x):
            if pd.isna(x):
                return np.nan
            # captura a primeira sequência que contenha ':'
            for part in [p.strip() for p in str(x).split(",") if p.strip()]:
                if ":" in part:
                    return part
            return np.nan
        df["ipv6"] = df["ipv6_address"].apply(_first_ipv6)

    return df, int(header_row_idx)


def unify_cortex(
    df: pd.DataFrame,
    dedup_on=("endpoint_name", "endpoint_alias"),
) -> Dict[str, pd.DataFrame]:
    """
    Deduplica mantendo o registro mais recente (por last_seen / last_upgrade_status_time),
    e produz resumos.

    Retorna dict de abas:
      - Base_Limpa
      - Resumo_Status (se disponível)
      - Resumo_OS (se disponível)
      - Falhas_Upgrade (se houver)
    """
    df_in = df.copy()

    # Dedup: mais recente vence
    if all(c in df_in.columns for c in dedup_on):
        sort_cols = [c for c in ("last_seen", "last_upgrade_status_time") if c in df_in.columns]
        if sort_cols:
            df_in = df_in.sort_values(sort_cols, ascending=False)
        base_limpa = df_in.drop_duplicates(subset=list(dedup_on), keep="first")
    else:
        base_limpa = df_in.drop_duplicates()

    # Resumos
    if "endpoint_status" in base_limpa.columns:
        resumo_status = (
            base_limpa.groupby("endpoint_status", dropna=False)
            .size().reset_index(name="qtd")
            .sort_values("qtd", ascending=False)
        )
    else:
        resumo_status = pd.DataFrame()

    if "operating_system" in base_limpa.columns:
        resumo_os = (
            base_limpa.groupby("operating_system", dropna=False)
            .size().reset_index(name="qtd")
            .sort_values("qtd", ascending=False)
        )
    else:
        resumo_os = pd.DataFrame()

    # Falhas de upgrade
    fail_mask = pd.Series(False, index=base_limpa.index)
    for col in ("last_upgrade_status", "last_upgrade_failure_reason"):
        if col in base_limpa.columns:
            fail_mask = fail_mask | base_limpa[col].astype(str).str.lower().str.contains(
                "fail|timed out|faulty|lost|error", na=False
            )
    falhas_upg = base_limpa[fail_mask].copy()

    sheets = {"Base_Limpa": base_limpa}
    if not resumo_status.empty:
        sheets["Resumo_Status"] = resumo_status
    if not resumo_os.empty:
        sheets["Resumo_OS"] = resumo_os
    if not falhas_upg.empty:
        sheets["Falhas_Upgrade"] = falhas_upg

    return sheets


# ==============================
# UI - Streamlit
# ==============================

st.set_page_config(page_title="Unificação Cortex XLSX", layout="wide")

with st.sidebar:
    st.title("Unificação – Cortex XLSX")
    st.caption("Faça upload de export(s) do Cortex (XDR) em formato .xlsx para unificar e gerar um Excel final.")
    st.markdown("**Dica**: mantenha `XlsxWriter` instalado para melhor formatação do Excel final.")

st.header("Unificação de Planilhas – Cortex")
st.write("Envie um ou mais arquivos `.xlsx` exportados do Cortex. O app detecta o cabeçalho, limpa e unifica.")

uploads = st.file_uploader(
    "Arraste e solte ou selecione os arquivos",
    type=["xlsx"],
    accept_multiple_files=True,
    help="Você pode enviar vários exports e o app irá consolidá-los.",
)

dedup_key = st.multiselect(
    "Chaves para deduplicação (mantém o registro mais recente)",
    ["endpoint_name", "endpoint_alias", "operating_system"],
    default=["endpoint_name", "endpoint_alias"],
    help="Use pelo menos uma chave que identifique o endpoint.",
)

processar = st.button("⚡ Processar", type="primary", use_container_width=True)

if not uploads:
    st.info("Envie ao menos 1 arquivo `.xlsx` para iniciar.")
elif processar:
    # Parseia todos e concatena
    with st.spinner("Lendo e normalizando os arquivos..."):
        parsed_dfs = []
        header_rows = []
        for up in uploads:
            df_parsed, hdr_idx = parse_cortex_excel(up)
            parsed_dfs.append(df_parsed)
            header_rows.append(hdr_idx)

        base = pd.concat(parsed_dfs, ignore_index=True) if len(parsed_dfs) > 1 else parsed_dfs[0]

        # Info rápida
        total_linhas = len(base)
        st.success(f"Arquivos lidos: **{len(uploads)}** · Linhas combinadas: **{total_linhas:,}** · Headers detectados: {header_rows}")

    with st.spinner("Unificando, deduplicando e gerando resumos..."):
        sheets = unify_cortex(base, dedup_on=tuple(dedup_key))
        base_limpa = sheets.get("Base_Limpa", pd.DataFrame())
        resumo_status = sheets.get("Resumo_Status", pd.DataFrame())
        resumo_os = sheets.get("Resumo_OS", pd.DataFrame())
        falhas_upg = sheets.get("Falhas_Upgrade", pd.DataFrame())

    # Exibição
    st.subheader("Base Limpa (deduplicada)")
    st.dataframe(base_limpa, use_container_width=True, height=450)

    cols = st.columns(3)
    with cols[0]:
        st.subheader("Resumo – Status")
        if resumo_status.empty:
            st.caption("_Sem coluna `endpoint_status` na base._")
        else:
            st.dataframe(resumo_status, use_container_width=True, height=280)

    with cols[1]:
        st.subheader("Resumo – Sistema Operacional")
        if resumo_os.empty:
            st.caption("_Sem coluna `operating_system` na base._")
        else:
            st.dataframe(resumo_os, use_container_width=True, height=280)

    with cols[2]:
        st.subheader("Falhas de Upgrade")
        if falhas_upg.empty:
            st.caption("_Nenhuma falha detectada com as palavras-chave (fail, timed out, faulty, lost, error)._")
        else:
            st.dataframe(falhas_upg, use_container_width=True, height=280)

    # Downloads
    st.divider()
    st.subheader("Exportar")
    xlsx_bytes = df_to_xlsx_bytes(sheets)
    st.download_button(
        "⬇️ Baixar Excel Unificado (XLSX)",
        data=xlsx_bytes,
        file_name=f"cortex_unificado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    # Footer
    st.caption(
        f"Relatório gerado em {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}. "
        "Caso precise de novas abas ou outro critério de dedup, me diga e eu ajusto."
    )
