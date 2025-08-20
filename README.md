# 📊 Cortex ETL – Bronze ➜ Silver ➜ Gold

Este projeto é uma aplicação em **Streamlit** que processa planilhas exportadas do **antivírus Cortex**, aplicando um fluxo de tratamento inspirado no modelo **Medalhão de Dados (Medallion Architecture)**:

- **Bronze** → Dados crus, extraídos diretamente da planilha.
- **Silver** → Dados tratados, com padronização de colunas, normalização de strings, conversão de datas e outros ajustes.
- **Gold** → Dados prontos para consumo, com regras de negócio aplicadas (deduplicação, filtros, enriquecimento, queries SQL, etc.).

O sistema também permite gerar **gráficos interativos com Plotly**, além de exportar os resultados em **CSV** ou **Parquet**.

---

## 🚀 Tecnologias Utilizadas

- [**Streamlit**](https://streamlit.io/) – Interface interativa para upload, transformação e visualização de dados.
- [**Pandas**](https://pandas.pydata.org/) – Manipulação e limpeza dos dados.
- [**DuckDB**](https://duckdb.org/) – Execução de SQL em memória sobre as tabelas Bronze/Silver/Gold.
- [**Plotly**](https://plotly.com/python/) – Visualizações interativas.
- [**NumPy**](https://numpy.org/) – Operações numéricas de apoio.
- [**OpenPyXL**](https://openpyxl.readthedocs.io/) – Leitura de planilhas Excel.
- [**PyArrow**](https://arrow.apache.org/docs/python/) – Exportação em Parquet.

---

## ⚙️ Funcionalidades

- **Upload** de planilhas (`.csv` ou `.xlsx`) exportadas pelo Cortex.
- **Bronze**: dados crus exibidos para inspeção.
- **Silver**:
  - Padronização de nomes de colunas.
  - Remoção de espaços extras em strings.
  - Conversão de datas em formato de string para `datetime`.
  - Espaços reservados (`TODO`) para inserir regras de negócio específicas.
- **Gold**:
  - Seleção de colunas.
  - Remoção de duplicados.
  - Filtros por colunas nulas.
  - Filtros por faixa de datas.
  - Exportação para CSV ou Parquet.
- **SQL opcional (DuckDB)**: rodar queries sobre `bronze`, `silver` e `gold`.
- **Visualizações (Plotly)**:
  - Histogramas.
  - Séries temporais.
  - Contagem por categorias (gráficos de barras).

---

