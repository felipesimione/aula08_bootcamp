from etl import pipeline_calcular_kpi_vendas_consolidado


pasta_arqgumento: str = 'data'
formato_de_saida: list = ['csv', 'parquet']

pipeline_calcular_kpi_vendas_consolidado(pasta_arqgumento, formato_de_saida)