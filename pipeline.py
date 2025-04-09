from etl import PipelineCalculator


pasta_argumento: str = 'data'
formato_de_saida: list = ['csv', 'parquet']


pipeline = PipelineCalculator()
pipeline.pipeline_calcular_kpi_vendas_consolidado(pasta_argumento, formato_de_saida)