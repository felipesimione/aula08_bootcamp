import pandas as pd
import os
import glob
from log import log_decorator

class ExtractDataframe:
    @log_decorator
    def extrair_dados_e_consolidar(self, pasta: str) -> pd.DataFrame:
        arquivos_json = glob.glob(os.path.join(pasta, '*.json'))
        df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
        df_total = pd.concat(df_list, ignore_index=True)
        return df_total

class EtlCalculator(ExtractDataframe):
    def __init__(self, fonte_dados: pd.DataFrame):
        self.df = fonte_dados

    @log_decorator
    def calcular_kpi_total_vendas(self) -> pd.DataFrame:
        self.df["Total"] = self.df["Quantidade"] * self.df["Venda"]
        return self.df

    @log_decorator
    def carregar_dados(self, format_saida: list):
        for formato in format_saida:
            if formato == 'csv':
                self.df.to_csv('dados.csv', index=False)
            elif formato == 'parquet':
                self.df.to_parquet('dados.parquet', index=False)

class PipelineCalculator:
    @log_decorator
    def pipeline_calcular_kpi_vendas_consolidado(self, pasta_argumento: str, formato_de_saida: list):
        extractor = ExtractDataframe()
        data_frame = extractor.extrair_dados_e_consolidar(pasta_argumento)
        etl = EtlCalculator(data_frame)
        data_frame_calculado = etl.calcular_kpi_total_vendas()
        etl.carregar_dados(formato_de_saida)