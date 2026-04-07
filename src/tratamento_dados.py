from pyspark.sql.functions import col, regexp_replace, substring

def tratar_dados(df):
    df_tratado = df

    colunas_padrao = {
    "MÊS COMPETÊNCIA": "mes_competencia",
    "MÊS REFERÊNCIA": "mes_referencia",
    "UF": "uf",
    "CÓDIGO MUNICÍPIO SIAFI": "codigo_municipio_siafi",
    "NOME MUNICÍPIO": "nome_municipio",
    "CPF FAVORECIDO": "cpf_favorecido",
    "NIS FAVORECIDO": "nis_favorecido",
    "NOME FAVORECIDO": "nome_favorecido",
    "VALOR PARCELA": "valor_parcela"
    }

    for antiga, nova in colunas_padrao.items():
        df_tratado = df_tratado.withColumnRenamed(antiga,nova)

    

    df_tratado = (
    df_tratado
    .dropna()
    .withColumn(
    #Na coluna valor parcela, vai trocar o caractere "," por "."
    "valor_parcela",
    regexp_replace(col("Valor_parcela"), "," , "."))
    .withColumn(
    #Na coluna valor parcela, vai trocar o caractere "," por "."
    "valor_parcela",
    col("Valor_parcela").cast("decimal(10,2)"))
    .withColumn(
    "ano_competencia",
    substring(col("data_compertencia"),1,4)
    )
    .withColumn(
    "mes_competencia",
    substring(col("data_compertencia"),5,2)
    )
    )

    return df_tratado