from leitura_dados import ler_dados
from tratamento_dados import tratar_dados
from pyspark.sql.functions import sum, avg, count, desc

caminho_csv = "dados/pagamentos.csv"

df = ler_dados(caminho_csv)

df_tratado = tratar_dados(df)

df_tratado.show(5)
df_tratado.printSchema()

df_tratado.agg(
    sum("valor_parcela").alias("total_pago"),
    avg("valor_parcela").alias("media_pagamento")
).show()

ranking_favorecido = df_tratado.groupBy("cpf_favorecido", "nome_favorecido")\
    .agg(
        sum("valor_parcela").alias("valor_total_acumulado"),
        count("valor_parcela").alias("quantidade_parcelas")
    )\
    .orderBy(desc("valor_total_acumulado"))

print("Ranking dos 5 primeiros com valores acumulados")
ranking_favorecido.show(5, truncate=False)

def analise_parcelas_por_pessoa(df, cpf):
    df_pessoa = (
        df.filter(df.cpf_favorecido == cpf)\
        .orderBy("ano_competencia", "mes_competencia")
    )
    resumo = ( 
        df_pessoa.agg (
            count("*").alias("quantidade_parcelas"),
            sum("valor_parcela").alias("total_recebido"),
            avg("avlor_parcela").alias("valor_medio_parcela")
        )
    )
    return df_pessoa , resumo
cpf_exemplo = "***.600.238-**"
df_pessoa , resumo_pessoa = analise_parcelas_por_pessoa(df_tratado, cpf_exemplo)
df_pessoa.show()
resumo_pessoa.show

