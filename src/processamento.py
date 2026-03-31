from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, sum, avg, count, desc

spark = SparkSession.builder\
.appName("Bolsa Familia")\
.getOrCreate()

caminho_csv = "dados/pagamentos.csv"

df = spark.read\
.option("header", True)\
.option("InferSchema", True)\
.option("sep", ";")\
.option("encoding","ISO-8859-1")\
.csv(caminho_csv)

df.show(3)
df.printSchema(3)

#PADRONIZAÇÃO DE DADOS

df_tratado = df.withColumnRenamed(\
    "MÊS COMPETÊNCIA", "mes_competencia"
    )

#PADRONIZAÇÃO DE DADOS DE TODAS AS COLUNAS - USADO PARA POUCAS COLUNAS

df_tratado = df_tratado.withColumnRenamed(\
    "MÊS REFERÊNCIA" , "mes_referencia")\
    .withColumnRenamed(\
    "UF" , "uf")\
    .withColumnRenamed(\
    "CÓDIGO MUNICÍPIO SIAFI" , "codigo_municipio_siafi")\
    .withColumnRenamed(\
    "NOME MUNICÍPIO" , "nome_municipio")\
    .withColumnRenamed(\
    "CPF FAVORECIDO" , "cpf_favorecido")\
    .withColumnRenamed(\
    "NIS FAVORECIDO" , "nis_favorecido")\
    .withColumnRenamed(\
    "NOME FAVORECIDO" , "nome_favorecido")\
    .withColumnRenamed(\
    "VALOR PARCELA" , "valor_parcela")

df_tratado.show()

#DEVOLVENDO A CONFIGURAÇÃO ORIGINAL
df_tratado = df

#df_tratado.show(5)

#CRIANDO UM DICIONARIO PARA RENOMEAR VARIAS COLUNAS DE UMA VEZ
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

df_tratado.show()

#TRATAMENTO AUTOMÁTICO
import unicodedata, re

def padronizar_nome(col):
    col = col.lower()#Transforma tudo para minúsculo
    col = unicodedata.normalize("NFD", col)#Separa todos os caracteres especiais dos caracteres comuns
    col = col.encode("ascii", "ignore").decode("utf-8")#Ignorando a codificação ASCI e transformando em UTF-8, e ignora os caravteres especiais
    col = re.sub(r"[^a-z0-9]+","_", col)#Aonde estiver espaço em branco trocar para "_"
    col = col.strip("_")#Identifica espaços em branco no início e substitui por "_"
    return col

df_tratado = df_tratado.toDF(
    *[padronizar_nome(c) for c in df_tratado.columns]
)

#Apaga valores nulos de uma tabela especifica
###df_tratado = df_tratado.dropna(subset=["valor_parcela"])

#Apaga valores nulos da tabela inteira
df_tratado = df_tratado.dropna()

#Troca de "," por "."
df_tratado = df_tratado.withColumn(
    #Na coluna valor parcela, vai trocar o caractere "," por "."
    "valor_parcela",
    regexp_replace(col("Valor_parcela"), "," , ".")
)

#Convertendo a coluna Valor Parcela de STRING para o tipo DECIMAL
df_tratado = df_tratado.withColumn(
    #Na coluna valor parcela, vai trocar o caractere "," por "."
    "valor_parcela",
    col("Valor_parcela").cast("decimal(10,2)")
)

#Calculando a somatoria do valor pago e tirando a média do valor pago
df_tratado.agg(
    sum("valor_parcela").alias("total_pago"),
    avg("valor_parcela").alias("media_pagamento")
).show()

df_tratado.groupBy("uf")\
    .agg(sum("valor_parcela").alias("total_pago"))\
    .orderBy("total_pago", ascending=False)\
    .show(10)

ranking_favorecido = df_tratado.groupBy("cpf_favorecido", "nome_favorecido")\
    .agg(
        sum("valor_parcela").alias("valor_total_acumulado"),
        count("valor_parcela").alias("quantidade_parcelas")
    )\
    .orderBy(desc("valor_total_acumulado"))