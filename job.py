from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def executar_pipeline_clientes(tabela_origem: str, tabela_destino: str):
    """
    Executa o Data Prep da tabela de clientes do Banco Aurora:
    - Leitura dos dados brutos
    - Arredondamento e limpeza de strings
    - Imputação de nulos
    - Salvamento no catálogo
    """
    spark = SparkSession.builder.getOrCreate()
    print(f"🚀 Iniciando processamento de: {tabela_origem}")



    # Garante que o schema de destino existe antes da gravação
    schema_destino = tabela_destino.split(".")[0]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_destino}")

        print(f"✅ Schema '{schema_destino}' garantido.")

        

    # 1. LEITURA (Extract)
    df = spark.table(tabela_origem)

    # 2. TRATAMENTO E REGRAS DE NEGÓCIO (Transform)
    df_tratado = (
        df
        # Deduplicação
        .dropDuplicates(["cliente_id"])
        
        # Padronização de textos e documentos
        .withColumn("nome", F.trim(F.upper(F.col("nome"))))
        .withColumn("cpf_limpo", F.regexp_replace(F.col("cpf"), r"[^\d]", ""))
        
        # Arredondamentos e Nulos
        .withColumn("renda_mensal", F.round(F.coalesce(F.col("renda_mensal"), F.lit(0.0)), 2))
        .withColumn("score_credito", F.coalesce(F.col("score_credito"), F.lit(0)))
        
        # Coluna de controle (Audit) para saber quando o registro foi processado
        .withColumn("data_processamento", F.current_timestamp())
    )

    # 3. CARGA E GRAVAÇÃO (Load)
    (
        df_tratado.write
        .mode("overwrite")                  # Ou "append" se for carga incremental
        .option("overwriteSchema", "true")  # Atualiza a estrutura no catálogo caso mude
        .saveAsTable(tabela_destino)
    )

    print(f"✅ Tabela salva com sucesso em: {tabela_destino}")

# Execução do pipeline
executar_pipeline_clientes(
    tabela_origem="banco_aurora_clientes",
    tabela_destino="workspace_db.tb_clientes_silver"
)
