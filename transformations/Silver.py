# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Silver
# MAGIC Pega as tabelas bronze, limpa, adiciona colunas de uso e devolve.

# COMMAND ----------

# DBTITLE 1,Imports e Variáveis
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DecimalType

# COMMAND ----------

# DBTITLE 1,Definição da Tabela

def tratar_tabela(nome):
    df = spark.read.table(f"kairos.bronze.{nome}")

    # Limpeza de texto nas colunas conhecidas
    colunas_texto = {
        "companies": ["name", "cnpj", "email", "plan"],
        "users": ["name", "last_name", "email"],
        "sectors": ["name", "type"],
        "products": ["brand", "name"],
        "categories": ["category"],
        "alerts": ["description", "status"],
        "purchases": ["status"],
        "audits": ["table_name", "operation", "users"],
    }

    for campo in colunas_texto.get(nome, []):
        df = df.withColumn(
            campo,
            F.when(
                F.trim(F.col(campo)) == "",
                F.lit(None).cast("string")
            ).otherwise(F.trim(F.col(campo)))
        )

    # Conversão de valores monetários
    if nome == "products":
        df = df.withColumn(
            "price",
            F.regexp_replace(F.col("price"), r"[^0-9.-]", "")
            .cast(DecimalType(18, 2))
        )

    if nome == "purchase_products":
        df = df.withColumn(
            "amount",
            F.regexp_replace(F.col("amount"), r"[^0-9.-]", "")
            .cast(DecimalType(18, 2))
        )

    # Conversão das quantidades
    colunas_inteiras = [
        "product_quantity",
        "restocked_quantity",
        "qnt_avarias",
        "quantity",
        "maximum_capacity",
    ]

    tabelas_quantidades = {
        "product_shelves",
        "product_inventories",
        "restockings",
        "purchase_products",
        "shelves",
    }

    if nome in tabelas_quantidades:
        for campo in colunas_inteiras:
            # Os campos são aplicados apenas nas tabelas
            # em que sabemos que existem
            if campo == "product_quantity" and nome in {
                "product_shelves", "product_inventories"
            }:
                df = df.withColumn(campo, F.col(campo).cast("long"))

            elif campo in {"restocked_quantity", "qnt_avarias"} and nome == "restockings":
                df = df.withColumn(campo, F.col(campo).cast("long"))

            elif campo == "quantity" and nome == "purchase_products":
                df = df.withColumn(campo, F.col(campo).cast("long"))

            elif campo == "maximum_capacity" and nome == "shelves":
                df = df.withColumn(campo, F.col(campo).cast("long"))

    # Padronização dos status
    if nome in {"alerts", "purchases"}:
        df = df.withColumn(
            "status",
            F.upper(F.trim(F.col("status")))
        )

    return df



@dp.materialized_view(name="kairos.silver.employees")
def silver_employees():
    return tratar_tabela("employees")


@dp.materialized_view(name="kairos.silver.customers")
def silver_customers():
    return tratar_tabela("customers")


# =========================
# ESTRUTURA DA LOJA
# =========================

@dp.materialized_view(name="kairos.silver.sectors")
def silver_sectors():
    return tratar_tabela("sectors")


@dp.materialized_view(name="kairos.silver.aisles")
def silver_aisles():
    return tratar_tabela("aisles")


@dp.materialized_view(name="kairos.silver.shelves")
def silver_shelves():
    return tratar_tabela("shelves")


@dp.materialized_view(name="kairos.silver.inventories")
def silver_inventories():
    return tratar_tabela("inventories")


@dp.materialized_view(name="kairos.silver.inventory_employees")
def silver_inventory_employees():
    return tratar_tabela("inventory_employees")


@dp.materialized_view(name="kairos.silver.shelf_employees")
def silver_shelf_employees():
    return tratar_tabela("shelf_employees")


@dp.materialized_view(name="kairos.silver.employee_aisles")
def silver_employee_aisles():
    return tratar_tabela("employee_aisles")


# =========================
# PRODUTOS E ESTOQUE
# =========================

@dp.materialized_view(name="kairos.silver.categories")
def silver_categories():
    return tratar_tabela("categories")


@dp.materialized_view(name="kairos.silver.products")
def silver_products():
    return tratar_tabela("products")


@dp.materialized_view(name="kairos.silver.product_shelves")
def silver_product_shelves():
    return tratar_tabela("product_shelves")


@dp.materialized_view(name="kairos.silver.product_inventories")
def silver_product_inventories():
    return tratar_tabela("product_inventories")


# =========================
# OPERAÇÕES DE ESTOQUE
# =========================

@dp.materialized_view(name="kairos.silver.restockings")
def silver_restockings():
    return tratar_tabela("restockings")


@dp.materialized_view(name="kairos.silver.alerts")
def silver_alerts():
    return tratar_tabela("alerts")


# =========================
# VENDAS
# =========================

@dp.materialized_view(name="kairos.silver.purchases")
def silver_purchases():
    return tratar_tabela("purchases")


@dp.materialized_view(name="kairos.silver.purchase_products")
def silver_purchase_products():
    return tratar_tabela("purchase_products")


# =========================
# AUDITORIA
# =========================

@dp.materialized_view(name="kairos.silver.audits")
def silver_audits():
    return tratar_tabela("audits")
