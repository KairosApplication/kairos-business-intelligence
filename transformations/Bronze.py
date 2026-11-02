# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Bronze
# MAGIC Lê o banco e passa ele pro workspace.

# COMMAND ----------

# DBTITLE 1,Imports e Variáveis
from pyspark import pipelines as dp

HOST = "placeholder"
PORT = "placeholder"
BANCO = "placeholder"
URL = f"jdbc:postgresql://{HOST}:{PORT}/{BANCO}"

ESCOPO = "postgres-secrets"
CHAVE_USUARIO = "username"
CHAVE_SENHA = "password"
USUARIO = dbutils.secrets.get(ESCOPO, CHAVE_USUARIO)
SENHA = dbutils.secrets.get(ESCOPO, CHAVE_SENHA)

# COMMAND ----------

# DBTITLE 1,Definição da Tabela
def ler_postgres(tabela):
    return (
        spark.read.format("jdbc")
        .option("url", URL)
        .option("dbtable", f"public.{tabela}")
        .option("user", USUARIO)
        .option("password", SENHA)
        .option("driver", "org.postgresql.Driver")
        .load()
    )



@dp.materialized_view(name="kairos.bronze.users")
def bronze_users():
    return ler_postgres("users")


@dp.materialized_view(name="kairos.bronze.employees")
def bronze_employees():
    return ler_postgres("employees")


@dp.materialized_view(name="kairos.bronze.inventory_employees")
def bronze_inventory_employees():
    return ler_postgres("inventory_employees")


@dp.materialized_view(name="kairos.bronze.shelf_employees")
def bronze_shelf_employees():
    return ler_postgres("shelf_employees")


@dp.materialized_view(name="kairos.bronze.employee_aisles")
def bronze_employee_aisles():
    return ler_postgres("employee_aisles")


@dp.materialized_view(name="kairos.bronze.categories")
def bronze_categories():
    return ler_postgres("categories")


@dp.materialized_view(name="kairos.bronze.product_shelves")
def bronze_product_shelves():
    return ler_postgres("product_shelves")


@dp.materialized_view(name="kairos.bronze.customers")
def bronze_customers():
    return ler_postgres("customers")


@dp.materialized_view(name="kairos.bronze.purchases")
def bronze_purchases():
    return ler_postgres("purchases")


@dp.materialized_view(name="kairos.bronze.purchase_products")
def bronze_purchase_products():
    return ler_postgres("purchase_products")


@dp.materialized_view(name="kairos.bronze.audits")
def bronze_audits():
    return ler_postgres("audits")