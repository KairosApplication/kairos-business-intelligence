# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Gold
# MAGIC Pega a tabela Silver, calcula e retorna as métricas que serão utilizadas:
# MAGIC
# MAGIC ## 1. Vendas (`daily_sales`)
# MAGIC
# MAGIC - **Faturamento diário:** valor total das vendas concluídas por dia.
# MAGIC - **Quantidade de vendas:** número de compras concluídas com itens.
# MAGIC - **Ticket médio:** faturamento dividido pela quantidade de vendas com itens.
# MAGIC - **Unidades vendidas:** quantidade total de produtos vendidos.
# MAGIC - **Linhas de itens vendidas:** quantidade de registros de produtos nas compras.
# MAGIC - **Faturamento do dia anterior:** valor usado para comparação.
# MAGIC - **Variação percentual do faturamento:** comparação com o dia anterior.
# MAGIC - **Comparação entre períodos:** identificação dos últimos 7 dias e dos 7 dias anteriores.
# MAGIC
# MAGIC ## 2. Vendas por categoria (`sales_by_category`)
# MAGIC
# MAGIC - **Faturamento por categoria:** receita gerada por cada categoria.
# MAGIC - **Participação percentual no faturamento:** quanto cada categoria representa do total.
# MAGIC - **Unidades vendidas por categoria.**
# MAGIC - **Quantidade de vendas por categoria.**
# MAGIC - **Quantidade de produtos vendidos por categoria.**
# MAGIC
# MAGIC ## 3. Estoque por produto (`stock_by_product`)
# MAGIC
# MAGIC - **Quantidade em prateleiras:** total de unidades registradas nas prateleiras.
# MAGIC - **Quantidade de prateleiras ocupadas:** número de prateleiras que contêm o produto.
# MAGIC - **Quantidade em inventários:** total de unidades registradas nos inventários.
# MAGIC - **Quantidade de inventários associados:** número de inventários que contêm o produto.
# MAGIC - **Preço, marca e categoria do produto:** informações para análise e identificação.
# MAGIC
# MAGIC > Os estoques de prateleiras e inventários são apresentados separadamente, pois ainda não foi confirmado se representam quantidades que podem ser somadas.
# MAGIC
# MAGIC ## 4. Rupturas e alertas (`stockout_summary`)
# MAGIC
# MAGIC - **Total de alertas por categoria.**
# MAGIC - **Alertas abertos:** quantidade de alertas que ainda estão em aberto.
# MAGIC - **Alertas resolvidos.**
# MAGIC - **Alertas cancelados.**
# MAGIC - **Produtos com alertas abertos:** quantidade distinta de produtos afetados.
# MAGIC - **Taxa aproximada de ruptura:** percentual de produtos da categoria que possuem alertas abertos.
# MAGIC
# MAGIC > Essa taxa é uma aproximação baseada nos alertas, não uma confirmação de ruptura física do estoque.
# MAGIC
# MAGIC ## 5. Reposição (`replenishment_summary`)
# MAGIC
# MAGIC - **Quantidade de reposições por dia.**
# MAGIC - **Unidades repostas por dia.**
# MAGIC - **Unidades avariadas durante as reposições.**
# MAGIC - **Quantidade de produtos repostos.**
# MAGIC - **Quantidade de funcionários envolvidos nas reposições.**
# MAGIC
# MAGIC ## 6. Visão geral das operações (`operations_summary`)
# MAGIC
# MAGIC - **Total de produtos cadastrados.**
# MAGIC - **Total de alertas abertos.**
# MAGIC - **Total de alertas registrados.**
# MAGIC - **Total de reposições realizadas.**
# MAGIC - **Total de unidades repostas.**
# MAGIC - **Total de unidades avariadas.**
# MAGIC - **Quantidade total de unidades nas prateleiras.**
# MAGIC - **Quantidade de produtos presentes nas prateleiras.**
# MAGIC - **Quantidade total de unidades nos inventários.**
# MAGIC - **Quantidade de produtos presentes nos inventários.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Resumo
# MAGIC
# MAGIC Atualmente, o Gold cobre principalmente três áreas:
# MAGIC
# MAGIC 1. **Desempenho de vendas:** faturamento, ticket médio, quantidade de vendas e desempenho por categoria.
# MAGIC 2. **Situação do estoque:** quantidades por produto, alertas e estimativa de rupturas.
# MAGIC 3. **Atividade de reposição:** quantidade de reposições, unidades repostas e avarias.
# MAGIC
# MAGIC Os indicadores que dependem de tarefas, prioridades ou tempos de resolução ainda precisam de dados adicionais ou de regras de negócio próprias.

# COMMAND ----------

# DBTITLE 1,Imports e Variáveis
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Definição da Tabela
# ============================================================
# 1. VENDAS DIARIAS
# Dashboard: faturamento, vendas, ticket médio e comparação.
# Considera os 14 dias mais recentes com vendas disponíveis
# PARA CADA EMPRESA.
# company_id vem de Purchases -> Customers.
# ============================================================

@dp.materialized_view(name="kairos.gold.daily_sales")
def daily_sales():

    purchases = spark.table("kairos.silver.purchases").alias("p")
    items = spark.table("kairos.silver.purchase_products").alias("i")
    customers = spark.table("kairos.silver.customers").alias("c")

    sales = (
        purchases
        .filter(F.col("p.status") == "COMPLETED")
        .join(
            customers,
            F.col("p.customer_id") == F.col("c.id"),
            "inner"
        )
        .join(
            items,
            F.col("p.id") == F.col("i.purchase_id"),
            "inner"
        )
        .select(
            F.col("c.company_id").alias("company_id"),
            F.col("p.id").alias("purchase_id"),
            F.to_date(F.col("p.date")).alias("sale_date"),
            F.col("i.quantity").alias("quantity"),
            F.col("i.amount").alias("amount"),
            F.col("i.id").alias("item_id")
        )
        .groupBy("company_id", "purchase_id", "sale_date")
        .agg(
            F.sum(F.col("quantity") * F.col("amount")).alias("purchase_revenue"),
            F.sum("quantity").alias("units_sold"),
            F.count("item_id").alias("item_lines")
        )
    )

    daily = (
        sales
        .groupBy("company_id", "sale_date")
        .agg(
            F.sum("purchase_revenue").alias("revenue"),
            F.countDistinct("purchase_id").alias("sales_count"),
            F.sum("units_sold").alias("units_sold"),
            F.sum("item_lines").alias("item_lines")
        )
    )

    # Seleciona os 14 dias mais recentes com vendas por empresa.
    rank_window = Window.partitionBy("company_id").orderBy(
        F.col("sale_date").desc()
    )

    latest_days = (
        daily
        .withColumn("date_rank", F.row_number().over(rank_window))
        .filter(F.col("date_rank") <= 14)
    )

    result = (
        latest_days
        .fillna({
            "revenue": 0,
            "sales_count": 0,
            "units_sold": 0,
            "item_lines": 0
        })
        .withColumn(
            "average_ticket",
            F.when(
                F.col("sales_count") > 0,
                F.col("revenue") / F.col("sales_count")
            ).otherwise(F.lit(0))
        )
    )

    # Comparação com o dia anterior disponível da MESMA empresa.
    comparison_window = (
        Window
        .partitionBy("company_id")
        .orderBy("sale_date")
    )

    return (
        result
        .withColumn(
            "previous_day_revenue",
            F.lag("revenue", 1).over(comparison_window)
        )
        .withColumn(
            "previous_day_sales",
            F.lag("sales_count", 1).over(comparison_window)
        )
        .withColumn(
            "revenue_change_percent",
            F.when(
                F.col("previous_day_revenue") > 0,
                (
                    (F.col("revenue") - F.col("previous_day_revenue"))
                    / F.col("previous_day_revenue")
                ) * 100
            ).otherwise(F.lit(None).cast("double"))
        )
        .withColumn(
            "period",
            F.when(
                F.col("date_rank") <= 7,
                "CURRENT_7_DAYS"
            ).otherwise("PREVIOUS_7_DAYS")
        )
        .drop("date_rank")
        .orderBy("company_id", "sale_date")
    )


# ============================================================
# 2. VENDAS POR CATEGORIA
# Insights: receita, unidades e participação por categoria.
# Categorias são separadas por company_id.
# ============================================================

@dp.materialized_view(name="kairos.gold.sales_by_category")
def sales_by_category():

    purchases = spark.table("kairos.silver.purchases").alias("p")
    items = spark.table("kairos.silver.purchase_products").alias("i")
    customers = spark.table("kairos.silver.customers").alias("cu")
    products = spark.table("kairos.silver.products").alias("pr")
    categories = spark.table("kairos.silver.categories").alias("ca")

    sales = (
        items
        .join(
            purchases.filter(F.col("p.status") == "COMPLETED"),
            F.col("i.purchase_id") == F.col("p.id"),
            "inner"
        )
        .join(
            customers,
            F.col("p.customer_id") == F.col("cu.id"),
            "inner"
        )
        .join(
            products,
            (F.col("i.product_id") == F.col("pr.id")) &
            (F.col("pr.company_id") == F.col("cu.company_id")),
            "inner"
        )
        .join(
            categories,
            (F.col("pr.category_id") == F.col("ca.id")) &
            (F.col("pr.company_id") == F.col("ca.company_id")),
            "left"
        )
        .select(
            F.col("cu.company_id").alias("company_id"),
            F.coalesce(
                F.col("ca.category"),
                F.lit("Sem categoria")
            ).alias("category"),
            F.col("i.quantity").alias("quantity"),
            F.col("i.amount").alias("amount"),
            F.col("p.id").alias("purchase_id"),
            F.col("pr.id").alias("product_id")
        )
    )

    sales = (
        sales
        .groupBy("company_id", "category")
        .agg(
            F.sum(F.col("quantity") * F.col("amount")).alias("revenue"),
            F.sum("quantity").alias("units_sold"),
            F.countDistinct("purchase_id").alias("sales_count"),
            F.countDistinct("product_id").alias("products_sold")
        )
    )

    # Participação calculada dentro de cada empresa, não no total geral.
    company_window = Window.partitionBy("company_id")

    return (
        sales
        .withColumn(
            "total_revenue",
            F.sum("revenue").over(company_window)
        )
        .withColumn(
            "revenue_share_percent",
            F.when(
                F.col("total_revenue") > 0,
                (F.col("revenue") / F.col("total_revenue")) * 100
            ).otherwise(F.lit(0))
        )
        .drop("total_revenue")
        .orderBy("company_id", F.desc("revenue"))
    )


# ============================================================
# 3. ESTOQUE POR PRODUTO
# Mantém prateleiras e inventários separados.
# company_id vem de Products.
# ============================================================

@dp.materialized_view(name="kairos.gold.stock_by_product")
def stock_by_product():

    products = spark.table("kairos.silver.products").alias("p")
    categories = spark.table("kairos.silver.categories").alias("c")
    shelves = spark.table("kairos.silver.product_shelves")
    inventories = spark.table("kairos.silver.product_inventories")

    shelf_stock = (
        shelves
        .groupBy("product_id")
        .agg(
            F.sum("product_quantity").alias("shelf_quantity"),
            F.countDistinct("shelf_id").alias("shelves_count")
        )
    )

    inventory_stock = (
        inventories
        .groupBy("product_id")
        .agg(
            F.sum("product_quantity").alias("inventory_quantity"),
            F.countDistinct("inventory_id").alias("inventories_count")
        )
    )

    return (
        products
        .join(
            categories,
            (F.col("p.category_id") == F.col("c.id")) &
            (F.col("p.company_id") == F.col("c.company_id")),
            "left"
        )
        .join(
            shelf_stock.alias("ss"),
            F.col("p.id") == F.col("ss.product_id"),
            "left"
        )
        .join(
            inventory_stock.alias("ins"),
            F.col("p.id") == F.col("ins.product_id"),
            "left"
        )
        .select(
            F.col("p.company_id").alias("company_id"),
            F.col("p.id").alias("product_id"),
            F.col("p.name").alias("product_name"),
            F.col("p.brand").alias("brand"),
            F.coalesce(
                F.col("c.category"),
                F.lit("Sem categoria")
            ).alias("category"),
            F.col("p.price").alias("price"),
            F.coalesce(F.col("ss.shelf_quantity"), F.lit(0)).alias("shelf_quantity"),
            F.coalesce(F.col("ss.shelves_count"), F.lit(0)).alias("shelves_count"),
            F.coalesce(F.col("ins.inventory_quantity"), F.lit(0)).alias("inventory_quantity"),
            F.coalesce(F.col("ins.inventories_count"), F.lit(0)).alias("inventories_count")
        )
    )


# ============================================================
# 4. RESUMO DE RUPTURAS
# Considera somente alertas OPEN na taxa de ruptura.
# company_id vem de Products.
# ============================================================

@dp.materialized_view(name="kairos.gold.stockout_summary")
def stockout_summary():

    alerts = spark.table("kairos.silver.alerts").alias("a")
    products = spark.table("kairos.silver.products").alias("p")
    categories = spark.table("kairos.silver.categories").alias("c")

    product_data = (
        products
        .join(
            categories,
            (F.col("p.category_id") == F.col("c.id")) &
            (F.col("p.company_id") == F.col("c.company_id")),
            "left"
        )
        .select(
            F.col("p.company_id").alias("company_id"),
            F.col("p.id").alias("product_id"),
            F.coalesce(
                F.col("c.category"),
                F.lit("Sem categoria")
            ).alias("category")
        )
    )

    product_totals = (
        product_data
        .groupBy("company_id", "category")
        .agg(
            F.countDistinct("product_id").alias("total_products")
        )
    )

    alert_totals = (
        alerts
        .join(
            product_data.alias("pd"),
            F.col("a.product_id") == F.col("pd.product_id"),
            "inner"
        )
        .groupBy(F.col("pd.company_id").alias("company_id"), F.col("pd.category").alias("category"))
        .agg(
            F.countDistinct("a.id").alias("total_alerts"),

            F.countDistinct(
                F.when(F.col("a.status") == "OPEN", F.col("a.id"))
            ).alias("open_alerts"),

            F.countDistinct(
                F.when(F.col("a.status") == "RESOLVED", F.col("a.id"))
            ).alias("resolved_alerts"),

            F.countDistinct(
                F.when(F.col("a.status") == "CANCELLED", F.col("a.id"))
            ).alias("cancelled_alerts"),

            F.countDistinct(
                F.when(F.col("a.status") == "OPEN", F.col("a.product_id"))
            ).alias("products_with_open_alerts")
        )
    )

    return (
        product_totals
        .join(
            alert_totals,
            ["company_id", "category"],
            "left"
        )
        .fillna({
            "total_alerts": 0,
            "open_alerts": 0,
            "resolved_alerts": 0,
            "cancelled_alerts": 0,
            "products_with_open_alerts": 0
        })
        .withColumn(
            "stockout_rate_percent",
            F.when(
                F.col("total_products") > 0,
                (
                    F.col("products_with_open_alerts")
                    / F.col("total_products")
                ) * 100
            ).otherwise(F.lit(0))
        )
    )


# ============================================================
# 5. RESUMO DE REPOSIÇÕES
# company_id vem de Restockings -> Inventories.
# ============================================================

@dp.materialized_view(name="kairos.gold.replenishment_summary")
def replenishment_summary():

    restockings = spark.table("kairos.silver.restockings").alias("r")
    inventories = spark.table("kairos.silver.inventories").alias("i")

    return (
        restockings
        .join(
            inventories,
            F.col("r.inventory_id") == F.col("i.id"),
            "inner"
        )
        .groupBy(
            F.col("i.company_id").alias("company_id"),
            F.to_date(F.col("r.restocking_date")).alias("restocking_day")
        )
        .agg(
            F.count(F.col("r.id")).alias("restockings_count"),
            F.sum(F.col("r.restocked_quantity")).alias("units_restocked"),
            F.sum(F.col("r.qnt_avarias")).alias("damaged_units"),
            F.countDistinct(F.col("r.product_id")).alias("products_restocked"),
            F.countDistinct(F.col("r.employee_id")).alias("employees_involved")
        )
        .orderBy("company_id", "restocking_day")
    )


# ============================================================
# 6. RESUMO GERAL DA OPERAÇÃO
# Uma linha por empresa.
# ============================================================

@dp.materialized_view(name="kairos.gold.operations_summary")
def operations_summary():

    companies = spark.table("kairos.silver.companies").select(
        F.col("id").alias("company_id")
    )

    products = spark.table("kairos.silver.products")
    alerts = spark.table("kairos.silver.alerts").alias("a")
    restockings = spark.table("kairos.silver.restockings").alias("r")
    inventories = spark.table("kairos.silver.inventories").alias("i")
    shelves = spark.table("kairos.silver.product_shelves").alias("ps")
    product_inventories = spark.table("kairos.silver.product_inventories").alias("pi")

    product_metrics = (
        products
        .groupBy("company_id")
        .agg(
            F.countDistinct("id").alias("total_products")
        )
    )

    # Alertas são atribuídos à empresa proprietária do produto.
    alert_metrics = (
        alerts
        .join(
            products.select(
                F.col("id").alias("product_id"),
                "company_id"
            ).alias("p"),
            F.col("a.product_id") == F.col("p.product_id"),
            "inner"
        )
        .groupBy(F.col("p.company_id").alias("company_id"))
        .agg(
            F.countDistinct(
                F.when(F.col("a.status") == "OPEN", F.col("a.id"))
            ).alias("open_alerts"),
            F.countDistinct(F.col("a.id")).alias("total_alerts")
        )
    )

    # Reposições são atribuídas à empresa proprietária do inventário.
    restocking_metrics = (
        restockings
        .join(
            inventories,
            F.col("r.inventory_id") == F.col("i.id"),
            "inner"
        )
        .groupBy(F.col("i.company_id").alias("company_id"))
        .agg(
            F.countDistinct(F.col("r.id")).alias("total_restockings"),
            F.sum(F.col("r.restocked_quantity")).alias("total_units_restocked"),
            F.sum(F.col("r.qnt_avarias")).alias("total_damaged_units")
        )
    )

    # Estoque de prateleiras atribuído à empresa do produto.
    shelf_metrics = (
        shelves
        .join(
            products.select(
                F.col("id").alias("product_id"),
                "company_id"
            ).alias("p"),
            F.col("ps.product_id") == F.col("p.product_id"),
            "inner"
        )
        .groupBy(F.col("p.company_id").alias("company_id"))
        .agg(
            F.sum(F.col("ps.product_quantity")).alias("total_shelf_quantity"),
            F.countDistinct(F.col("ps.product_id")).alias("products_on_shelves")
        )
    )

    # Estoque de inventários atribuído à empresa do inventário.
    inventory_metrics = (
        product_inventories
        .join(
            inventories,
            F.col("pi.inventory_id") == F.col("i.id"),
            "inner"
        )
        .groupBy(F.col("i.company_id").alias("company_id"))
        .agg(
            F.sum(F.col("pi.product_quantity")).alias("total_inventory_quantity"),
            F.countDistinct(F.col("pi.product_id")).alias("products_in_inventories")
        )
    )

    return (
        companies
        .join(product_metrics, "company_id", "left")
        .join(alert_metrics, "company_id", "left")
        .join(restocking_metrics, "company_id", "left")
        .join(shelf_metrics, "company_id", "left")
        .join(inventory_metrics, "company_id", "left")
        .fillna({
            "total_products": 0,
            "open_alerts": 0,
            "total_alerts": 0,
            "total_restockings": 0,
            "total_units_restocked": 0,
            "total_damaged_units": 0,
            "total_shelf_quantity": 0,
            "products_on_shelves": 0,
            "total_inventory_quantity": 0,
            "products_in_inventories": 0
        })
        .orderBy("company_id")
    )