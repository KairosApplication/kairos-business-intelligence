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
# Considera os 14 dias mais recentes com vendas disponíveis.
# ============================================================

@dp.materialized_view(name="kairos.gold.daily_sales")
def daily_sales():

    purchases = spark.table("kairos.silver.purchases")
    items = spark.table("kairos.silver.purchase_products")

    sales = (
        purchases
        .filter(F.col("status") == "COMPLETED")
        .join(items, purchases.id == items.purchase_id, "inner")
        .groupBy(
            purchases.id.alias("purchase_id"),
            F.to_date(purchases.date).alias("sale_date")
        )
        .agg(
            F.sum(
                F.col("quantity") * F.col("amount")
            ).alias("purchase_revenue"),
            F.sum("quantity").alias("units_sold"),
            F.count(items.id).alias("item_lines")
        )
    )

    daily = (
        sales
        .groupBy("sale_date")
        .agg(
            F.sum("purchase_revenue").alias("revenue"),
            F.countDistinct("purchase_id").alias("sales_count"),
            F.sum("units_sold").alias("units_sold"),
            F.sum("item_lines").alias("item_lines")
        )
    )

    # Seleciona os 14 dias mais recentes com vendas.
    latest_days = (
        daily
        .withColumn(
            "date_rank",
            F.row_number().over(
                Window.orderBy(F.col("sale_date").desc())
            )
        )
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

    # Ordena cronologicamente para calcular a comparação.
    window = Window.orderBy("sale_date")

    return (
        result
        .withColumn(
            "previous_day_revenue",
            F.lag("revenue", 1).over(window)
        )
        .withColumn(
            "previous_day_sales",
            F.lag("sales_count", 1).over(window)
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
        .orderBy("sale_date")
    )


# ============================================================
# 2. VENDAS POR CATEGORIA
# Insights: receita, unidades e participação por categoria.
# Agrupa categorias com o mesmo nome.
# ============================================================

@dp.materialized_view(name="kairos.gold.sales_by_category")
def sales_by_category():

    purchases = spark.table("kairos.silver.purchases")
    items = spark.table("kairos.silver.purchase_products")
    products = spark.table("kairos.silver.products")
    categories = spark.table("kairos.silver.categories")

    sales = (
        items
        .join(
            purchases.filter(F.col("status") == "COMPLETED"),
            items.purchase_id == purchases.id,
            "inner"
        )
        .join(
            products,
            items.product_id == products.id,
            "inner"
        )
        .join(
            categories.select(
                "id",
                "category"
            ),
            products.category_id == categories.id,
            "left"
        )
        .groupBy(
            F.coalesce(
                F.col("category"),
                F.lit("Sem categoria")
            ).alias("category")
        )
        .agg(
            F.sum(
                F.col("quantity") * F.col("amount")
            ).alias("revenue"),
            F.sum("quantity").alias("units_sold"),
            F.countDistinct(purchases.id).alias("sales_count"),
            F.countDistinct(products.id).alias("products_sold")
        )
    )

    total = sales.agg(
        F.sum("revenue").alias("total_revenue")
    )

    return (
        sales
        .crossJoin(total)
        .withColumn(
            "revenue_share_percent",
            F.when(
                F.col("total_revenue") > 0,
                (F.col("revenue") / F.col("total_revenue")) * 100
            ).otherwise(F.lit(0))
        )
        .drop("total_revenue")
        .orderBy(F.desc("revenue"))
    )


# ============================================================
# 3. ESTOQUE POR PRODUTO
# Mantém prateleiras e inventários separados.
# ============================================================

@dp.materialized_view(name="kairos.gold.stock_by_product")
def stock_by_product():

    products = spark.table("kairos.silver.products")
    categories = spark.table("kairos.silver.categories")
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
            categories.select(
                "id",
                F.col("category").alias("category")
            ),
            products.category_id == categories.id,
            "left"
        )
        .join(
            shelf_stock,
            products.id == shelf_stock.product_id,
            "left"
        )
        .join(
            inventory_stock,
            products.id == inventory_stock.product_id,
            "left"
        )
        .select(
            products.id.alias("product_id"),
            products.name.alias("product_name"),
            products.brand,
            F.coalesce(
                F.col("category"),
                F.lit("Sem categoria")
            ).alias("category"),
            products.price,
            F.coalesce(
                F.col("shelf_quantity"),
                F.lit(0)
            ).alias("shelf_quantity"),
            F.coalesce(
                F.col("shelves_count"),
                F.lit(0)
            ).alias("shelves_count"),
            F.coalesce(
                F.col("inventory_quantity"),
                F.lit(0)
            ).alias("inventory_quantity"),
            F.coalesce(
                F.col("inventories_count"),
                F.lit(0)
            ).alias("inventories_count")
        )
    )


# ============================================================
# 4. RESUMO DE RUPTURAS
# Insights: alertas por categoria, status e taxa aproximada.
# Considera somente alertas OPEN na taxa de ruptura.
# ============================================================

@dp.materialized_view(name="kairos.gold.stockout_summary")
def stockout_summary():

    alerts = spark.table("kairos.silver.alerts")
    products = spark.table("kairos.silver.products")
    categories = spark.table("kairos.silver.categories")

    product_totals = (
        products
        .join(
            categories.select(
                "id",
                F.col("category").alias("category")
            ),
            products.category_id == categories.id,
            "left"
        )
        .groupBy(
            F.coalesce(
                F.col("category"),
                F.lit("Sem categoria")
            ).alias("category")
        )
        .agg(
            F.countDistinct(products.id).alias("total_products")
        )
    )

    alert_totals = (
        alerts
        .join(
            products,
            alerts.product_id == products.id,
            "inner"
        )
        .join(
            categories.select(
                "id",
                F.col("category").alias("category")
            ),
            products.category_id == categories.id,
            "left"
        )
        .groupBy(
            F.coalesce(
                F.col("category"),
                F.lit("Sem categoria")
            ).alias("category")
        )
        .agg(
            F.countDistinct(alerts.id).alias("total_alerts"),

            F.countDistinct(
                F.when(
                    alerts.status == "OPEN",
                    alerts.id
                )
            ).alias("open_alerts"),

            F.countDistinct(
                F.when(
                    alerts.status == "RESOLVED",
                    alerts.id
                )
            ).alias("resolved_alerts"),

            F.countDistinct(
                F.when(
                    alerts.status == "CANCELLED",
                    alerts.id
                )
            ).alias("cancelled_alerts"),

            F.countDistinct(
                F.when(
                    alerts.status == "OPEN",
                    alerts.product_id
                )
            ).alias("products_with_open_alerts")
        )
    )

    return (
        product_totals
        .join(
            alert_totals,
            "category",
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
# Dashboard e Insights: reposições, unidades e avarias.
# ============================================================

@dp.materialized_view(name="kairos.gold.replenishment_summary")
def replenishment_summary():

    restockings = spark.table("kairos.silver.restockings")

    return (
        restockings
        .groupBy(
            F.to_date("restocking_date").alias("restocking_day")
        )
        .agg(
            F.count("id").alias("restockings_count"),
            F.sum("restocked_quantity").alias("units_restocked"),
            F.sum("qnt_avarias").alias("damaged_units"),
            F.countDistinct("product_id").alias("products_restocked"),
            F.countDistinct("employee_id").alias("employees_involved")
        )
    )


# ============================================================
# 6. RESUMO GERAL DA OPERAÇÃO
# Indicadores gerais para os cards do Dashboard.
# ============================================================

@dp.materialized_view(name="kairos.gold.operations_summary")
def operations_summary():

    products = spark.table("kairos.silver.products")
    alerts = spark.table("kairos.silver.alerts")
    restockings = spark.table("kairos.silver.restockings")
    shelves = spark.table("kairos.silver.product_shelves")
    inventories = spark.table("kairos.silver.product_inventories")

    product_metrics = products.agg(
        F.countDistinct("id").alias("total_products")
    )

    alert_metrics = alerts.agg(
        F.countDistinct(
            F.when(
                F.col("status") == "OPEN",
                F.col("id")
            )
        ).alias("open_alerts"),

        F.countDistinct("id").alias("total_alerts")
    )

    restocking_metrics = restockings.agg(
        F.countDistinct("id").alias("total_restockings"),
        F.sum("restocked_quantity").alias("total_units_restocked"),
        F.sum("qnt_avarias").alias("total_damaged_units")
    )

    shelf_metrics = shelves.agg(
        F.sum("product_quantity").alias("total_shelf_quantity"),
        F.countDistinct("product_id").alias("products_on_shelves")
    )

    inventory_metrics = inventories.agg(
        F.sum("product_quantity").alias("total_inventory_quantity"),
        F.countDistinct("product_id").alias("products_in_inventories")
    )

    return (
        product_metrics
        .crossJoin(alert_metrics)
        .crossJoin(restocking_metrics)
        .crossJoin(shelf_metrics)
        .crossJoin(inventory_metrics)
        .fillna(0)
    )