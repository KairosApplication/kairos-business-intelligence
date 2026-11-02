# Databricks notebook source
# MAGIC %md
# MAGIC # Configurações de Secrets para Notebooks BI
# MAGIC

# COMMAND ----------

# DBTITLE 1,Teste
import getpass
import requests
import getpass as gp

# URL base do seu workspace
workspace = input("URL do workspace Databricks: ").strip().rstrip("/")

# Credencial temporária
token = gp.getpass("Token do Databricks: ")

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

print("Autenticação preparada.")

# COMMAND ----------

scope = "postgres-secrets"

usuario = input("Usuário do PostgreSQL: ")
senha = gp.getpass("Senha do PostgreSQL: ")

def salvar_secret(chave, valor):
    resposta = requests.post(
        f"{workspace}/api/2.0/secrets/put",
        headers=headers,
        json={
            "scope": scope,
            "key": chave,
            "string_value": valor
        },
        timeout=30
    )

    if resposta.ok:
        print(f"Secret '{chave}' cadastrado.")
    else:
        raise RuntimeError(
            f"Erro ao cadastrar '{chave}': "
            f"{resposta.status_code} — {resposta.text}"
        )

salvar_secret("username", usuario)
salvar_secret("password", senha)

del token, headers, usuario, senha
print("Processo concluído.")

# COMMAND ----------

usuario = dbutils.secrets.get(
    scope="postgres-secrets",
    key="username"
)

print("Usuário recuperado com sucesso.")