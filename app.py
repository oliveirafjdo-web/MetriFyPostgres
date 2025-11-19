
from flask import Flask, render_template, request, redirect, url_for
import pandas as pd

app = Flask(__name__)

# Armazena dados em memória (após importação do Excel)
dados_vendas = None

def calcular_totais(df):
    # Coluna Q = receita líquida (segundo sua planilha)
    receita_total = df["Q"].sum()

    # Comissão = soma da coluna K
    comissao_total = df["K"].sum()

    # Imposto = 5% sobre a coluna H (valor bruto)
    imposto_total = (df["H"] * 0.05).sum()

    # Despesas = 3,5% sobre a coluna Q (valor líquido)
    despesas_total = (df["Q"] * 0.035).sum()

    lucro_liquido = receita_total - comissao_total - imposto_total - despesas_total
    return receita_total, comissao_total, imposto_total, despesas_total, lucro_liquido


@app.route("/")
def dashboard():
    global dados_vendas
    if dados_vendas is None:
        return render_template("dashboard.html", vazio=True)

    receita, comissao, imposto, despesas, lucro = calcular_totais(dados_vendas)

    return render_template(
        "dashboard.html",
        receita=receita,
        comissao=comissao,
        imposto=imposto,
        despesas=despesas,
        lucro=lucro,
        vazio=False,
    )


@app.route("/importar", methods=["POST"])
def importar():
    global dados_vendas
    file = request.files.get("arquivo")

    if not file:
        return redirect(url_for("dashboard"))

    # Lê planilha do Mercado Livre
    df = pd.read_excel(file)

    # Guarda em memória
    dados_vendas = df

    return redirect(url_for("dashboard"))


@app.route("/relatorio")
def relatorio():
    global dados_vendas
    if dados_vendas is None:
        return redirect(url_for("dashboard"))

    df = dados_vendas.copy()

    # Cria colunas calculadas
    df["Comissão"] = df["K"]
    df["Imposto"] = df["H"] * 0.05
    df["Despesas"] = df["Q"] * 0.035
    df["Lucro"] = df["Q"] - df["Comissão"] - df["Imposto"] - df["Despesas"]

    registros = df.to_dict(orient="records")

    return render_template("relatorio.html", tabela=registros)
