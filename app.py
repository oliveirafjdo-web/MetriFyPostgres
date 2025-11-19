import os
from datetime import datetime

from flask import Flask, render_template, request, redirect, url_for, flash
from werkzeug.utils import secure_filename

from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Float,
    ForeignKey, func, select, insert, update, delete
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError

import pandas as pd

# --------------------------------------------------------------------
# Configuração de banco: Postgres em produção, SQLite em desenvolvimento
# --------------------------------------------------------------------
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///metrifiy.db")
UPLOAD_FOLDER = os.environ.get("UPLOAD_FOLDER", "uploads")

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.secret_key = os.environ.get("SECRET_KEY", "metrifypremium-secret")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

engine: Engine = create_engine(DATABASE_URL, future=True)
metadata = MetaData()

# --------------------------------------------------------------------
# Definição das tabelas (compatível com Postgres e SQLite)
# --------------------------------------------------------------------
produtos = Table(
    "produtos",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("nome", String(255), nullable=False),
    Column("sku", String(100), unique=True),
    Column("custo_unitario", Float, nullable=False, server_default="0"),
    Column("preco_venda_sugerido", Float, nullable=False, server_default="0"),
    Column("estoque_inicial", Integer, nullable=False, server_default="0"),
    Column("estoque_atual", Integer, nullable=False, server_default="0"),
    Column("curva", String(1)),
)

vendas = Table(
    "vendas",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("produto_id", Integer, ForeignKey("produtos.id"), nullable=False),
    Column("data_venda", String(50)),
    Column("quantidade", Integer, nullable=False),
    Column("preco_venda_unitario", Float, nullable=False),
    Column("receita_total", Float, nullable=False),
    Column("custo_total", Float, nullable=False),
    Column("margem_contribuicao", Float, nullable=False),
    Column("origem", String(50)),
    Column("numero_venda_ml", String(100)),
    Column("lote_importacao", String(50)),
)

def init_db():
    """Cria as tabelas se não existirem (Postgres ou SQLite)."""
    metadata.create_all(engine)

# --------------------------------------------------------------------
# Utilidades para datas
# --------------------------------------------------------------------
MESES_PT = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3,
    "abril": 4, "maio": 5, "junho": 6, "julho": 7,
    "agosto": 8, "setembro": 9, "outubro": 10,
    "novembro": 11, "dezembro": 12,
}

def parse_data_venda(texto):
    if isinstance(texto, datetime):
        return texto
    if not isinstance(texto, str) or not texto.strip():
        return None
    try:
        partes = texto.split()
        dia = int(partes[0])
        mes_nome = partes[2].lower()
        ano = int(partes[4])
        hora_min = partes[5]
        hora, minuto = hora_min.split(":")
        return datetime(ano, MESES_PT[mes_nome], int(dia), int(hora), int(minuto))
    except Exception:
        return None

# --------------------------------------------------------------------
# Importação de vendas do Mercado Livre
# --------------------------------------------------------------------
def importar_vendas_ml(caminho_arquivo, engine: Engine):
    lote_id = datetime.now().isoformat(timespec="seconds")

    df = pd.read_excel(
        caminho_arquivo,
        sheet_name="Vendas BR",
        header=5
    )
    if "N.º de venda" not in df.columns:
        raise ValueError("Planilha não está no formato esperado: coluna 'N.º de venda' não encontrada.")

    df = df[df["N.º de venda"].notna()]

    vendas_importadas = 0
    vendas_sem_sku = 0
    vendas_sem_produto = 0

    with engine.begin() as conn:
        for _, row in df.iterrows():
            sku = str(row.get("SKU") or "").strip()
            if not sku:
                vendas_sem_sku += 1
                continue

            prod = conn.execute(
                select(produtos.c.id, produtos.c.custo_unitario).where(produtos.c.sku == sku)
            ).mappings().first()
            if not prod:
                vendas_sem_produto += 1
                continue

            produto_id = prod["id"]
            custo_unitario = float(prod["custo_unitario"] or 0.0)

            data_venda_raw = row.get("Data da venda")
            data_venda = parse_data_venda(data_venda_raw)
            unidades = row.get("Unidades")
            try:
                unidades = int(unidades) if unidades == unidades else 0
            except Exception:
                unidades = 0

            total_brl = row.get("Total (BRL)")
            try:
                receita_total = float(total_brl) if total_brl == total_brl else 0.0
            except Exception:
                receita_total = 0.0

            preco_medio_venda = receita_total / unidades if unidades > 0 else 0.0
            custo_total = custo_unitario * unidades
            margem_contribuicao = receita_total - custo_total
            numero_venda_ml = str(row.get("N.º de venda"))

            conn.execute(
                insert(vendas).values(
                    produto_id=produto_id,
                    data_venda=data_venda.isoformat() if data_venda else None,
                    quantidade=unidades,
                    preco_venda_unitario=preco_medio_venda,
                    receita_total=receita_total,
                    custo_total=custo_total,
                    margem_contribuicao=margem_contribuicao,
                    origem="Mercado Livre",
                    numero_venda_ml=numero_venda_ml,
                    lote_importacao=lote_id,
                )
            )

            conn.execute(
                update(produtos)
                .where(produtos.c.id == produto_id)
                .values(estoque_atual=produtos.c.estoque_atual - unidades)
            )

            vendas_importadas += 1

    return {
        "lote_id": lote_id,
        "vendas_importadas": vendas_importadas,
        "vendas_sem_sku": vendas_sem_sku,
        "vendas_sem_produto": vendas_sem_produto,
    }

# --------------------------------------------------------------------
# Rotas
# --------------------------------------------------------------------
@app.route("/")
def dashboard():
    with engine.connect() as conn:
        total_produtos = conn.execute(
            select(func.count()).select_from(produtos)
        ).scalar_one()

        estoque_total = conn.execute(
            select(func.coalesce(func.sum(produtos.c.estoque_atual), 0))
        ).scalar_one()

        receita_total = conn.execute(
            select(func.coalesce(func.sum(vendas.c.receita_total), 0))
        ).scalar_one()

        lucro_total = conn.execute(
            select(func.coalesce(func.sum(vendas.c.margem_contribuicao), 0))
        ).scalar_one()

        margem_media = conn.execute(
            select(
                func.coalesce(
                    func.avg(
                        func.nullif(
                            (vendas.c.margem_contribuicao / vendas.c.receita_total) * 100,
                            0
                        )
                    ),
                    0
                )
            )
        ).scalar_one()

        ticket_medio = conn.execute(
            select(func.coalesce(func.avg(vendas.c.preco_venda_unitario), 0))
        ).scalar_one()

        # Produto mais vendido
        produto_mais_vendido = conn.execute(
            select(produtos.c.nome, func.sum(vendas.c.quantidade).label("qtd"))
            .select_from(vendas.join(produtos))
            .group_by(produtos.c.id)
            .order_by(func.sum(vendas.c.quantidade).desc())
            .limit(1)
        ).first()

        # Maior lucro
        produto_maior_lucro = conn.execute(
            select(produtos.c.nome, func.sum(vendas.c.margem_contribuicao).label("lucro"))
            .select_from(vendas.join(produtos))
            .group_by(produtos.c.id)
            .order_by(func.sum(vendas.c.margem_contribuicao).desc())
            .limit(1)
        ).first()

        # Pior margem
        produto_pior_margem = conn.execute(
            select(produtos.c.nome, func.sum(vendas.c.margem_contribuicao).label("margem"))
            .select_from(vendas.join(produtos))
            .group_by(produtos.c.id)
            .order_by(func.sum(vendas.c.margem_contribuicao).asc())
            .limit(1)
        ).first()

    return render_template(
        "dashboard.html",
        total_produtos=total_produtos,
        estoque_total=estoque_total,
        receita_total=receita_total,
        lucro_total=lucro_total,
        margem_media=margem_media,
        ticket_medio=ticket_medio,
        comissao_total=0,
        produto_mais_vendido=produto_mais_vendido,
        produto_maior_lucro=produto_maior_lucro,
        produto_pior_margem=produto_pior_margem,
    )

@app.route("/produtos")
def lista_produtos():
    with engine.connect() as conn:
        produtos_rows = conn.execute(select(produtos).order_by(produtos.c.nome)).mappings().all()
    return render_template("produtos.html", produtos=produtos_rows)

@app.route("/produtos/novo", methods=["GET", "POST"])
def novo_produto():
    if request.method == "POST":
        nome = request.form["nome"]
        sku = request.form["sku"]
        custo_unitario = float(request.form.get("custo_unitario", 0) or 0)
        preco_venda_sugerido = float(request.form.get("preco_venda_sugerido", 0) or 0)
        estoque_inicial = int(request.form.get("estoque_inicial", 0) or 0)

        with engine.begin() as conn:
            conn.execute(
                insert(produtos).values(
                    nome=nome,
                    sku=sku,
                    custo_unitario=custo_unitario,
                    preco_venda_sugerido=preco_venda_sugerido,
                    estoque_inicial=estoque_inicial,
                    estoque_atual=estoque_inicial,
                )
            )
        flash("Produto cadastrado com sucesso!", "success")
        return redirect(url_for("lista_produtos"))

    return render_template("produto_form.html", produto=None)

@app.route("/produtos/<int:produto_id>/editar", methods=["GET", "POST"])
def editar_produto(produto_id):
    if request.method == "POST":
        nome = request.form["nome"]
        sku = request.form["sku"]
        custo_unitario = float(request.form.get("custo_unitario", 0) or 0)
        preco_venda_sugerido = float(request.form.get("preco_venda_sugerido", 0) or 0)
        estoque_atual = int(request.form.get("estoque_atual", 0) or 0)

        with engine.begin() as conn:
            conn.execute(
                update(produtos)
                .where(produtos.c.id == produto_id)
                .values(
                    nome=nome,
                    sku=sku,
                    custo_unitario=custo_unitario,
                    preco_venda_sugerido=preco_venda_sugerido,
                    estoque_atual=estoque_atual,
                )
            )
        flash("Produto atualizado!", "success")
        return redirect(url_for("lista_produtos"))

    with engine.connect() as conn:
        produto_row = conn.execute(
            select(produtos).where(produtos.c.id == produto_id)
        ).mappings().first()

    if not produto_row:
        flash("Produto não encontrado.", "danger")
        return redirect(url_for("lista_produtos"))

    return render_template("produto_form.html", produto=produto_row)

@app.route("/produtos/<int:produto_id>/excluir", methods=["POST"])
def excluir_produto(produto_id):
    with engine.begin() as conn:
        conn.execute(delete(produtos).where(produtos.c.id == produto_id))
    flash("Produto excluído.", "success")
    return redirect(url_for("lista_produtos"))

@app.route("/vendas")
def lista_vendas():
    with engine.connect() as conn:
        vendas_rows = conn.execute(
            select(
                vendas.c.id,
                vendas.c.data_venda,
                vendas.c.quantidade,
                vendas.c.preco_venda_unitario,
                vendas.c.receita_total,
                vendas.c.margem_contribuicao,
                vendas.c.origem,
                vendas.c.numero_venda_ml,
                vendas.c.lote_importacao,
                produtos.c.nome,
            )
            .select_from(vendas.join(produtos))
            .order_by(vendas.c.data_venda.desc(), vendas.c.id.desc())
        ).mappings().all()

        lotes = conn.execute(
            select(
                vendas.c.lote_importacao.label("lote_importacao"),
                func.count().label("qtd_vendas"),
                func.coalesce(func.sum(vendas.c.receita_total), 0).label("receita_lote"),
            )
            .where(vendas.c.lote_importacao.isnot(None))
            .group_by(vendas.c.lote_importacao)
            .order_by(vendas.c.lote_importacao.desc())
        ).mappings().all()

    return render_template("vendas.html", vendas=vendas_rows, lotes=lotes)

@app.route("/vendas/<int:venda_id>/editar", methods=["GET", "POST"])
def editar_venda(venda_id):
    if request.method == "POST":
        quantidade = int(request.form["quantidade"])
        preco_venda_unitario = float(request.form["preco_venda_unitario"])
        custo_total = float(request.form["custo_total"])

        receita_total = quantidade * preco_venda_unitario
        margem_contribuicao = receita_total - custo_total

        with engine.begin() as conn:
            conn.execute(
                update(vendas)
                .where(vendas.c.id == venda_id)
                .values(
                    quantidade=quantidade,
                    preco_venda_unitario=preco_venda_unitario,
                    receita_total=receita_total,
                    margem_contribuicao=margem_contribuicao,
                )
            )
        flash("Venda atualizada com sucesso!", "success")
        return redirect(url_for("lista_vendas"))

    with engine.connect() as conn:
        venda_row = conn.execute(
            select(
                vendas.c.id,
                vendas.c.data_venda,
                vendas.c.quantidade,
                vendas.c.preco_venda_unitario,
                vendas.c.custo_total,
                produtos.c.nome,
            )
            .select_from(vendas.join(produtos))
            .where(vendas.c.id == venda_id)
        ).mappings().first()

    if not venda_row:
        flash("Venda não encontrada.", "danger")
        return redirect(url_for("lista_vendas"))

    return render_template("editar_venda.html", venda=venda_row)

@app.route("/vendas/<int:venda_id>/excluir", methods=["POST"])
def excluir_venda(venda_id):
    with engine.begin() as conn:
        conn.execute(delete(vendas).where(vendas.c.id == venda_id))
    flash("Venda excluída com sucesso!", "success")
    return redirect(url_for("lista_vendas"))

@app.route("/vendas/lote/<lote_id>/excluir", methods=["POST"])
def excluir_lote_vendas(lote_id):
    with engine.begin() as conn:
        conn.execute(delete(vendas).where(vendas.c.lote_importacao == lote_id))
    flash("Lote de importação excluído com sucesso!", "success")
    return redirect(url_for("lista_vendas"))

@app.route("/importar_ml", methods=["GET", "POST"])
def importar_ml_view():
    if request.method == "POST":
        if "arquivo" not in request.files:
            flash("Nenhum arquivo enviado.", "danger")
            return redirect(request.url)
        file = request.files["arquivo"]
        if file.filename == "":
            flash("Selecione um arquivo.", "danger")
            return redirect(request.url)
        filename = secure_filename(file.filename)
        caminho = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(caminho)

        try:
            resumo = importar_vendas_ml(caminho, engine)
            flash(
                f"Importação concluída. Lote {resumo['lote_id']} - "
                f"{resumo['vendas_importadas']} vendas importadas, "
                f"{resumo['vendas_sem_sku']} sem SKU, "
                f"{resumo['vendas_sem_produto']} sem produto cadastrado.",
                "success",
            )
        except Exception as e:
            flash(f"Erro na importação: {e}", "danger")
        return redirect(url_for("importar_ml_view"))

    return render_template("importar_ml.html")

@app.route("/relatorio_lucro")
def relatorio_lucro():
    with engine.connect() as conn:
        linhas = conn.execute(
            select(
                produtos.c.nome,
                func.sum(vendas.c.quantidade).label("qtd"),
                func.sum(vendas.c.receita_total).label("receita"),
                func.sum(vendas.c.custo_total).label("custo"),
                func.sum(vendas.c.margem_contribuicao).label("margem"),
            )
            .select_from(vendas.join(produtos))
            .group_by(produtos.c.id)
            .order_by(func.sum(vendas.c.margem_contribuicao).desc())
        ).mappings().all()

    return render_template("relatorio_lucro.html", linhas=linhas)

@app.route("/estoque")
def estoque_view():
    with engine.connect() as conn:
        produtos_rows = conn.execute(
            select(
                produtos.c.nome,
                produtos.c.sku,
                produtos.c.estoque_atual,
                produtos.c.custo_unitario,
            ).order_by(produtos.c.nome)
        ).mappings().all()
    return render_template("estoque.html", produtos=produtos_rows)

@app.route("/configuracoes")
def configuracoes_view():
    return render_template("configuracoes.html")

if __name__ == "__main__":
    init_db()
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
