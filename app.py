import os
from datetime import datetime
from io import BytesIO

from flask import Flask, render_template, request, redirect, url_for, flash, send_file
from werkzeug.utils import secure_filename

from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Float,
    ForeignKey, func, select, insert, update, delete
)
from sqlalchemy.engine import Engine
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
# Definição das tabelas
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

ajustes_estoque = Table(
    "ajustes_estoque",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("produto_id", Integer, ForeignKey("produtos.id"), nullable=False),
    Column("data_ajuste", String(50)),
    Column("tipo", String(20)),  # entrada, saida
    Column("quantidade", Integer),
    Column("custo_unitario", Float),
    Column("observacao", String(255)),
)

configuracoes = Table(
    "configuracoes",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("imposto_percent", Float, nullable=False, server_default="0"),
    Column("despesas_percent", Float, nullable=False, server_default="0"),
)

def init_db():
    """Cria as tabelas se não existirem e garante 1 linha em configuracoes."""
    metadata.create_all(engine)
    with engine.begin() as conn:
        row = conn.execute(
            select(configuracoes.c.id).limit(1)
        ).first()
        if not row:
            conn.execute(
                insert(configuracoes).values(id=1, imposto_percent=0.0, despesas_percent=0.0)
            )

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
            titulo = str(row.get("Título do anúncio") or "").strip()

            produto_row = None

            if sku:
                produto_row = conn.execute(
                    select(produtos.c.id, produtos.c.custo_unitario)
                    .where(produtos.c.sku == sku)
                ).mappings().first()
            else:
                # tenta pelo nome do produto = título do anúncio
                if titulo:
                    produto_row = conn.execute(
                        select(produtos.c.id, produtos.c.custo_unitario)
                        .where(produtos.c.nome == titulo)
                    ).mappings().first()

            if not sku and not produto_row:
                vendas_sem_sku += 1
                continue

            if not produto_row:
                vendas_sem_produto += 1
                continue

            produto_id = produto_row["id"]
            custo_unitario = float(produto_row["custo_unitario"] or 0.0)

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


def parse_brl(valor):
    """Converte valores no formato brasileiro (R$ 1.234,56) para float."""
    if valor is None:
        return 0.0
    # Se já for número
    if isinstance(valor, (int, float)):
        try:
            if pd.isna(valor):
                return 0.0
        except Exception:
            pass
        return float(valor)
    s = str(valor).strip()
    if not s:
        return 0.0
    s = s.replace("R$", "").replace("\u00a0", "").replace(" ", "")
    # remove separador de milhar e troca vírgula por ponto
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def importar_vendas_template(caminho_arquivo, engine: Engine):
    """Importa vendas a partir do template consolidado (SKU, Título, Quantidade, Receita, Comissao, PrecoMedio)."""
    lote_id = datetime.now().isoformat(timespec="seconds")

    # tenta ler a aba 'Template'; se não existir, usa a primeira
    try:
        df = pd.read_excel(caminho_arquivo, sheet_name="Template")
    except Exception:
        df = pd.read_excel(caminho_arquivo, sheet_name=0)

    colunas_obrig = {"SKU", "Título", "Quantidade", "Receita", "Comissao", "PrecoMedio"}
    if not colunas_obrig.issubset(set(df.columns)):
        raise ValueError("Planilha não está no formato esperado: colunas 'SKU, Título, Quantidade, Receita, Comissao, PrecoMedio' são obrigatórias.")

    vendas_importadas = 0
    vendas_sem_sku = 0
    vendas_sem_produto = 0

    with engine.begin() as conn:
        for _, row in df.iterrows():
            sku = str(row.get("SKU") or "").strip()
            titulo = str(row.get("Título") or "").strip()

            qtd_raw = row.get("Quantidade")
            try:
                quantidade = int(qtd_raw) if qtd_raw == qtd_raw else 0
            except Exception:
                quantidade = 0

            if quantidade <= 0:
                continue

            receita_total = parse_brl(row.get("Receita"))
            comissao = parse_brl(row.get("Comissao"))

            # Encontrar produto
            produto_row = None
            if sku:
                produto_row = conn.execute(
                    select(produtos.c.id, produtos.c.custo_unitario)
                    .where(produtos.c.sku == sku)
                ).mappings().first()
            if not produto_row and titulo:
                produto_row = conn.execute(
                    select(produtos.c.id, produtos.c.custo_unitario)
                    .where(produtos.c.nome == titulo)
                ).mappings().first()

            if not sku and not produto_row:
                vendas_sem_sku += 1
                continue

            if not produto_row:
                vendas_sem_produto += 1
                continue

            produto_id = produto_row["id"]
            custo_unitario = float(produto_row["custo_unitario"] or 0.0)

            custo_total = custo_unitario * quantidade

            # margem antes da comissão
            margem_bruta = receita_total - custo_total
            # Opção B: reduzir margem pela comissão
            margem_contribuicao = margem_bruta - comissao

            # Opção 2: ignorar PrecoMedio da planilha, calcular pelo total / quantidade
            preco_venda_unitario = receita_total / quantidade if quantidade > 0 else 0.0

            conn.execute(
                insert(vendas).values(
                    produto_id=produto_id,
                    data_venda=datetime.now().isoformat(),
                    quantidade=quantidade,
                    preco_venda_unitario=preco_venda_unitario,
                    receita_total=receita_total,
                    custo_total=custo_total,
                    margem_contribuicao=margem_contribuicao,
                    origem="Template",
                    numero_venda_ml=None,
                    lote_importacao=lote_id,
                )
            )

            conn.execute(
                update(produtos)
                .where(produtos.c.id == produto_id)
                .values(estoque_atual=produtos.c.estoque_atual - quantidade)
            )

            vendas_importadas += 1

    return {
        "lote_id": lote_id,
        "vendas_importadas": vendas_importadas,
        "vendas_sem_sku": vendas_sem_sku,
        "vendas_sem_produto": vendas_sem_produto,
    }

# --------------------------------------------------------------------
# Rotas principais

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

        produto_mais_vendido = conn.execute(
            select(produtos.c.nome, func.sum(vendas.c.quantidade).label("qtd"))
            .select_from(vendas.join(produtos))
            .group_by(produtos.c.id)
            .order_by(func.sum(vendas.c.quantidade).desc())
            .limit(1)
        ).first()

        produto_maior_lucro = conn.execute(
            select(produtos.c.nome, func.sum(vendas.c.margem_contribuicao).label("lucro"))
            .select_from(vendas.join(produtos))
            .group_by(produtos.c.id)
            .order_by(func.sum(vendas.c.margem_contribuicao).desc())
            .limit(1)
        ).first()

        produto_pior_margem = conn.execute(
            select(produtos.c.nome, func.sum(vendas.c.margem_contribuicao).label("margem"))
            .select_from(vendas.join(produtos))
            .group_by(produtos.c.id)
            .order_by(func.sum(vendas.c.margem_contribuicao).asc())
            .limit(1)
        ).first()

        cfg = conn.execute(select(configuracoes).where(configuracoes.c.id == 1)).mappings().first()

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
        cfg=cfg,
    )

# ---------------- PRODUTOS ----------------
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

# ---------------- VENDAS ----------------
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

        produtos_rows = conn.execute(
            select(produtos.c.id, produtos.c.nome).order_by(produtos.c.nome)
        ).mappings().all()

    return render_template("vendas.html", vendas=vendas_rows, lotes=lotes, produtos=produtos_rows)

@app.route("/vendas/manual", methods=["POST"])
def criar_venda_manual():
    produto_id = int(request.form["produto_id"])
    quantidade = int(request.form.get("quantidade", 0) or 0)
    preco_unit = float(request.form.get("preco_venda_unitario", 0) or 0)
    data_venda_str = request.form.get("data_venda") or datetime.now().isoformat()

    with engine.begin() as conn:
        prod = conn.execute(
            select(produtos.c.custo_unitario).where(produtos.c.id == produto_id)
        ).mappings().first()
        custo_unitario = float(prod["custo_unitario"] or 0.0) if prod else 0.0

        receita_total = quantidade * preco_unit
        custo_total = quantidade * custo_unitario
        margem_contribuicao = receita_total - custo_total

        conn.execute(
            insert(vendas).values(
                produto_id=produto_id,
                data_venda=data_venda_str,
                quantidade=quantidade,
                preco_venda_unitario=preco_unit,
                receita_total=receita_total,
                custo_total=custo_total,
                margem_contribuicao=margem_contribuicao,
                origem="Manual",
                numero_venda_ml=None,
                lote_importacao=None,
            )
        )

        conn.execute(
            update(produtos)
            .where(produtos.c.id == produto_id)
            .values(estoque_atual=produtos.c.estoque_atual - quantidade)
        )

    flash("Venda manual registrada com sucesso!", "success")
    return redirect(url_for("lista_vendas"))

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

# ---------------- IMPORT / EXPORT ----------------
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
                f"{resumo['vendas_sem_sku']} sem SKU/Título, "
                f"{resumo['vendas_sem_produto']} sem produto cadastrado.",
                "success",
            )
        except Exception as e:
            flash(f"Erro na importação: {e}", "danger")
        return redirect(url_for("importar_ml_view"))

    return render_template("importar_ml.html")


@app.route("/importar_template", methods=["POST"])
def importar_template():
    """Importa vendas a partir do template consolidado preenchido manualmente."""
    if "arquivo_template" not in request.files:
        flash("Nenhum arquivo enviado para o template.", "danger")
        return redirect(url_for("importar_ml_view"))
    file = request.files["arquivo_template"]
    if file.filename == "":
        flash("Selecione um arquivo para o template.", "danger")
        return redirect(url_for("importar_ml_view"))
    filename = secure_filename(file.filename)
    caminho = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    file.save(caminho)

    try:
        resumo = importar_vendas_template(caminho, engine)
        flash(
            f"Template importado. Lote {resumo['lote_id']} - "
            f"{resumo['vendas_importadas']} vendas importadas, "
            f"{resumo['vendas_sem_sku']} linhas sem SKU/Título, "
            f"{resumo['vendas_sem_produto']} sem produto cadastrado.",
            "success",
        )
    except Exception as e:
        flash(f"Erro na importação pelo template: {e}", "danger")
    return redirect(url_for("importar_ml_view"))


@app.route("/exportar_consolidado")
def exportar_consolidado():
    """Exporta planilha de consolidação das vendas."""
    with engine.connect() as conn:
        rows = conn.execute(
            select(
                vendas.c.id.label("ID Venda"),
                vendas.c.data_venda.label("Data venda"),
                produtos.c.nome.label("Produto"),
                produtos.c.sku.label("SKU"),
                vendas.c.quantidade.label("Quantidade"),
                vendas.c.preco_venda_unitario.label("Preço unitário"),
                vendas.c.receita_total.label("Receita total"),
                vendas.c.custo_total.label("Custo total"),
                vendas.c.margem_contribuicao.label("Margem contribuição"),
                vendas.c.origem.label("Origem"),
                vendas.c.numero_venda_ml.label("Nº venda ML"),
                vendas.c.lote_importacao.label("Lote importação"),
            ).select_from(vendas.join(produtos))
        ).mappings().all()

    df = pd.DataFrame(rows)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Consolidado")
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name=f"consolidado_vendas_{datetime.now().date()}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )



@app.route("/exportar_template")
def exportar_template():
    """Exporta o modelo de planilha para preenchimento manual (SKU, Título, Quantidade, Receita, Comissao, PrecoMedio)."""
    cols = ["SKU", "Título", "Quantidade", "Receita", "Comissao", "PrecoMedio"]
    df = pd.DataFrame(columns=cols)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Template")
    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name="template_consolidacao_vendas.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# ---------------- ESTOQUE / AJUSTES ----------------
@app.route("/estoque")
def estoque_view():
    with engine.connect() as conn:
        produtos_rows = conn.execute(
            select(
                produtos.c.id,
                produtos.c.nome,
                produtos.c.sku,
                produtos.c.estoque_atual,
                produtos.c.custo_unitario,
            ).order_by(produtos.c.nome)
        ).mappings().all()
    return render_template("estoque.html", produtos=produtos_rows)

@app.route("/estoque/ajuste", methods=["POST"])
def ajuste_estoque():
    produto_id = int(request.form["produto_id"])
    tipo = request.form["tipo"]  # entrada ou saida
    quantidade = int(request.form.get("quantidade", 0) or 0)
    custo_unitario = request.form.get("custo_unitario")
    observacao = request.form.get("observacao") or ""

    custo_unitario_val = float(custo_unitario) if custo_unitario not in (None, "",) else None

    fator = 1 if tipo == "entrada" else -1

    with engine.begin() as conn:
        if custo_unitario_val is not None:
            conn.execute(
                update(produtos)
                .where(produtos.c.id == produto_id)
                .values(custo_unitario=custo_unitario_val)
            )

        conn.execute(
            update(produtos)
            .where(produtos.c.id == produto_id)
            .values(estoque_atual=produtos.c.estoque_atual + fator * quantidade)
        )

        conn.execute(
            insert(ajustes_estoque).values(
                produto_id=produto_id,
                data_ajuste=datetime.now().isoformat(),
                tipo=tipo,
                quantidade=quantidade,
                custo_unitario=custo_unitario_val,
                observacao=observacao,
            )
        )

    flash("Ajuste de estoque registrado!", "success")
    return redirect(url_for("estoque_view"))

# ---------------- CONFIGURAÇÕES ----------------
@app.route("/configuracoes", methods=["GET", "POST"])
def configuracoes_view():
    if request.method == "POST":
        imposto_percent = float(request.form.get("imposto_percent", 0) or 0)
        despesas_percent = float(request.form.get("despesas_percent", 0) or 0)
        with engine.begin() as conn:
            conn.execute(
                update(configuracoes)
                .where(configuracoes.c.id == 1)
                .values(imposto_percent=imposto_percent, despesas_percent=despesas_percent)
            )
        flash("Configurações salvas!", "success")
        return redirect(url_for("configuracoes_view"))

    with engine.connect() as conn:
        cfg = conn.execute(
            select(configuracoes).where(configuracoes.c.id == 1)
        ).mappings().first()

    return render_template("configuracoes.html", cfg=cfg)

# ---------------- RELATÓRIO LUCRO ----------------

@app.route("/relatorio_lucro")
def relatorio_lucro():
    with engine.connect() as conn:
        linhas_db = conn.execute(
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

        cfg = conn.execute(
            select(configuracoes).where(configuracoes.c.id == 1)
        ).mappings().first()

    imposto_percent = float(cfg["imposto_percent"] or 0) if cfg else 0.0
    despesas_percent = float(cfg["despesas_percent"] or 0) if cfg else 0.0

    linhas = []
    total_qtd = total_receita = total_custo = total_margem = 0.0
    total_impostos = total_despesas = total_lucro_liquido = 0.0

    for row in linhas_db:
        receita = float(row["receita"] or 0)
        custo = float(row["custo"] or 0)
        margem = float(row["margem"] or 0)
        qtd = int(row["qtd"] or 0)

        impostos = receita * imposto_percent / 100.0
        despesas = receita * despesas_percent / 100.0
        lucro_liquido = margem - impostos - despesas

        linhas.append({
            "nome": row["nome"],
            "qtd": qtd,
            "receita": receita,
            "custo": custo,
            "margem": margem,
            "impostos": impostos,
            "despesas": despesas,
            "lucro_liquido": lucro_liquido,
        })

        total_qtd += qtd
        total_receita += receita
        total_custo += custo
        total_margem += margem
        total_impostos += impostos
        total_despesas += despesas
        total_lucro_liquido += lucro_liquido

    totais = {
        "qtd": total_qtd,
        "receita": total_receita,
        "custo": total_custo,
        "margem": total_margem,
        "impostos": total_impostos,
        "despesas": total_despesas,
        "lucro_liquido": total_lucro_liquido,
    }

    return render_template("relatorio_lucro.html", linhas=linhas, totais=totais,
                           imposto_percent=imposto_percent, despesas_percent=despesas_percent)


if __name__ == "__main__":
    init_db()
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
