import os
from datetime import datetime, date

import pandas as pd
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
)
from werkzeug.utils import secure_filename

from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    Date,
    DateTime,
    select,
    insert,
    update,
    func,
)
from sqlalchemy.engine import Engine

# ----------------------------------------------------------------------
# Configuração básica
# ----------------------------------------------------------------------

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///metrifiy5_1.db")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret")
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

engine: Engine = create_engine(DATABASE_URL, future=True)
metadata = MetaData()

# ----------------------------------------------------------------------
# Tabelas
# ----------------------------------------------------------------------

produtos = Table(
    "produtos",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("nome", String, nullable=False),
    Column("sku", String, nullable=True, unique=True),
    Column("custo_unitario", Float, default=0.0),
    Column("estoque_atual", Integer, default=0),
)

vendas = Table(
    "vendas",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("produto_id", Integer, nullable=False),
    Column("data_venda", Date, nullable=True),
    Column("quantidade", Integer, default=0),
    Column("preco_venda_unitario", Float, default=0.0),
    Column("receita_total", Float, default=0.0),
    Column("custo_total", Float, default=0.0),
    Column("margem_contribuicao", Float, default=0.0),  # já pós comissão
    Column("origem", String, default="Manual"),
    Column("numero_venda_ml", String, nullable=True),
    Column("lote_importacao", String, nullable=True),
    Column("criado_em", DateTime, default=datetime.utcnow),
)

configuracoes = Table(
    "configuracoes",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("imposto_percent", Float, default=0.0),
    Column("despesas_percent", Float, default=0.0),
)

metadata.create_all(engine)

# garante um único registro em configuracoes
with engine.begin() as conn:
    qtd_cfg = conn.execute(
        select(func.count()).select_from(configuracoes)
    ).scalar_one()
    if qtd_cfg == 0:
        conn.execute(
            insert(configuracoes).values(
                id=1,
                imposto_percent=0.0,
                despesas_percent=0.0,
            )
        )

# ----------------------------------------------------------------------
# Funções auxiliares
# ----------------------------------------------------------------------


def parse_brl(valor):
    """Converte 'R$ 1.234,56' ou '1234,56' em float."""
    if valor is None:
        return 0.0
    if isinstance(valor, (int, float)):
        try:
            import math as _math
            if hasattr(_math, "isnan") and _math.isnan(valor):
                return 0.0
        except Exception:
            pass
        return float(valor)
    s = str(valor).strip()
    if not s:
        return 0.0
    s = s.replace("R$", "").replace("\u00a0", "").replace(" ", "")
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def parse_data_venda(valor):
    if isinstance(valor, date):
        return valor
    if isinstance(valor, datetime):
        return valor.date()
    if valor is None:
        return None
    s = str(valor).strip()
    if not s:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None


# ----------------------------------------------------------------------
# Importação Vendas BR Mercado Livre
# ----------------------------------------------------------------------


def importar_vendas_ml(caminho_arquivo: str, engine: Engine):
    """
    Lê a aba 'Vendas BR' do relatório oficial do Mercado Livre.

    Usa:
      - coluna H: 'Receita por produtos (BRL)'  -> receita bruta
      - coluna K: 'Tarifa de venda e impostos (BRL)' -> comissão (normalmente negativa)
    """
    lote_id = datetime.now().isoformat(timespec="seconds")

    df = pd.read_excel(
        caminho_arquivo,
        sheet_name="Vendas BR",
        header=5,
    )

    if "N.º de venda" not in df.columns:
        raise ValueError(
            "Planilha não está no formato esperado: coluna 'N.º de venda' não encontrada."
        )

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
                    select(produtos.c.id, produtos.c.custo_unitario).where(
                        produtos.c.sku == sku
                    )
                ).mappings().first()
            if not produto_row and titulo:
                produto_row = conn.execute(
                    select(produtos.c.id, produtos.c.custo_unitario).where(
                        produtos.c.nome == titulo
                    )
                ).mappings().first()

            if not sku and not produto_row:
                vendas_sem_sku += 1
                continue

            if not produto_row:
                vendas_sem_produto += 1
                continue

            produto_id = produto_row["id"]
            custo_unitario = float(produto_row["custo_unitario"] or 0.0)

            data_venda = parse_data_venda(row.get("Data da venda"))
            unidades = row.get("Unidades")
            try:
                unidades = int(unidades) if unidades == unidades else 0
            except Exception:
                unidades = 0

            # Receita BRUTA (coluna H)
            receita_total = parse_brl(row.get("Receita por produtos (BRL)"))
            # Comissão / tarifas (coluna K - normalmente negativa)
            comissao_val = parse_brl(row.get("Tarifa de venda e impostos (BRL)"))

            preco_venda_unitario = receita_total / unidades if unidades > 0 else 0.0
            custo_total = custo_unitario * unidades

            margem_bruta = receita_total - custo_total
            margem_contribuicao = margem_bruta + comissao_val  # comissao_val negativa

            numero_venda_ml = str(row.get("N.º de venda"))

            conn.execute(
                insert(vendas).values(
                    produto_id=produto_id,
                    data_venda=data_venda,
                    quantidade=unidades,
                    preco_venda_unitario=preco_venda_unitario,
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


# ----------------------------------------------------------------------
# Importação de template consolidado
# ----------------------------------------------------------------------


def importar_vendas_template(caminho_arquivo: str, engine: Engine):
    """
    Lê um arquivo no formato:
      SKU | Título | Quantidade | Receita | Comissao | PrecoMedio
    """
    lote_id = datetime.now().isoformat(timespec="seconds")

    try:
        df = pd.read_excel(caminho_arquivo, sheet_name="Template")
    except Exception:
        df = pd.read_excel(caminho_arquivo)

    colunas_obrig = {"SKU", "Título", "Quantidade", "Receita", "Comissao", "PrecoMedio"}
    if not colunas_obrig.issubset(set(df.columns)):
        raise ValueError(
            "Planilha não está no formato esperado. Colunas obrigatórias: "
            "'SKU, Título, Quantidade, Receita, Comissao, PrecoMedio'."
        )

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
            comissao_val = parse_brl(row.get("Comissao"))

            produto_row = None
            if sku:
                produto_row = conn.execute(
                    select(produtos.c.id, produtos.c.custo_unitario).where(
                        produtos.c.sku == sku
                    )
                ).mappings().first()
            if not produto_row and titulo:
                produto_row = conn.execute(
                    select(produtos.c.id, produtos.c.custo_unitario).where(
                        produtos.c.nome == titulo
                    )
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
            margem_bruta = receita_total - custo_total
            margem_contribuicao = margem_bruta - comissao_val  # aqui comissao positiva

            preco_venda_unitario = receita_total / quantidade if quantidade > 0 else 0.0

            conn.execute(
                insert(vendas).values(
                    produto_id=produto_id,
                    data_venda=date.today(),
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


# ----------------------------------------------------------------------
# Dashboard
# ----------------------------------------------------------------------


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

        margem_media = 0.0
        if receita_total > 0:
            margem_media = (lucro_total / receita_total) * 100.0

        ticket_medio = conn.execute(
            select(func.coalesce(func.avg(vendas.c.preco_venda_unitario), 0))
        ).scalar_one()

        # comissão total = (receita - custo) - margem (margem já pós comissão)
        comissao_total = conn.execute(
            select(
                func.coalesce(
                    func.sum(
                        vendas.c.receita_total
                        - vendas.c.custo_total
                        - vendas.c.margem_contribuicao
                    ),
                    0,
                )
            )
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

        cfg = conn.execute(
            select(configuracoes).where(configuracoes.c.id == 1)
        ).mappings().first()

    return render_template(
        "dashboard.html",
        total_produtos=total_produtos,
        estoque_total=estoque_total,
        receita_total=receita_total,
        lucro_total=lucro_total,
        margem_media=margem_media,
        ticket_medio=ticket_medio,
        comissao_total=comissao_total,
        produto_mais_vendido=produto_mais_vendido,
        produto_maior_lucro=produto_maior_lucro,
        produto_pior_margem=produto_pior_margem,
        cfg=cfg,
    )


# ----------------------------------------------------------------------
# Produtos
# ----------------------------------------------------------------------


@app.route("/produtos")
def lista_produtos():
    with engine.connect() as conn:
        rows = conn.execute(
            select(produtos).order_by(produtos.c.nome)
        ).mappings().all()
    return render_template("produtos.html", produtos=rows)


@app.route("/produtos/novo", methods=["GET", "POST"])
def novo_produto():
    if request.method == "POST":
        nome = request.form.get("nome", "").strip()
        sku = request.form.get("sku", "").strip()
        custo = parse_brl(request.form.get("custo_unitario"))
        estoque_inicial = int(request.form.get("estoque_inicial") or 0)

        if not nome:
            flash("Nome é obrigatório.", "danger")
            return redirect(url_for("novo_produto"))

        with engine.begin() as conn:
            conn.execute(
                insert(produtos).values(
                    nome=nome,
                    sku=sku or None,
                    custo_unitario=custo,
                    estoque_atual=estoque_inicial,
                )
            )

        flash("Produto cadastrado.", "success")
        return redirect(url_for("lista_produtos"))

    return render_template("produto_form.html", produto=None)


@app.route("/produtos/<int:produto_id>/editar", methods=["GET", "POST"])
def editar_produto(produto_id):
    with engine.connect() as conn:
        produto = conn.execute(
            select(produtos).where(produtos.c.id == produto_id)
        ).mappings().first()

    if not produto:
        flash("Produto não encontrado.", "danger")
        return redirect(url_for("lista_produtos"))

    if request.method == "POST":
        nome = request.form.get("nome", "").strip()
        sku = request.form.get("sku", "").strip()
        custo = parse_brl(request.form.get("custo_unitario"))

        if not nome:
            flash("Nome é obrigatório.", "danger")
            return redirect(url_for("editar_produto", produto_id=produto_id))

        with engine.begin() as conn:
            conn.execute(
                update(produtos)
                .where(produtos.c.id == produto_id)
                .values(nome=nome, sku=sku or None, custo_unitario=custo)
            )

        flash("Produto atualizado.", "success")
        return redirect(url_for("lista_produtos"))

    return render_template("produto_form.html", produto=produto)


# ----------------------------------------------------------------------
# Estoque - ajustes
# ----------------------------------------------------------------------


@app.route("/estoque", methods=["GET", "POST"])
def ajustes_estoque():
    with engine.connect() as conn:
        lista = conn.execute(
            select(produtos).order_by(produtos.c.nome)
        ).mappings().all()

    if request.method == "POST":
        produto_id = int(request.form.get("produto_id") or 0)
        tipo = request.form.get("tipo")
        quantidade = int(request.form.get("quantidade") or 0)

        if not produto_id or not tipo or quantidade <= 0:
            flash("Preencha produto, tipo e quantidade.", "danger")
            return redirect(url_for("ajustes_estoque"))

        delta = quantidade if tipo == "entrada" else -quantidade

        with engine.begin() as conn:
            conn.execute(
                update(produtos)
                .where(produtos.c.id == produto_id)
                .values(estoque_atual=produtos.c.estoque_atual + delta)
            )

        flash("Ajuste de estoque registrado.", "success")
        return redirect(url_for("ajustes_estoque"))

    return render_template("estoque.html", produtos=lista)


# ----------------------------------------------------------------------
# Importar vendas (ML / Template)
# ----------------------------------------------------------------------


@app.route("/importar_ml", methods=["GET", "POST"])
def importar_ml_view():
    if request.method == "POST":
        tipo = request.form.get("tipo")  # "ml" ou "template"
        file = request.files.get("arquivo")

        if not file or file.filename == "":
            flash("Selecione um arquivo.", "danger")
            return redirect(url_for("importar_ml_view"))

        filename = secure_filename(file.filename)
        caminho = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(caminho)

        try:
            if tipo == "ml":
                resumo = importar_vendas_ml(caminho, engine)
                flash(
                    f"Importação Vendas BR concluída. Lote {resumo['lote_id']} - "
                    f"{resumo['vendas_importadas']} vendas importadas, "
                    f"{resumo['vendas_sem_sku']} sem SKU, "
                    f"{resumo['vendas_sem_produto']} sem produto cadastrado.",
                    "success",
                )
            elif tipo == "template":
                resumo = importar_vendas_template(caminho, engine)
                flash(
                    f"Importação template concluída. Lote {resumo['lote_id']} - "
                    f"{resumo['vendas_importadas']} vendas importadas, "
                    f"{resumo['vendas_sem_sku']} sem SKU, "
                    f"{resumo['vendas_sem_produto']} sem produto cadastrado.",
                    "success",
                )
            else:
                flash("Tipo de importação inválido.", "danger")
        except Exception as e:
            flash(f"Erro na importação: {e}", "danger")

        return redirect(url_for("importar_ml_view"))

    return render_template("importar_ml.html")


# ----------------------------------------------------------------------
# Vendas - manual
# ----------------------------------------------------------------------


@app.route("/vendas")
def lista_vendas():
    with engine.connect() as conn:
        rows = conn.execute(
            select(
                vendas.c.id,
                vendas.c.data_venda,
                vendas.c.quantidade,
                vendas.c.receita_total,
                vendas.c.custo_total,
                vendas.c.margem_contribuicao,
                vendas.c.origem,
                produtos.c.nome.label("produto_nome"),
            )
            .select_from(vendas.join(produtos))
            .order_by(vendas.c.data_venda.desc(), vendas.c.id.desc())
        ).mappings().all()
    return render_template("vendas.html", vendas=rows)


@app.route("/vendas/nova", methods=["GET", "POST"])
def nova_venda():
    with engine.connect() as conn:
        lista_prod = conn.execute(
            select(produtos).order_by(produtos.c.nome)
        ).mappings().all()

    if request.method == "POST":
        produto_id = int(request.form.get("produto_id") or 0)
        data_venda = parse_data_venda(request.form.get("data_venda")) or date.today()
        quantidade = int(request.form.get("quantidade") or 0)
        preco_unitario = parse_brl(request.form.get("preco_unitario"))

        if not produto_id or quantidade <= 0 or preco_unitario <= 0:
            flash("Preencha produto, quantidade e preço.", "danger")
            return redirect(url_for("nova_venda"))

        with engine.begin() as conn:
            prod = conn.execute(
                select(produtos.c.custo_unitario).where(produtos.c.id == produto_id)
            ).first()
            if not prod:
                flash("Produto não encontrado.", "danger")
                return redirect(url_for("nova_venda"))

            custo_unitario = float(prod[0] or 0.0)

            receita_total = preco_unitario * quantidade
            custo_total = custo_unitario * quantidade
            margem_contribuicao = receita_total - custo_total  # sem comissão

            conn.execute(
                insert(vendas).values(
                    produto_id=produto_id,
                    data_venda=data_venda,
                    quantidade=quantidade,
                    preco_venda_unitario=preco_unitario,
                    receita_total=receita_total,
                    custo_total=custo_total,
                    margem_contribuicao=margem_contribuicao,
                    origem="Manual",
                )
            )

            conn.execute(
                update(produtos)
                .where(produtos.c.id == produto_id)
                .values(estoque_atual=produtos.c.estoque_atual - quantidade)
            )

        flash("Venda registrada.", "success")
        return redirect(url_for("lista_vendas"))

    return render_template("venda_form.html", produtos=lista_prod)


# ----------------------------------------------------------------------
# Relatório de lucro
# ----------------------------------------------------------------------


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
    total_comissao = total_impostos = total_despesas = total_lucro_liquido = 0.0

    for row in linhas_db:
        receita = float(row["receita"] or 0.0)
        custo = float(row["custo"] or 0.0)
        margem = float(row["margem"] or 0.0)  # já pós comissão
        qtd = int(row["qtd"] or 0)

        # comissão positiva: (receita - custo) - margem_pós_comissão
        comissao = (receita - custo) - margem
        if comissao < 0:
            comissao = 0.0

        receita_liquida = receita - comissao

        # imposto sobre valor bruto
        impostos = receita * imposto_percent / 100.0
        # despesas sobre valor líquido
        despesas = receita_liquida * despesas_percent / 100.0

        lucro_liquido = margem - impostos - despesas

        linhas.append(
            {
                "nome": row["nome"],
                "qtd": qtd,
                "receita": receita,
                "custo": custo,
                "margem": margem,
                "comissao": comissao,
                "receita_liquida": receita_liquida,
                "impostos": impostos,
                "despesas": despesas,
                "lucro_liquido": lucro_liquido,
            }
        )

        total_qtd += qtd
        total_receita += receita
        total_custo += custo
        total_margem += margem
        total_comissao += comissao
        total_impostos += impostos
        total_despesas += despesas
        total_lucro_liquido += lucro_liquido

    totais = {
        "qtd": total_qtd,
        "receita": total_receita,
        "custo": total_custo,
        "margem": total_margem,
        "comissao": total_comissao,
        "impostos": total_impostos,
        "despesas": total_despesas,
        "lucro_liquido": total_lucro_liquido,
    }

    return render_template(
        "relatorio_lucro.html",
        linhas=linhas,
        totais=totais,
        imposto_percent=imposto_percent,
        despesas_percent=despesas_percent,
    )


# ----------------------------------------------------------------------
# Configurações
# ----------------------------------------------------------------------


@app.route("/configuracoes", methods=["GET", "POST"])
def configuracoes_view():
    if request.method == "POST":
        imposto = parse_brl(request.form.get("imposto_percent"))
        despesas = parse_brl(request.form.get("despesas_percent"))

        with engine.begin() as conn:
            conn.execute(
                update(configuracoes)
                .where(configuracoes.c.id == 1)
                .values(imposto_percent=imposto, despesas_percent=despesas)
            )

        flash("Configurações salvas.", "success")
        return redirect(url_for("configuracoes_view"))

    with engine.connect() as conn:
        cfg = conn.execute(
            select(configuracoes).where(configuracoes.c.id == 1)
        ).mappings().first()

    return render_template("configuracoes.html", cfg=cfg)


# ----------------------------------------------------------------------
# Main (para rodar local)
# ----------------------------------------------------------------------


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
