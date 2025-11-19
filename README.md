# MetriFy 5.1 — Postgres + vendas manuais + ajustes de estoque

- Dashboard com métricas
- Cadastro de produtos
- Estoque com ajustes (entrada/saída e custo)
- Importar vendas do Mercado Livre (tenta por SKU e por título)
- Exportar consolidação em .xlsx
- Vendas com inclusão manual
- Relatório de lucro por produto
- Configurações (imposto e despesas em % sobre receita)

## Banco de dados

- Desenvolvimento: SQLite (`sqlite:///metrifiy.db`)
- Produção: defina `DATABASE_URL` apontando para Postgres, exemplo:

`DATABASE_URL=postgresql://usuario:senha@host:5432/dbname`

## Rodar local

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Acesse http://localhost:5000
