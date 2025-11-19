# MetriFy 5.0 — versão Postgres

ERP simples em Flask para controle de produtos, estoque, importação de vendas do Mercado Livre e relatório de lucro.

## Banco de dados

- Em desenvolvimento: usa SQLite por padrão (`sqlite:///metrifiy.db`).
- Em produção (Render / Railway / etc.): defina a variável `DATABASE_URL` apontando para o Postgres.
  - Exemplo: `DATABASE_URL=postgresql://usuario:senha@host:5432/dbname`

## Rodar local

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Acesse http://localhost:5000
