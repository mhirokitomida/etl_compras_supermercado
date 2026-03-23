# 🛒 Compras Supermercado

Projeto em Python para **extração, processamento e análise de dados de compras** a partir de QR Codes de notas fiscais eletrônicas (NFC-e).

O sistema implementa um **pipeline automatizado de ponta a ponta**, transformando imagens de notas em dados estruturados prontos para análise.

# 🔄 Fluxo completo

* Leitura de QR Code a partir de imagens
* Acesso automático à página da NFC-e
* Extração dos itens via web scraping
* Limpeza e padronização dos dados
* Armazenamento em banco de dados (PostgreSQL / Supabase)
* Geração de arquivos analíticos (Parquet)
* 📊 Consumo dos dados em dashboard Power BI

---

# 🚀 Funcionalidades

✔️ Leitura robusta de QR Code (com múltiplas estratégias de processamento de imagem)

✔️ Extração automática de dados diretamente da NFC-e

✔️ Estruturação completa dos itens (produto, quantidade, valores, etc.)

✔️ Persistência em banco relacional (PostgreSQL)

✔️ Geração de datasets otimizados (Parquet)

✔️ Controle de arquivos já processados (evita duplicidade)

✔️ Sistema de logs para rastreabilidade

✔️ 📊 Dashboard interativo com Power BI

---

# 📊 Dashboard (Power BI)

👉 **[Acesse o dashboard](https://app.powerbi.com/view?r=eyJrIjoiY2I3NTMyNWItODRiZS00ZTQ0LTg0MTgtMTg4ZWQxYmNlYzI5IiwidCI6IjA1MWVlYzAzLTIzM2UtNGIxZi04MDA5LWZiYWE3NTc3MTgxZiJ9)**

---

# 🧠 Tecnologias utilizadas

* Python 3.x
* pandas
* requests
* BeautifulSoup
* opencv-python
* pyzbar
* SQLAlchemy
* dotenv
* PostgreSQL / Supabase
* Power BI

---

# 📁 Estrutura do projeto

📦 projeto
┣ 📜 main.py
┣ 📂 notas_compras_supermercado/
┃ ┣ 📂 nao_processado/
┃ ┗ 📂 processado/
┣ 📂 dados_processados/
┃ ┣ 📂 compras_supermercado/
┃ ┗ 📂 produtos/
┣ 📂 logs/
┗ 📜 README.md

---

# ▶️ Como funciona

1. Adicione imagens de notas na pasta:

notas_compras_supermercado/nao_processado/

2. O sistema:

* Lê o QR Code
* Acessa a NFC-e
* Extrai os dados
* Armazena no banco
* Salva arquivos Parquet
* Move arquivos processados
* Atualiza o dashboard 

---

# 📈 Diferenciais do projeto

* Pipeline completo (ETL real)
* Integração com banco de dados
* Dados prontos para análise
* Visualização em BI

---

# ⚠️ Observações

* Algumas NFC-e podem variar de estrutura dependendo do estado
* O projeto depende da disponibilidade do site da nota fiscal
* Pode ser necessário configurar certificado SSL para integração com banco
