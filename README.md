# 🛒 Compras Supermercado

Projeto em Python para **extração, processamento e análise de compras de supermercado** a partir de QR Codes de NFC-e.

O sistema automatiza todo o fluxo:

* Leitura de QR Code
* Acesso à nota fiscal
* Extração dos itens
* Tratamento dos dados
* Armazenamento em banco
* 📊 Visualização em Power BI

---

# 🚀 Funcionalidades

✔️ Leitura de QR Code a partir de imagens
✔️ Extração automática de URL da NFC-e
✔️ Web scraping dos dados da nota fiscal
✔️ Estruturação dos itens (nome, quantidade, preço, etc.)
✔️ Integração com banco de dados (PostgreSQL / Supabase)
✔️ Pipeline automatizado
✔️ 📊 Dashboard interativo com Power BI

---

# 📊 Dashboard Exemplo (Power BI)

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

# 🔄 Fluxo do projeto

Imagem → QR Code → URL NFC-e → Scraping → DataFrame → Banco → Power BI

---

# 🗃️ Exemplo de dados

Produto | Quantidade | Preço | Total
Arroz 5kg | 1 | 25.00 | 25.00
Leite Integral | 2 | 4.50 | 9.00
