# -*- coding: utf-8 -*-
"""
Editor Spyder

Este é um arquivo de script temporário.
"""

# =============================
# BIBLIOTECAS
# =============================
import pandas as pd
import os
import re
import cv2
from pyzbar.pyzbar import decode
import shutil

import requests
from bs4 import BeautifulSoup

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# =============================
# CONEXÃO COM POSTGRES (SUPABASE)
# =============================
load_dotenv()

db_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

print("Conectado ao PostgreSQL com sucesso!")

# =============================
# LEITURA DE QR CODE
# =============================
def ler_qr_e_retornar_url(caminho_img):
    img = cv2.imread(caminho_img)

    img = cv2.resize(img, None, fx=3, fy=3, interpolation=cv2.INTER_CUBIC)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    tentativas = []

    tentativas.append(gray)

    _, th = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    tentativas.append(th)

    blur = cv2.GaussianBlur(gray, (5, 5), 0)
    _, th2 = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    tentativas.append(th2)

    for tentativa in tentativas:
        resultados = decode(tentativa)

        if resultados:
            texto = resultados[0].data.decode("utf-8").strip()

            if texto.startswith("http"):
                return texto
            else:
                return texto

    return None

# =============================
# EXTRAÇÃO NFC-e
# =============================
def extrair_itens_nfce(url):
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    itens = []

    for tr in soup.find_all("tr"):
        try:
            nome = tr.find("span", class_="txtTit").get_text(strip=True)

            qtd = tr.find("span", class_="Rqtd").get_text(strip=True)
            qtd = qtd.replace("Qtde.:", "").strip()

            unidade = tr.find("span", class_="RUN").get_text(strip=True)
            unidade = unidade.replace("UN:", "").strip()

            vl_unit = tr.find("span", class_="RvlUnit").get_text(strip=True)
            vl_unit = vl_unit.replace("Vl. Unit.:", "").strip()

            vl_total = tr.find("span", class_="valor").get_text(strip=True)

            itens.append({
                "produto": nome,
                "quantidade": qtd,
                "unidade": unidade,
                "valor_unitario": vl_unit,
                "valor_total": vl_total
            })

        except AttributeError:
            continue

    df_itens = pd.DataFrame(itens)

    try:
        fonte = soup.find("div", id="u20").get_text(strip=True)
    except:
        fonte = None

    data_emissao = pd.NaT
    try:
        tag_emissao = soup.find("strong", string=lambda x: x and "Emissão" in x)

        if tag_emissao:
            texto = tag_emissao.next_sibling

            if texto:
                texto = str(texto).strip()
                data_str = texto.split("-")[0].strip()
                data_emissao = pd.to_datetime(data_str, dayfirst=True)
    except:
        pass

    try:
        chave = soup.find("span", class_="chave").get_text(strip=True)
        chave = re.sub(r"\s+", "", chave)
    except:
        chave = None

    df_nota = pd.DataFrame([{
        "fonte": fonte,
        "data": data_emissao,
        "chave_acesso": chave,
        "url_qr": url
    }])

    return df_itens, df_nota

# =============================
# PIPELINE PRINCIPAL
# =============================
def processar_notas(
    pasta_entrada="notas_compras_supermercado/nao_processado",
    pasta_processado="notas_compras_supermercado/processado",
    pasta_dados="dados_processados/compras_supermercado",
    pasta_produtos="dados_processados/produtos"
):

    os.makedirs(pasta_processado, exist_ok=True)
    os.makedirs(pasta_dados, exist_ok=True)
    os.makedirs(pasta_produtos, exist_ok=True)

    arquivos = sorted(os.listdir(pasta_entrada))

    for arquivo in arquivos:
        caminho_arquivo = os.path.join(pasta_entrada, arquivo)

        # =============================
        # VERIFICA SE JÁ FOI PROCESSADO
        # =============================
        with db_engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1 FROM notas_processadas WHERE nome_arquivo = :nome"),
                {"nome": arquivo}
            ).fetchone()

        if result:
            print(f"Já processado: {arquivo}")
            continue

        print(f"Processando: {arquivo}")

        url = ler_qr_e_retornar_url(caminho_arquivo)

        if not url:
            print(f"Não conseguiu ler QR: {arquivo}")
            continue

        df_itens, df_nota = extrair_itens_nfce(url)

        if df_itens.empty:
            print(f"Sem dados: {arquivo}")
            continue

        data = df_nota["data"].iloc[0]

        df_itens["data"] = data
        df_itens["arquivo_origem"] = arquivo
        df_nota["nome_arquivo"] = arquivo

        def limpar_numero(valor):
            return float(valor.replace(".", "").replace(",", "."))

        df_itens["quantidade"] = df_itens["quantidade"].apply(limpar_numero)
        df_itens["valor_unitario"] = df_itens["valor_unitario"].apply(limpar_numero)
        df_itens["valor_total"] = df_itens["valor_total"].apply(limpar_numero)

        # =============================
        # SALVAR PARQUET
        # =============================
        nome_parquet = arquivo.replace(".jpg", ".parquet")
        caminho_parquet = os.path.join(pasta_dados, nome_parquet)

        df_itens.to_parquet(caminho_parquet, index=False)

        # =============================
        # INSERÇÃO NO BANCO
        # =============================
        with db_engine.begin() as conn:

            df_itens.to_sql("compras_supermercado", conn, if_exists="append", index=False)
            df_nota.to_sql("notas_processadas", conn, if_exists="append", index=False)

            produtos_df = df_itens[["produto"]].drop_duplicates()

            for _, row in produtos_df.iterrows():
                conn.execute(
                    text("""
                        INSERT INTO produtos (produto)
                        VALUES (:produto)
                        ON CONFLICT (produto) DO NOTHING
                    """),
                    {"produto": row["produto"]}
                )

        # =============================
        # ATUALIZA PARQUET DE PRODUTOS
        # =============================
        with db_engine.connect() as conn:
            produtos_total = pd.read_sql("SELECT * FROM produtos", conn)

        produtos_total.to_parquet(
            os.path.join(pasta_produtos, "produtos.parquet"),
            index=False
        )

        # =============================
        # MOVE ARQUIVO
        # =============================
        shutil.move(
            caminho_arquivo,
            os.path.join(pasta_processado, arquivo)
        )

        print(f"Finalizado: {arquivo}")


# =============================
# EXECUÇÃO
# =============================
if __name__ == "__main__":
    processar_notas()