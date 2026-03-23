# -*- coding: utf-8 -*-
"""
Editor Spyder

Pipeline completo de processamento de notas fiscais NFC-e:
- Leitura de QR Code
- Extração de dados da nota
- Limpeza e estruturação dos dados
- Armazenamento em banco de dados (PostgreSQL)
- Salvamento em arquivos parquet
- Controle de arquivos processados
- Logging de execução
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
from datetime import datetime


# =============================
# LOG SIMPLES (TXT)
# =============================

# Caminho base do projeto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Diretório de logs
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Arquivo de log
LOG_FILE = os.path.join(LOG_DIR, "pipeline.txt")


def log(msg):
    """
    Registra mensagens em arquivo de log com timestamp.

    Parâmetros:
        msg (str): Mensagem a ser registrada

    Retorno:
        None
    """
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now()} - {msg}\n")


log("Logger inicializado corretamente")


# =============================
# FUNÇÃO: CRIAR PASTA COM GITIGNORE
# =============================

def criar_pasta_com_gitignore(caminho):
    """
    Cria uma pasta e garante que ela seja versionada corretamente no GitHub
    usando um .gitignore e um .gitkeep.

    Parâmetros:
        caminho (str): caminho da pasta a ser criada

    Retorno:
        None
    """
    os.makedirs(caminho, exist_ok=True)

    gitignore_path = os.path.join(caminho, ".gitignore")
    gitkeep_path = os.path.join(caminho, ".gitkeep")

    # Cria .gitignore se não existir
    if not os.path.exists(gitignore_path):
        with open(gitignore_path, "w", encoding="utf-8") as f:
            f.write("# Mantém a pasta no GitHub\n")
            f.write("!.gitkeep\n")

    # Cria .gitkeep se não existir
    if not os.path.exists(gitkeep_path):
        open(gitkeep_path, "a").close()


# =============================
# CONEXÃO COM POSTGRES
# =============================

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Cria conexão com o banco PostgreSQL usando credenciais do .env
db_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}?sslmode=require"
)

log("Conectado ao PostgreSQL")


# =============================
# LEITURA DE QR CODE
# =============================

def ler_qr_e_retornar_url(caminho_img):
    """
    Lê uma imagem, tenta extrair QR Code e retorna a URL encontrada.

    Estratégias usadas:
        - Redimensionamento
        - Conversão para escala de cinza
        - Threshold
        - Blur + threshold

    Parâmetros:
        caminho_img (str): caminho da imagem

    Retorno:
        str ou None: URL extraída ou None se não encontrar
    """
    img = cv2.imread(caminho_img)

    # Se a imagem não for carregada corretamente
    if img is None:
        return None

    # Aumenta resolução para melhorar leitura do QR
    img = cv2.resize(img, None, fx=3, fy=3, interpolation=cv2.INTER_CUBIC)

    # Converte para escala de cinza
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    # Lista de tentativas de processamento
    tentativas = [gray]

    # Aplica threshold (binarização)
    _, th = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    tentativas.append(th)

    # Aplica blur + threshold
    blur = cv2.GaussianBlur(gray, (5, 5), 0)
    _, th2 = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    tentativas.append(th2)

    # Tenta decodificar o QR em cada variação da imagem
    for tentativa in tentativas:
        resultados = decode(tentativa)
        if resultados:
            return resultados[0].data.decode("utf-8").strip()

    return None


# =============================
# EXTRAÇÃO NFC-e
# =============================

def extrair_itens_nfce(url):
    """
    Extrai dados de uma NFC-e a partir da URL do QR Code.

    Parâmetros:
        url (str): URL da nota fiscal

    Retorno:
        tuple:
            df_itens (DataFrame): itens da compra
            df_nota (DataFrame): metadados da nota
    """
    headers = {"User-Agent": "Mozilla/5.0"}

    # Requisição da página da nota
    response = requests.get(url, headers=headers, timeout=10)
    soup = BeautifulSoup(response.text, "html.parser")

    itens = []

    # Percorre os itens da nota na tabela HTML
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

            # Armazena item estruturado
            itens.append({
                "produto": nome,
                "quantidade": qtd,
                "unidade": unidade,
                "valor_unitario": vl_unit,
                "valor_total": vl_total
            })

        except AttributeError:
            # Ignora linhas que não são itens válidos
            continue

    # DataFrame dos itens
    df_itens = pd.DataFrame(itens)

    # Nome do estabelecimento
    try:
        fonte = soup.find("div", id="u20").get_text(strip=True)
    except:
        fonte = None

    # Data de emissão da nota
    data_emissao = pd.NaT
    try:
        tag_emissao = soup.find("strong", string=lambda x: x and "Emissão" in x)

        if tag_emissao:
            texto = tag_emissao.next_sibling

            if texto:
                data_str = str(texto).split("-")[0].strip()
                data_emissao = pd.to_datetime(data_str, dayfirst=True)
    except:
        pass

    # Chave de acesso da nota
    try:
        chave = soup.find("span", class_="chave").get_text(strip=True)
        chave = re.sub(r"\s+", "", chave)
    except:
        chave = None

    # DataFrame de metadados da nota
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
    """
    Pipeline principal que:
        - Lê imagens de notas fiscais
        - Extrai dados via QR Code
        - Processa e limpa os dados
        - Salva em parquet
        - Insere no banco de dados
        - Organiza arquivos processados
    """

    # Garante que todas as pastas existam
    criar_pasta_com_gitignore(pasta_entrada)
    criar_pasta_com_gitignore(pasta_processado)
    criar_pasta_com_gitignore(pasta_dados)
    criar_pasta_com_gitignore(pasta_produtos)

    # Extensões válidas de imagem
    extensoes_validas = (".jpg", ".jpeg", ".png", ".webp")

    # Lista apenas arquivos de imagem válidos
    arquivos = sorted([
        f for f in os.listdir(pasta_entrada)
        if f.lower().endswith(extensoes_validas)
    ])

    if not arquivos:
        log("Nenhum arquivo encontrado.")
        return

    # Loop principal
    for arquivo in arquivos:
        try:
            caminho_arquivo = os.path.join(pasta_entrada, arquivo)

            # Verifica se já foi processado no banco
            with db_engine.connect() as conn:
                result = conn.execute(
                    text("SELECT 1 FROM notas_processadas WHERE nome_arquivo = :nome"),
                    {"nome": arquivo}
                ).fetchone()

            if result:
                log(f"Já processado: {arquivo}")
                continue

            log(f"Processando: {arquivo}")

            # Leitura do QR Code
            url = ler_qr_e_retornar_url(caminho_arquivo)

            if not url:
                log(f"QR não lido: {arquivo}")
                continue

            # Extração dos dados da nota
            df_itens, df_nota = extrair_itens_nfce(url)

            if df_itens.empty:
                log(f"Sem dados: {arquivo}")
                continue

            # Adiciona informações adicionais
            data = df_nota["data"].iloc[0]

            df_itens["data"] = data
            df_itens["arquivo_origem"] = arquivo
            df_nota["nome_arquivo"] = arquivo

            # Função auxiliar para converter números
            def limpar_numero(valor):
                return float(valor.replace(".", "").replace(",", "."))

            # Conversão dos valores
            df_itens["quantidade"] = df_itens["quantidade"].apply(limpar_numero)
            df_itens["valor_unitario"] = df_itens["valor_unitario"].apply(limpar_numero)
            df_itens["valor_total"] = df_itens["valor_total"].apply(limpar_numero)

            # Salva em parquet
            nome_parquet = arquivo.replace(".jpg", ".parquet")
            caminho_parquet = os.path.join(pasta_dados, nome_parquet)

            df_itens.to_parquet(caminho_parquet, index=False)

            # Insere dados no banco
            with db_engine.begin() as conn:

                df_itens.to_sql("compras_supermercado", conn, if_exists="append", index=False)
                df_nota.to_sql("notas_processadas", conn, if_exists="append", index=False)

                produtos_df = df_itens[["produto"]].drop_duplicates()

                # Insere produtos únicos
                for _, row in produtos_df.iterrows():
                    conn.execute(
                        text("""
                            INSERT INTO produtos (produto)
                            VALUES (:produto)
                            ON CONFLICT (produto) DO NOTHING
                        """),
                        {"produto": row["produto"]}
                    )

            # Atualiza parquet de produtos
            with db_engine.connect() as conn:
                produtos_total = pd.read_sql("SELECT * FROM produtos", conn)

            produtos_total.to_parquet(
                os.path.join(pasta_produtos, "produtos.parquet"),
                index=False
            )

            # Move arquivo para pasta de processados
            shutil.move(
                caminho_arquivo,
                os.path.join(pasta_processado, arquivo)
            )

            log(f"Finalizado: {arquivo}")

        except Exception as e:
            # Log de erro sem quebrar o pipeline
            log(f"Erro no arquivo {arquivo}: {e}")


# =============================
# EXECUÇÃO
# =============================

if __name__ == "__main__":
    log("Iniciando processamento...")

    processar_notas()

    log("Processamento finalizado!")