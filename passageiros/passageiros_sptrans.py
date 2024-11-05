import requests
import pandas as pd
import time
import os
import boto3
from botocore.client import Config
from datetime import datetime
import shutil  # Importando shutil para excluir diretórios

# Adicione os anos que você deseja
anos = [2021, 2022, 2023]

# Mapeamento de números para meses
meses = {
    "01": "Janeiro",
    "02": "Fevereiro",
    "03": "Março",
    "04": "Abril",
    "05": "Maio",
    "06": "Junho",
    "07": "Julho",
    "08": "Agosto",
    "09": "Setembro",
    "10": "Outubro",
    "11": "Novembro",
    "12": "Dezembro"
}

# URL base para o download das planilhas
base_url = "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/Consolidado"

diretorio_atual = os.path.dirname(os.path.abspath(__file__))

# Diretório onde os arquivos serão salvos
download_dir = os.path.join(diretorio_atual, "planilhas_sptrans")
os.makedirs(download_dir, exist_ok=True)

# Criar uma nova pasta para os resultados
resultado_dir = os.path.join(diretorio_atual, 'media_passageiros')
os.makedirs(resultado_dir, exist_ok=True)

# Configurações do MinIO
minio_url = 'http://localhost:9050'  # ou o IP do container se estiver em outra máquina
access_key = 'datalake'
secret_key = 'datalake'
bucket_name = 'trusted'  # Crie esse bucket no MinIO antes

# Inicializa o cliente do MinIO
s3 = boto3.client('s3', 
                  endpoint_url=minio_url, 
                  aws_access_key_id=access_key, 
                  aws_secret_access_key=secret_key, 
                  config=Config(signature_version='s3v4'))

# Lista para armazenar arquivos que falharam ao baixar
arquivos_falhados = []

# Função para fazer o download do arquivo
def download_file(link, nome_arquivo, tentativas=3, timeout=8): # Valor padrão de 3 tentativas  # Timeout de 8 segundos
    caminho_completo = os.path.join(download_dir, nome_arquivo)
    
    for tentativa in range(tentativas):
        try:
            print(f"Tentativa {tentativa + 1} de baixar {nome_arquivo}...")
            response = requests.get(link, timeout=timeout)
            if response.status_code == 200:
                with open(caminho_completo, 'wb') as file:  
                    file.write(response.content)
                print(f"Arquivo {nome_arquivo} baixado com sucesso!")
                return caminho_completo  # Retorna o caminho do arquivo baixado
            else:
                print(f"Falha ao baixar {nome_arquivo}, URL: {link}")
                return None # Retorna None se não conseguiu baixar
        except requests.exceptions.RequestException as e:
            print(f"Erro ao tentar baixar o arquivo {nome_arquivo}: {e}")
            if tentativa < tentativas - 1:
                print("Tentando novamente em 5 segundos...")
                time.sleep(5)  # Espera 5 segundos antes de tentar novamente
    print(f"Todas as tentativas falharam para {nome_arquivo}. Retornando None.")
    return None  # Retorna None se falhou em todas as tentativas
    

# Função para processar o arquivo
def processar_arquivo(caminho_arquivo, mes_num, mes, ano):
    # Ler o arquivo Excel
    try:
        df = pd.read_excel(caminho_arquivo, header=2, sheet_name=1)
    except Exception as e:
        print(f"Erro ao ler o arquivo {caminho_arquivo}: {e}")
        return  # Sai da função se houver erro

    # Filtrar e renomear colunas
    df_filtrado = df[['Linha', 'Tot Passageiros Transportados']].rename(columns={
        'Tot Passageiros Transportados': 'tot_passageiros_transportados',
        'Linha': 'linha'
    })

    # Calcular média diária
    df_filtrado['media_diaria'] = (df_filtrado['tot_passageiros_transportados'].fillna(0) / 30).astype(int).astype(str)
    df_filtrado['tot_passageiros_transportados'] = df_filtrado['tot_passageiros_transportados'].astype(str)
    df_filtrado['mes'] = mes
    df_filtrado['ano'] = ano

    # Separar 'linha' em 'letreiro' e 'itinerario'
    separados = df_filtrado['linha'].str.split(' - ', expand=True)
    df_filtrado[['letreiro', 'itinerario']] = separados.reindex(columns=[0, 1], fill_value='')

    # Salvar em JSON
    caminho_json = os.path.join(resultado_dir, f'media_passageiros_{mes_num}_{mes}_{ano}.json')
    
    try:
        df_filtrado.to_json(caminho_json, orient='records', lines=True)
        print(f"Arquivo {caminho_json} salvo com sucesso!")

        # Chamar a função de upload após salvar o JSON
        fazer_upload(caminho_json, mes_num, mes, ano)
    except Exception as e:
        print(f"Erro ao salvar o arquivo JSON: {e}")


def fazer_upload(file_path, mes_num, mes_nome, ano):
    file_name = f'media_passageiros_{mes_num}_{mes_nome}_{ano}.json'

    # Criar o prefixo de ano para organizar o arquivo dentro do bucket
    prefixo_ano = f"{'passageiros'}/{ano}/{file_name}"

    if os.path.exists(file_path):
        try:
            s3.upload_file(file_path, bucket_name, prefixo_ano)
            print(f'Arquivo {prefixo_ano} enviado com sucesso para o bucket {bucket_name}!')
        except Exception as e:
            print(f'Erro ao enviar o arquivo: {e}')
    else:
        print(f'O arquivo {file_path} não existe. Upload não realizado.')


# Loop pelos anos e meses
for ano in anos:
    for mes_num, mes_nome in meses.items():
        # Nome do arquivo e URL para download
        url = f"{base_url} {mes_num}-{mes_nome}-{ano}.xls"  # Verifique a URL
        nome_arquivo = f"Consolidado_{mes_num}_{mes_nome}_{ano}.xlsx"
        
        print(f"Verificando {nome_arquivo}...")

        # Baixar o arquivo apenas se não existir
        caminho_arquivo = download_file(url, nome_arquivo)
        if caminho_arquivo is None:
            arquivos_falhados.append(nome_arquivo)  # Adiciona o arquivo à lista de falhas
            print(f"Arquivo {nome_arquivo} falhou ao ser baixado.")
            continue  # Pula para o próximo arquivo se o download falhar

        # Processar o arquivo baixado
        processar_arquivo(caminho_arquivo, mes_num, mes_nome, ano)  # Chame a função aqui

# Exibir arquivos que falharam ao baixar
if arquivos_falhados:
    print("Os seguintes arquivos falharam ao ser baixados:")
    for arquivo in arquivos_falhados:
        print(f"- {arquivo}")


# Excluir a pasta de download ao final do processamento
try:
    shutil.rmtree(download_dir)  # Remove o diretório e todo o seu conteúdo
    print(f"Pasta {download_dir} excluída com sucesso!")
except Exception as e:
    print(f"Erro ao excluir a pasta {download_dir}: {e}")

print("Processamento concluído!")