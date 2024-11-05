import os
import time
import requests
import shutil
import pandas as pd
from minio import Minio
from minio.error import S3Error

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

# Diretório de download e lista de anos a serem processados
download_dir = "planilhas_sptrans"
os.makedirs(download_dir, exist_ok=True)
anos = [2023]

# Configurações do MinIO
minio_client = Minio(
    "localhost:9050",
    access_key="datalake",
    secret_key="datalake",
    secure=False
)
bucket_name = "trusted"
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

# Função para fazer o download do arquivo
def download_file(link, nome_arquivo):
    response = requests.get(link)
    if response.status_code == 200:
        with open(os.path.join(download_dir, nome_arquivo), 'wb') as file:
            file.write(response.content)
        print(f"Arquivo {nome_arquivo} baixado com sucesso!")
    else:
        print(f"Falha ao baixar {nome_arquivo}")

# Função para processar o arquivo Excel e salvar em JSON
def process_excel_to_json(arquivo_excel, nome_arquivo_json, mes, ano):
    try:
        df = pd.read_excel(arquivo_excel, header=2, sheet_name=1)
        df_filtrado = df[['Linha', 'Tot Passageiros Transportados']]
        df_filtrado = df_filtrado.rename(columns={
            'Tot Passageiros Transportados': 'tot_passageiros_transportados',
            'Linha': 'linha'
        })
        
        df_filtrado['media_diaria'] = (df_filtrado['tot_passageiros_transportados'] / 30).astype(int).astype(str)
        df_filtrado['tot_passageiros_transportados'] = df_filtrado['tot_passageiros_transportados'].astype(str)
        df_filtrado['mes'] = mes
        df_filtrado['ano'] = ano
        separados = df_filtrado['linha'].str.split(' - ', expand=True)
        if separados.shape[1] == 2:
            df_filtrado[['letreiro', 'itinerario']] = separados
        else:
            df_filtrado[['letreiro', 'itinerario']] = separados.reindex(columns=[0, 1])
        
        # Salvando em JSON
        caminho_json = os.path.join(download_dir, nome_arquivo_json)
        df_filtrado.to_json(caminho_json, orient='records', lines=True)
        print(f"Dados processados e salvos em {nome_arquivo_json}")
        
        return caminho_json
    except Exception as e:
        print(f"Erro ao processar {arquivo_excel}: {e}")
        return None

# Função para upload de arquivos JSON para o MinIO
def upload_to_minio(file_path, object_name):
    try:
        minio_client.fput_object(bucket_name, object_name, file_path)
        print(f"{os.path.basename(file_path)} enviado com sucesso para {bucket_name}/{object_name}.")
    except S3Error as err:
        print(f"Erro ao enviar {os.path.basename(file_path)}: {err}")

# Iteração sobre anos e meses para download, processamento e upload
for ano in anos:
    for i, mes in meses.items():
        # Construindo URL e nome do arquivo
        url = f"{base_url} {i}-{mes}-{ano}.xls"
        nome_arquivo_excel = f"Consolidado_{i}_{mes}_{ano}.xls"
        
        # Download do arquivo
        print(f"Baixando: {nome_arquivo_excel}")
        download_file(url, nome_arquivo_excel)
        # Processamento do arquivo Excel para JSON
        caminho_excel = os.path.join(download_dir, nome_arquivo_excel)
        nome_arquivo_json = f"media_passageiros_{i}_{mes}_{ano}.json"
        caminho_json = process_excel_to_json(caminho_excel, nome_arquivo_json, mes, ano)
        
        # Upload do arquivo JSON para o MinIO
        if caminho_json:
            object_name = f"passageiros/{ano}/{nome_arquivo_json}"
            upload_to_minio(caminho_json, object_name)


if os.path.exists(download_dir):
    shutil.rmtree(download_dir)
    print(f"Pasta '{download_dir}' excluída após o envio dos arquivos JSON.")
else:
    print(f"Pasta '{download_dir}' não encontrada para exclusão.")