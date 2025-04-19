import pandas as pd
import os
from pathlib import Path

# Caminho base
base_path = Path(__file__).resolve().parent
input_file = base_path / 'dataset' / '2021-2022' / 'WeatherEvents_Jan2016-Dec2022.csv'
output_dir = base_path / 'dataset' / 'Wheater_cleaned'
output_file = output_dir / 'WeatherEvents_2021_2022.csv'

# Verifica se o ficheiro existe
if not input_file.exists():
    print(f" Ficheiro não encontrado: {input_file}")
    exit()

# Garante que o diretório de output existe  
os.makedirs(output_dir, exist_ok=True)

print(f" Lendo ficheiro: {input_file}")

# Lê em chunks, caso o ficheiro seja grande
chunk_size = 500_000
filtered_chunks = []

with pd.read_csv(input_file, chunksize=chunk_size) as reader:
    for i, chunk in enumerate(reader, start=1):
        print(f"🔄 Processando chunk #{i}...")

        # Verifica se a coluna existe
        if 'StartTime(UTC)' not in chunk.columns:
            print(" Coluna 'StartTime(UTC)' não encontrada!")
            break

        # Converte para datetime
        chunk['StartTime(UTC)'] = pd.to_datetime(chunk['StartTime(UTC)'], errors='coerce')

        # Filtra apenas os anos 2021 e 2022
        filtered = chunk[chunk['StartTime(UTC)'].dt.year.isin([2021, 2022])]

        if not filtered.empty:
            filtered_chunks.append(filtered)

# Junta e salva
if filtered_chunks:
    df_result = pd.concat(filtered_chunks)
    df_result.to_csv(output_file, index=False)
    print(f" Ficheiro limpo salvo em: {output_file}")
else:
    print(" Nenhum dado encontrado para os anos 2021 e 2022.")
