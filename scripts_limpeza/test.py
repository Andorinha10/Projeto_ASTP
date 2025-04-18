import pandas as pd
import os
from pathlib import Path

# Diretórios de origem e destino
input_dir = '/Users/pedrofs/Library/CloudStorage/OneDrive-ISCTE-IUL/Mestrado/2ªSem/ASTP/Projeto/dataset/2021-2022/Limpo'
output_file = '/Users/pedrofs/Library/CloudStorage/OneDrive-ISCTE-IUL/Mestrado/2ªSem/ASTP/Projeto/dataset/2021-2022/amostra_ordenada_2021_2022.csv'

# Amostra por ficheiro
amostra_por_ficheiro = 50000

# Lista dos ficheiros (ordenada manualmente por nome)
ficheiros = sorted([
    f for f in os.listdir(input_dir)
    if f.startswith("Flights_2021_") or f.startswith("Flights_2022_")
], key=lambda x: (int(x.split("_")[1]), int(x.split("_")[2].replace(".csv", ""))))

# Lista para guardar DataFrames temporários
amostras_ordenadas = []

# Extrair amostras por ficheiro, mantendo ordem
for ficheiro in ficheiros:
    caminho = os.path.join(input_dir, ficheiro)
    print(f" A extrair amostra de: {ficheiro}")
    
    df = pd.read_csv(caminho)
    if len(df) > amostra_por_ficheiro:
        df_amostra = df.sample(n=amostra_por_ficheiro, random_state=42)
    else:
        df_amostra = df

    amostras_ordenadas.append(df_amostra)

# Concatenar por ordem cronológica dos ficheiros
df_final = pd.concat(amostras_ordenadas, ignore_index=True)

# Garantir ordenação por FlightDate (string ou datetime, conforme necessário)
df_final['FlightDate'] = pd.to_datetime(df_final['FlightDate'])
df_final = df_final.sort_values(by='FlightDate').reset_index(drop=True)

# Guardar o CSV final ordenado
df_final.to_csv(output_file, index=False)

print(f"\n Ficheiro ordenado guardado com sucesso: {output_file}")
print(f" Total de linhas: {len(df_final)}")
