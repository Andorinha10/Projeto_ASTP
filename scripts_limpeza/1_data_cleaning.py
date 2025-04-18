import pandas as pd
import os
from pathlib import Path

# Caminhos base
input_dir = '/Users/pedrofs/Library/CloudStorage/OneDrive-ISCTE-IUL/Mestrado/2ªSem/ASTP/Projeto/dataset/2021-2022/Original'
output_dir = '/Users/pedrofs/Library/CloudStorage/OneDrive-ISCTE-IUL/Mestrado/2ªSem/ASTP/Projeto/dataset/2021-2022/Limpo'

# Garante que o output_dir existe
os.makedirs(output_dir, exist_ok=True)

# Colunas a manter
colunas_a_manter = [
    'FlightDate', 'Airline', 'Origin', 'Dest', 'Cancelled', 'Diverted', 
    'CRSDepTime', 'DepTime', 'DepDelayMinutes', 'DepDelay', 'ArrTime', 
    'CRSElapsedTime', 'ActualElapsedTime', 'Distance', 
    'Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 
    'Marketing_Airline_Network', 'IATA_Code_Marketing_Airline', 
    'Flight_Number_Marketing_Airline', 'Operating_Airline', 
    'IATA_Code_Operating_Airline', 'OriginAirportID', 'OriginCityMarketID', 
    'OriginCityName', 'OriginState', 'OriginStateName', 'DestAirportID', 
    'DestCityMarketID', 'DestCityName', 'DestState', 'DestStateName', 'DepDel15', 
    'DepTimeBlk', 'TaxiOut', 'TaxiIn', 'CRSArrTime', 'ArrDelay', 'ArrDel15', 'ArrTimeBlk'
]

# Processar apenas os ficheiros de 2021 e 2022
for year in [2021, 2022]:
    for month in range(1, 13):
        file_name = f"Flights_{year}_{month}.csv"
        input_file = os.path.join(input_dir, file_name)
        output_file = os.path.join(output_dir, file_name)

        # Verifica se o ficheiro existe antes de tentar processar
        if not Path(input_file).exists():
            print(f" Ficheiro não encontrado: {input_file}")
            continue

        print(f"\n Iniciando limpeza de: {file_name}")
        chunk_counter = 0

        with pd.read_csv(input_file, chunksize=500_000) as reader:
            for chunk in reader:
                chunk_counter += 1
                print(f" Processando chunk #{chunk_counter} de {file_name}...")

                # Filtrar colunas
                chunk = chunk[[col for col in colunas_a_manter if col in chunk.columns]]

                # Guardar no output
                mode = 'w' if chunk_counter == 1 else 'a'
                chunk.to_csv(output_file, index=False, mode=mode, header=(chunk_counter == 1))

        print(f" Limpeza completa → {output_file}")
