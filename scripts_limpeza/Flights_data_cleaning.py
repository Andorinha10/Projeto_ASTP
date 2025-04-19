import pandas as pd
import os
from pathlib import Path

# Caminhos base relativos ao script
base_path = Path(__file__).resolve().parent.parent
input_dir = base_path / 'dataset' / '2021-2022'
output_dir = base_path / 'dataset' / 'Voos_cleaned'

# Garante que o diretório de saída existe
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

# Processar os ficheiros combinados
for year in [2021, 2022]:
    input_file = input_dir / f"Combined_Flights_{year}.csv"
    output_file = output_dir / f"Cleaned_Flights_{year}.csv"

    if not input_file.exists():
        print(f" Ficheiro não encontrado: {input_file}")
        continue

    print(f"\n Iniciando limpeza de: {input_file.name}")
    chunk_counter = 0

    with pd.read_csv(input_file, chunksize=500_000) as reader:
        for chunk in reader:
            chunk_counter += 1
            print(f" Processando chunk #{chunk_counter}...")

            # Filtrar colunas
            chunk = chunk[[col for col in colunas_a_manter if col in chunk.columns]]

            # Guardar no output
            mode = 'w' if chunk_counter == 1 else 'a'
            chunk.to_csv(output_file, index=False, mode=mode, header=(chunk_counter == 1))

    print(f" Limpeza completa → {output_file}")
