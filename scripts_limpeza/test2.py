import pandas as pd

# Caminho para o ficheiro original
input_file = '/Users/pedrofs/Library/CloudStorage/OneDrive-ISCTE-IUL/Mestrado/2ªSem/ASTP/Projeto/dataset/WeatherEvents_Jan2016-Dec2022.csv'

# Caminho para o ficheiro limpo
output_file = '/Users/pedrofs/Library/CloudStorage/OneDrive-ISCTE-IUL/Mestrado/2ªSem/ASTP/Projeto/dataset/FINAL/WeatherEvents_2021_2022.csv'

# Colunas a manter
colunas_a_manter = [
    'EventId', 'Type', 'Severity', 'StartTime(UTC)', 
    'EndTime(UTC)', 'AirportCode', 'City', 'State'
]

# Ler o dataset
df = pd.read_csv(input_file)

# Converter StartTime(UTC) para datetime
df['StartTime(UTC)'] = pd.to_datetime(df['StartTime(UTC)'])

# Filtrar para os anos de 2021 e 2022
df_filtrado = df[(df['StartTime(UTC)'].dt.year >= 2021) & (df['StartTime(UTC)'].dt.year <= 2022)]

# Manter apenas as colunas selecionadas
df_limpo = df_filtrado[colunas_a_manter]

# Guardar o dataset limpo
df_limpo.to_csv(output_file, index=False)

print(f'Dataset limpo guardado em: {output_file}')
