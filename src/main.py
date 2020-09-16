import pandas as pd
import os
import numpy as np

def read_Scope(file_path):
    """ Check if any line in the file contains given string """
    # Open the file in read only mode
    df = pd.read_csv(file_path, encoding = 'utf-16', sep=';', names=['Reporting unit (code)', 'Reporting unit (description)', 'Revised method (Closing)', 'Revised Conso. (Closing)', 'Revised Own. Int. (Closing)', 'Revised Fin. Int. (Closing)', 'Revised method (Opening package)', 'Revised Conso. (Opening package)', 'Revised Own. Int. (Opening package)', 'Revised Fin. Int. (Opening package)', 'Var. Method (revised)', 'Var. Conso. (revised)', 'Var. Own. Int. (revised)', 'Var. Fin. Int. (revised)', 'Initial method (Closing)', 'Initial Conso. (Closing)', 'Initial Own. Int. (Closing)', 'Initial Fin. Int. (Closing)', 'Initial method (Opening package)', 'Initial Conso. (Opening package)', 'Initial Own. Int. (Opening package)', 'Initial Fin. Int. (Opening package)', 'Var. Method (initial)', 'Var. Conso. (initial)', 'Var. Own. Int. (initial)', 'Var. Fin. Int. (initial)', 'Scope status (Closing)', 'Subscope (Closing)', 'Subscope period (Closing)', 'Subscope version (Closing)', 'Scope custom property (Closing)', 'Scope status (Opening package)', 'Subscope (Opening package)', 'Subscope period (Opening package)', 'Subscope version (Opening package)', 'Scope custom property (Opening package)', 'Acquiring R.U.', 'Interm. D.E.P.', 'Interm. Conso.', 'Interm. Own. Int.', 'Interm. Fin. Int.', 'Level', 'Linked R.U.'])
    df = df[df.Level.notnull()]
    df = df[df['Reporting unit (code)'] != 'Reporting unit (code)']
    df = df[~df['Reporting unit (code)'].astype(str).str.startswith('S')]
    df = df.reset_index(drop=True)
    df = df.drop(["Var. Method (revised)", "Var. Conso. (revised)", "Var. Own. Int. (revised)", "Var. Fin. Int. (revised)", "Initial method (Closing)", "Initial Conso. (Closing)", "Initial Own. Int. (Closing)", "Initial Fin. Int. (Closing)", "Initial method (Opening package)", "Initial Conso. (Opening package)", "Initial Own. Int. (Opening package)", "Initial Fin. Int. (Opening package)", "Var. Method (initial)", "Var. Conso. (initial)", "Var. Own. Int. (initial)", "Var. Fin. Int. (initial)", "Subscope (Closing)", "Subscope period (Closing)", "Subscope version (Closing)", "Scope custom property (Closing)", "Scope status (Opening package)", "Subscope (Opening package)", "Subscope period (Opening package)", "Subscope version (Opening package)", "Scope custom property (Opening package)", "Acquiring R.U.", "Interm. D.E.P.", "Interm. Conso.", "Interm. Own. Int.", "Interm. Fin. Int.", "Level", "Linked R.U."], axis=1)
    return df

global_path = 'C:\\OneDrive\\EDP\\O365_P&C Data Lake - General\\MetaDataSources\\Company\\Source\\'

month = 8
year = 2020

month_path = os.path.join(global_path,str(year),str(month).zfill(2))

Source_EDPR = [file_name for file_name in os.listdir(month_path) if "EDPR scope" in file_name][0]
Source_EDPR_NA = [file_name for file_name in os.listdir(month_path) if "EDPR-NA scope" in file_name][0]
Source_NEO3 = [file_name for file_name in os.listdir(month_path) if "NEO-3 scope" in file_name][0]
Source_BR = [file_name for file_name in os.listdir(month_path) if "BR scope" in file_name][0]
Source_OF = [file_name for file_name in os.listdir(month_path) if "OF scope" in file_name][0]

Destination_file = os.path.join(month_path, 'Scope '+str(month)+'M'+str(year)[2:]+'.csv')

Source_RU = [file_name for file_name in os.listdir(month_path) if file_name.startswith('RU ')][0]
print ('EDPR Scope: '+ Source_EDPR)
print ('EDPR-NA Scope: '+Source_EDPR_NA)
print ('NEO-3 Scope: '+Source_NEO3)
print ('BR Scope: '+Source_BR)
print ('OF Scope: '+Source_OF)
print ('RU file: '+Source_RU)
print ('Destination file: '+Destination_file)

df_EDPR_RU = pd.read_csv(os.path.join(month_path,Source_RU), encoding = 'utf-16', sep=';', skiprows=2).drop("Unnamed: 9",axis=1)

df_EDPR = read_Scope(os.path.join(month_path,Source_EDPR))

df_EDPR_NA = read_Scope(os.path.join(month_path,Source_EDPR_NA))
df_EDPR_NA['EDPR-NA Scope'] = 'EDPR-NA'

df_EDPR_NEO3 = read_Scope(os.path.join(month_path,Source_NEO3))
df_EDPR_NEO3['NEO-3 Scope'] = 'NEO-3'

df_EDPR_BR = read_Scope(os.path.join(month_path,Source_BR))
df_EDPR_BR['EDPR-BR Scope'] = 'EDPR-BR'

df_EDPR_OF = read_Scope(os.path.join(month_path,Source_OF))
df_EDPR_OF['EDPR-OF Scope'] = 'EDPR-OF'

df_merged = pd.merge(df_EDPR, df_EDPR_NA, how='left', on='Reporting unit (code)', suffixes=('', '_y')).drop(['Reporting unit (description)_y', 'Revised method (Closing)_y', 'Revised Conso. (Closing)_y', 'Revised Own. Int. (Closing)_y', 'Revised Fin. Int. (Closing)_y', 'Revised method (Opening package)_y', 'Revised Conso. (Opening package)_y', 'Revised Own. Int. (Opening package)_y', 'Revised Fin. Int. (Opening package)_y', 'Scope status (Closing)_y'], axis=1)

df_merged = pd.merge(df_merged, df_EDPR_NEO3, how='left', on='Reporting unit (code)', suffixes=('', '_y')).drop(['Reporting unit (description)_y', 'Revised method (Closing)_y', 'Revised Conso. (Closing)_y', 'Revised Own. Int. (Closing)_y', 'Revised Fin. Int. (Closing)_y', 'Revised method (Opening package)_y', 'Revised Conso. (Opening package)_y', 'Revised Own. Int. (Opening package)_y', 'Revised Fin. Int. (Opening package)_y', 'Scope status (Closing)_y'], axis=1)

df_merged = pd.merge(df_merged, df_EDPR_BR, how='left', on='Reporting unit (code)', suffixes=('', '_y')).drop(['Reporting unit (description)_y', 'Revised method (Closing)_y', 'Revised Conso. (Closing)_y', 'Revised Own. Int. (Closing)_y', 'Revised Fin. Int. (Closing)_y', 'Revised method (Opening package)_y', 'Revised Conso. (Opening package)_y', 'Revised Own. Int. (Opening package)_y', 'Revised Fin. Int. (Opening package)_y', 'Scope status (Closing)_y'], axis=1)

df_merged = pd.merge(df_merged, df_EDPR_OF, how='left', on='Reporting unit (code)', suffixes=('', '_y')).drop(['Reporting unit (description)_y', 'Revised method (Closing)_y', 'Revised Conso. (Closing)_y', 'Revised Own. Int. (Closing)_y', 'Revised Fin. Int. (Closing)_y', 'Revised method (Opening package)_y', 'Revised Conso. (Opening package)_y', 'Revised Own. Int. (Opening package)_y', 'Revised Fin. Int. (Opening package)_y', 'Scope status (Closing)_y'], axis=1)

# create a list of our conditions
conditions = [
    (df_merged['EDPR-NA Scope'].notnull()),
    (df_merged['NEO-3 Scope'].notnull()),
    (df_merged['EDPR-BR Scope'].notnull()),
    (df_merged['EDPR-OF Scope'].notnull()),
    True
    ]

# create a list of the values we want to assign for each condition
values = ['EDPR-NA', 'NEO-3', 'EDPR-BR', 'EDPR-OF', 'GR-EDP-RENOV']

# create a new column and use np.select to assign values to it using our lists as arguments
df_merged['Scope'] = np.select(conditions, values)

df_merged['Scope-Check'] = df_merged.isnull().sum(axis=1) >=3

df_EDPR_RU = df_EDPR_RU.rename(columns = {'Code' : 'Reporting unit (code)', 'País (Code)': 'Country', 'Estrutura de gestão (Code)': 'Management Structure', 'Moeda (Code)': 'D_CU', 'Código SAP (Code)': 'SAP Code_PreAdj', 'Código SAP (Long description)': 'SAP description'}).drop(['Long description', 'Estrutura de gestão (Long description)', 'Date created'], axis=1)

df_merged = pd.merge(df_merged, df_EDPR_RU, how='left', on='Reporting unit (code)')

# create a list of our conditions
conditions2 = [
    (df_merged['Reporting unit (code)'].astype(str).str.endswith("EM")),
    (df_merged['Reporting unit (code)'].astype(str).str.endswith("MEP")),
    True
    ]

# create a list of the values we want to assign for each condition
values2 = [df_merged['SAP Code_PreAdj'] + 'EM', df_merged['SAP Code_PreAdj'] + 'MEP', df_merged['SAP Code_PreAdj']]

# create a new column and use np.select to assign values to it using our lists as arguments
df_merged['SAP Code'] = np.select(conditions2, values2)


# df_merged['SAP Code'] = df_merged.apply(lambda row: row['SAP Code_PreAdj'] + 'EM' if row['Reporting unit (code)'].str.endswith("EM") else row['SAP Code_PreAdj'])

#or row['SAP Code_PreAdj'].astype(str).str.endswith('MEP') else row['SAP Code_PreAdj']

df_merged.to_csv(Destination_file, encoding='utf-16', index=False)
