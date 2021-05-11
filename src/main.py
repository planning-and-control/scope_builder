##########################################################################################################
#
# Generate scopes from Magnitude scope files, identifying platform location
#
# Variables in variables.py
#
# global_path => Source folder with year\month struture for scopes
# month => Month number as integrer
# year => Year number as integrer
#
##########################################################################################################

import pandas as pd
import os
import numpy as np
from variables import *

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

def process_scopes():

    # define file paths
    month_path = os.path.join(global_path,str(year),str(month).zfill(2))

    month_path_sp = os.path.join(month_path, 'Scopes')

    Source_EDPR = [file_name for file_name in os.listdir(month_path_sp) if "EDPR scope" in file_name][0]
    Source_EDPR_NA = [file_name for file_name in os.listdir(month_path_sp) if "EDPR-NA scope" in file_name][0]
    Source_NEO3 = [file_name for file_name in os.listdir(month_path_sp) if "NEO-3 scope" in file_name][0]
    Source_BR = [file_name for file_name in os.listdir(month_path_sp) if "BR scope" in file_name][0]
    Source_OF = [file_name for file_name in os.listdir(month_path_sp) if "OF scope" in file_name][0]

    Destination_file = os.path.join(month_path, 'Scope '+str(month)+'M'+str(year)[2:]+'.csv')

    month_path_ru = os.path.join(month_path, 'RUs')

    Source_RU = [file_name for file_name in os.listdir(month_path_ru) if file_name.startswith('RU ')][0]

    # output paths to be used
    print ('EDPR Scope: '+ Source_EDPR)
    print ('EDPR-NA Scope: '+Source_EDPR_NA)
    print ('NEO-3 Scope: '+Source_NEO3)
    print ('BR Scope: '+Source_BR)
    print ('OF Scope: '+Source_OF)
    print ('RU file: '+Source_RU)
    print ('Destination file: '+Destination_file)

    # read RU file

    df_EDPR_RU = pd.read_csv(os.path.join(month_path_ru,Source_RU), encoding = 'utf-16', sep=';', skiprows=2).drop("Unnamed: 9",axis=1)

    # read scopes

    df_EDPR = read_Scope(os.path.join(month_path_sp,Source_EDPR))

    df_EDPR_NA = read_Scope(os.path.join(month_path_sp,Source_EDPR_NA))
    df_EDPR_NA['EDPR-NA Scope'] = 'EDPR-NA'

    df_EDPR_NEO3 = read_Scope(os.path.join(month_path_sp,Source_NEO3))
    df_EDPR_NEO3['NEO-3 Scope'] = 'NEO-3'

    df_EDPR_BR = read_Scope(os.path.join(month_path_sp,Source_BR))
    df_EDPR_BR['EDPR-BR Scope'] = 'EDPR-BR'

    df_EDPR_OF = read_Scope(os.path.join(month_path_sp,Source_OF))
    df_EDPR_OF['EDPR-OF Scope'] = 'EDPR-OF'

    # merge scopes to identify EDPR SPVs presence in platform scopes

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
    #add extra scopes
    path_config = os.path.join(global_path, "Scopes_Config.xlsx")
    df_config = pd.read_excel(path_config, sheet_name="SCOPES", dtype={"SPV Reference": "str"})

    dtypes_extra_cols = {
    'Revised method (Closing)': 'str',
    'Revised Conso. (Closing)': 'str',
    'Revised Own. Int. (Closing)': 'str',
    'Revised Fin. Int. (Closing)': 'str',
    'Revised method (Opening package)': 'str',
    'Revised Conso. (Opening package)': 'str',
    'Revised Own. Int. (Opening package)': 'str',
    'Revised Fin. Int. (Opening package)': 'str',
    'Scope status (Closing)': 'str',
    'EDPR-NA Scope': 'str',
    'NEO-3 Scope': 'str',
    'EDPR-BR Scope': 'str',
    'EDPR-OF Scope': 'str',
    'Scope': 'str',
    'Scope-Check': 'str'}
    
    df_extra_cols = pd.read_excel(path_config, sheet_name="NO_SPV_REF", dtype=dtypes_extra_cols)

    for _, row in df_config.iterrows():
        
        a = row["SPV Reference"]
        b = row["File Prefix"]
        print(f"Treating {b}")
        filename = list(filter(lambda x: row["File Prefix"] in x, os.listdir(month_path_sp)))[0]
        file_path = file_path = os.path.join(month_path_sp, filename)
        df_scope_extra = read_Scope(file_path)
        
        #remove duplicates, in case D_RU are in df_merged
        df_scope_extra = df_scope_extra[~df_scope_extra["Reporting unit (code)"].isin(df_merged["Reporting unit (code)"])]
        df_scope_extra.reset_index(inplace=True, drop=True)
        
        #if companies in scope extra are null because all were included in df_merge, go to next iteration
        if df_scope_extra.empty:
            print("Passing to next iteration, dataframe empty.")
            continue
        
        #add extra columns
        if type(row["SPV Reference"]) != float:
            df_merged_filtered = df_merged[df_merged["Reporting unit (code)"]==row["SPV Reference"]][df_extra_cols.columns].reset_index(drop=True)            
            assert df_merged_filtered.shape[0] == 1, f"Please review {df_merged_filtered.shape[0]}"
            for col in df_extra_cols.columns:
                if col in df_scope_extra.columns:
                    df_scope_extra.drop(col, axis=1, inplace=True)
            
            df_scope_extra.to_csv(f"../output/scope_extra_pre_{a}.csv", index=False)
            df_merged_filtered.to_csv(f"../output/filtered_pre_{a}.csv", index=False)
            df_scope_extra = pd.concat([df_scope_extra, df_merged_filtered], axis=1).fillna(method="ffill")
            df_scope_extra.to_csv(f"../output/scope_extra_post_{a}.csv", index=False)
            print(df_scope_extra)
            # for col in df_extra_cols.columns:
            #     df_scope_extra[col].fillna(method="ffill", inplace=True)
            df_merged = pd.concat([df_merged, df_scope_extra], ignore_index=True)
        else:
            
            c = df_scope_extra["Reporting unit (code)"].unique()
            print(f"Treating {b, a}")
            for col in df_extra_cols.columns:
                if col in df_scope_extra.columns:
                    df_scope_extra.drop(col, axis=1, inplace=True)
            df_scope_extra = pd.concat([df_scope_extra, df_merged_filtered], axis=1).fillna(method="ffill")
            print(df_scope_extra)
            df_merged = pd.concat([df_merged, df_scope_extra], ignore_index=True)
        df_merged.to_csv(f"../output/df_merged_{a}.csv", index=False)
    # df_merged = pd.concat([df_merged].extend(list_df_extra_scopes))

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

if __name__ == "__main__":
    process_scopes()