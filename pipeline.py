import os
import pandas as pd
from dagster import pipeline, solid, execute_pipeline


@solid
def solid1():
    data_path = './Input'
    metadata_path ='./metadata.csv'
    output_path = './Output/{0}'

    metadata_df = pd.read_csv(metadata_path)

    for file_name  in os.listdir(data_path):
        if '.csv' in file_name:
            file_path = os.path.join(data_path, file_name)
        else:
            continue
        data_df = pd.read_csv(file_path, names=['Name', 'Reading Date', 'Value'])

        new_cols = ['Site_name', 'PLC name', 'Sub System Name', 'Reading type', 'Reading Attribute']
        name_col_splitted_to_new_cols_df = data_df['Name'].str.split('.', expand=True)
        name_col_splitted_to_new_cols_df.drop(5, inplace=True, axis=1)
        data_df[new_cols] = name_col_splitted_to_new_cols_df
        data_df.drop('Name', axis=1, inplace=True)
        data_df = data_df.loc[:, ['Site_name', 'PLC name', 'Sub System Name', 'Reading type', 'Reading Attribute', 'Reading Date', 'Value']]

        data_df.to_csv(output_path.format(f'data_df_after_splitting_name_col_{file_name}'), index=False)

        

        # Part2 - merging the metadata
        plc_to_machine_type = {'PLC303': 'excavator', 
                        'PLC0120': 'wheel trencher',
                        'PLC0240': 'generator',
                        'PLC970': 'front loader', 
                            'PLC030': 'scraper'}
        plc_to_machine_type_df = pd.DataFrame(list(plc_to_machine_type.items()), columns=['plc_name', 'machine_type'])
        metadata_with_plc_name_df = pd.merge(metadata_df, plc_to_machine_type_df, on='machine_type')
        data_df.rename(columns={'PLC name': 'plc_name'}, inplace=True)
        main_df = pd.merge(data_df, metadata_with_plc_name_df, on='plc_name')
        main_df.to_csv(output_path.format(f'main_df_{file_name}'), index=False)




@pipeline
def pipeline_run():
    solid1()

if __name__ == "__main__":
    result = execute_pipeline(pipeline_run)
