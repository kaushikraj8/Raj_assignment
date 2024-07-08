import zipfile
import pandas as pd
import os
import datetime
import logging
import traceback

# Create a 'logs' directory if it doesn't exist
log_directory = 'logs'
log_directory_path = os.path.join(os.getcwd(), log_directory)
if not os.path.exists(log_directory_path):
    os.makedirs(log_directory_path)

# Set up logging configuration
log_file = os.path.join(log_directory_path, f"Question3_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # defining the path to the uploaded zip file
    zip_file_path = os.path.join(os.getcwd(), 'Q3.zip')
    logging.info(f'unzipping the {zip_file_path} zip file')
    # Extract the contents of the zip file
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
            extracted_files = zip_file.namelist()
            zip_file.extractall()
    except:
        logging.error('Error occurred while unzipping the file')
        traceback.print_exc()
        raise
    logging.info(f'file unzipped : {extracted_files}')
    # Load the CSV files
    try:
        columns_to_keep = pd.read_csv('Columns_to_keep.csv', header=None)
        data1 = pd.read_csv('data1.csv')
        data2 = pd.read_csv('data2.csv')
        entitylist = pd.read_csv('entitylist.csv', header=None, names=['entityID'])
        mapping_data = pd.read_csv('mapping_data.csv')
    except:
        logging.error('Error occurred while loading csv files')
        traceback.print_exc()
        raise
    logging.info(f"csv file loaded to the dataframe P{extracted_files}")

    # Merge data1.csv and data2.csv
    combined_data = pd.concat([data1, data2], ignore_index=True)
    logging.info("merging the data from the two files data1.csv and data2.csv in dataframe")
    # Convert the 'fields' column from JSON strings to dictionaries
    combined_data['fields'] = combined_data['fields'].apply(eval)

    # Create a dictionary from the mapping_data csv file for easy lookup
    field_mapping = dict(zip(mapping_data['fieldId'].astype(str), mapping_data['fieldName']))

    # Function to rename fields based on mapping
    def rename_fields(fields):
        return {field_mapping.get(k, k): v for k, v in fields.items()}

    combined_data['fields'] = combined_data['fields'].apply(rename_fields)
    logging.info('renaming the fields')
    # Convert the 'fields' dictionary to separate columns
    fields_df = combined_data['fields'].apply(pd.Series)

    columns_to_keep_list = columns_to_keep[0].tolist()

    # Identify valid columns that exist in both the DataFrame and the columns_to_keep_list
    valid_columns = [col for col in columns_to_keep_list if col in fields_df.columns]

    # Identify missing columns that are present in column_to_keep_list but not in DataFrame
    missing_columns = [col for col in columns_to_keep_list if col not in fields_df.columns]

    if missing_columns:
        logging.warning(f"\n\nThe following columns were specified but not found in the DataFrame:\n\n {missing_columns},\n\n Total Missing Column Count: {len(missing_columns)}")

    df_filtered = fields_df[valid_columns]

    result_df = pd.concat([combined_data, df_filtered], axis=1)

    result_df['entityId'] = result_df['entityId'].astype(int)

    # Choosing only those entityIDs that are present in entityid csv in our final output
    # Converting entityID values to integers where possible
    valid_entity_ids = []
    for entity_id_str in entitylist['entityID']:
        try:
            entity_id_int = int(entity_id_str)
            valid_entity_ids.append(entity_id_int)
        except ValueError:
            logging.warning(f"Could not convert '{entity_id_str}' to int. Skipping this value.")

    # Final filtered dataframe
    filtered_result_df = result_df[result_df['entityId'].isin(valid_entity_ids)]
    missign_entity= entitylist[~entitylist['entityID'].isin(filtered_result_df['entityId'].astype(str))]
    logging.info(f"\nTotal missing entityID count:\n {len(missign_entity)}\n")
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        logging.info(f"\nMissing entityIDs: \n{missign_entity}\n")
    try:
        filtered_result_df.to_csv('final_result_for_question_two.csv')
    except:
        logging.error("Failed to save the final result to a CSV file.")
        traceback.print_exc()
    logging.info("Data processing completed successfully and saved to final_result_for_question_two.csv file")

except Exception as e:
    logging.error(f"An error occurred: {str(e)}")
    traceback.print_exc()