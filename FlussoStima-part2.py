import pandas as pd
import re
import numpy as np
import os

# Define input path
#directory='C:\\Users\\gremotti\\Gmr S.r.L\\GMR-Progetti - Documenti\\2211-11 ClonazioneFarmacie\\'
directory='C:\\GMR\\Progetti\\2211-11 ClonazioneFarmacie\\'

dati_dm_file=directory+'dati\\DatiDM2'
dati_dm_file=directory+'dati\\SalesProva'

anag_file=directory+'PerPython\\anag_prod.csv'

file_name_part2 = 'sales6.pkl'

# Load the CSV file using '|' as the separator
df = pd.read_csv(anag_file, sep='|')

# Rename the columns
df.columns = ['tx_aic_code', 'prod_typ_code']

# Filter the 'prod_typ_code' column to include only 'E', 'G', and 'X'
filtered_df = df[
    df['prod_typ_code'].isin(['E', 'G', 'X'])].copy()  # Ensure to create a copy to avoid SettingWithCopyWarning

# Create a new column 'prod_typ' based on conditions using .loc
filtered_df.loc[:, 'prod_typ'] = filtered_df['prod_typ_code'].apply(lambda x: 'E' if x in ['E', 'G'] else 'X')

# Save filtered_df as a variable named Anag_prod
Anag_prod = filtered_df

# Count the occurrences of each value in the 'prod_typ' column
Update_counts = filtered_df['prod_typ'].value_counts()


# Check if the directory exists
if not os.path.isdir(dati_dm_file):
    print(f"Directory {dati_dm_file} does not exist.")
    exit()

# Function to check if the month is within the desired range
def is_valid_month(filename):
    try:
        month = int(filename[2:4])
        return 1 <= month <= 14
    except ValueError:
        return False


# Function to process all valid .gz files in the directory
def process_valid_gz_files(dati_dm_file):
    # Get the list of .gz files in the directory
    gz_files = [f for f in os.listdir(dati_dm_file) if f.endswith('.gz')]

    if not gz_files:
        print("No .gz files found in the directory.")
        return []

    valid_gz_files = [f for f in gz_files if is_valid_month(f)]

    dataframes = []
    for gz_file in valid_gz_files:
        file_path = os.path.join(dati_dm_file, gz_file)

        try:
            print(f"Start reading {file_path}")
            # Read the compressed CSV file
            df = pd.read_csv(file_path, compression='gzip', sep=';', quotechar='"', header=0)

            print(f"Successfully read {file_path}")

            # Extract year and month from filename
            year = gz_file[:2]
            month = gz_file[2:4]

            # Add year and month columns to DataFrame
            df['year'] = year
            df['month'] = month

            # Calculate the number of rows after filtering
            final_row_count = df.shape[0]
            print(f"nrow {final_row_count}")

            # Filtering the DataFrame to only keep rows where 'fonte' contains 'tx'
            df = df[df['fonte'].str.contains('tx')]
            # Calculate the number of rows after filtering
            final_row_count = df.shape[0]
            print(f"nrow filtro tx {final_row_count}")

            # Perform inner join on 'tx_aic_code' column
            df = pd.merge(df, Anag_prod, on='tx_aic_code', how='inner')
            final_row_count = df.shape[0]
            print(f"nrow dopo merge {final_row_count}")

            print(f"Merge OK read {file_path}")

            # Group by 'pdf_phy_id', 'prod_typ', 'year', and 'month' and create a new table
            df = df.groupby(['pdf_phy_id', 'prod_typ', 'year', 'month']).agg(
                count=('pdf_phy_id', 'size'),
                sum_unita=('unita', 'sum'),
                sum_euro=('euro', 'sum')
            ).reset_index()

            print(f"Group by OK {file_path}")
            dataframes.append(df)

        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return

    return dataframes

# Process all valid .gz files in the directory
gz_dataframes = process_valid_gz_files(dati_dm_file)

# Concatenate all the dataframes into one
if gz_dataframes:
    all_data_df = pd.concat(gz_dataframes, ignore_index=True)
else:
    print("No valid .gz files processed.")
    all_data_df = pd.DataFrame()

# Save all_data_df as a global variable
globals()['DatiDM2_all_gz_dataframes'] = all_data_df


# Perform inner join on 'tx_aic_code' column
#merged_df = pd.merge(DatiDM2_all_gz_dataframes, Anag_prod, on='tx_aic_code', how='inner')

# Save merged_df as a global variable
#globals()['Matched_data'] = merged_df

# Calculate the number of rows before filtering
initial_row_count = merged_df.shape[0]

# Filtering the DataFrame to only keep rows where 'fonte' contains 'tx'
filtered_df = merged_df[merged_df['fonte'].str.contains('tx')]

# Calculate the number of rows after filtering
final_row_count = filtered_df.shape[0]

# Calculate the number of rows deleted
rows_deleted = initial_row_count - final_row_count

# Save the filtered dataframe as a global variable
globals()['Filtered_data'] = filtered_df

# Group by 'pdf_phy_id', 'prod_typ', 'year', and 'month' and create a new table
grouped_df = filtered_df.groupby(['pdf_phy_id', 'prod_typ', 'year', 'month']).agg(
    count=('pdf_phy_id', 'size'),
    sum_unita=('unita', 'sum'),
    sum_euro=('euro', 'sum')
).reset_index()

# Rename columns in grouped_df
grouped_df = grouped_df.rename(columns={'sum_unita': 'unita', 'sum_euro': 'euro'})

# Display the grouped dataframe with 'nobs_vend', 'unita_index', and 'euro_index'
print(grouped_df)

# Save the grouped dataframe as a global variable
globals()['Grouped_data'] = grouped_df

# Print confirmation message
print("\nGrouped data saved as global variable: 'Grouped_data'")

all_data = pd.concat([Grouped_data])

#Group by 'pdf_phy_id', 'prd_typ', 'year', and 'month'
sales2 = all_data.groupby(['pdf_phy_id', 'prod_typ', 'month']).sum().reset_index()

#Group by 'pdf_phy_id', 'prod_typ', 'year', and 'month' to calculate aggregations
grouped_data = all_data.groupby(['pdf_phy_id', 'prod_typ', 'year', 'month']).agg({
    'unita': 'sum',  # Sum of 'unita'
    'euro': 'sum'    # Sum of 'euro'
}).reset_index()

#Calculate the count of observations ('nobs_vend') per group of 'pdf_phy_id' and 'prod_typ'
count_data = all_data.groupby(['pdf_phy_id', 'prod_typ']).size().reset_index(name='nobs_vend')

#Merge the count_data back with grouped_data
sales3 = grouped_data.merge(count_data, on=['pdf_phy_id', 'prod_typ'], how='left')


#Concatenate all DataFrames
all_data = pd.concat([Grouped_data])


#Group by 'pdf_phy_id' and 'prod_typ' to calculate 'unita_sum' and 'euro_sum'
sum_data = all_data.groupby(['pdf_phy_id', 'prod_typ'])[['unita', 'euro']].sum().reset_index()
sum_data = sum_data.rename(columns={'unita': 'unita_sum', 'euro': 'euro_sum'})

#Merge the 'unita_sum' and 'euro_sum' columns back with the original DataFrame
all_data = all_data.merge(sum_data, on=['pdf_phy_id', 'prod_typ'], how='left')

#Calculate the count of observations ('nobs_vend') per group of 'pdf_phy_id' and 'prod_typ'
count_data = all_data.groupby(['pdf_phy_id', 'prod_typ']).size().reset_index(name='nobs_vend')

#Merge the count_data back with all_data
all_data = all_data.merge(count_data, on=['pdf_phy_id', 'prod_typ'], how='left')

#Filter rows where 'nobs_vend' equals 12
filtered_data = all_data[all_data['nobs_vend'] == 12].copy()

#Calculate the 'unita_index' and 'euro_index'
filtered_data['unita_index'] = (filtered_data['unita'] * 12) / filtered_data['unita_sum']
filtered_data['euro_index'] = (filtered_data['euro'] * 12) / filtered_data['euro_sum']

#Drop the 'unita_sum' and 'euro_sum' columns
filtered_data = filtered_data.drop(columns=['unita_sum', 'euro_sum'])


#Group by 'pdf_phy_id', 'prod_typ', 'year', and 'month' if further aggregation is needed
common_sample3a = filtered_data.groupby(['pdf_phy_id', 'prod_typ', 'year', 'month']).sum().reset_index()

#Concatenate all DataFrames
all_data = pd.concat([Grouped_data])

# Ensure 'month' is of integer type
all_data['month'] = all_data['month'].astype(int)

#Group by 'pdf_phy_id' and 'prod_typ' to calculate 'unita_sum' and 'euro_sum'
sum_data = all_data.groupby(['pdf_phy_id', 'prod_typ'])[['unita', 'euro']].sum().reset_index()
sum_data = sum_data.rename(columns={'unita': 'unita_sum', 'euro': 'euro_sum'})

#Merge the 'unita_sum' and 'euro_sum' columns back with the original DataFrame
all_data = all_data.merge(sum_data, on=['pdf_phy_id', 'prod_typ'], how='left')

#Calculate the count of observations ('nobs_vend') per group of 'pdf_phy_id' and 'prod_typ'
count_data = all_data.groupby(['pdf_phy_id', 'prod_typ']).size().reset_index(name='nobs_vend')

#Merge the count_data back with all_data
all_data = all_data.merge(count_data, on=['pdf_phy_id', 'prod_typ'], how='left')

#Calculate the 'unita_index' and 'euro_index' based on the 'nobs_vend' condition
all_data.loc[all_data['nobs_vend'] == 12, 'unita_index'] = (
    all_data.loc[all_data['nobs_vend'] == 12, 'unita'] * 12) / all_data.loc[all_data['nobs_vend'] == 12, 'unita_sum']
all_data.loc[all_data['nobs_vend'] == 12, 'euro_index'] = (
    all_data.loc[all_data['nobs_vend'] == 12, 'euro'] * 12) / all_data.loc[all_data['nobs_vend'] == 12, 'euro_sum']

#Drop the 'unita_sum' and 'euro_sum' columns
all_data = all_data.drop(columns=['unita_sum', 'euro_sum'])

#Calculate the median values of 'unita_index' and 'euro_index' grouped by 'month' and 'prod_typ'

# Filter out rows where 'unita_index' or 'euro_index' are NaN before calculating the medians
filtered_data = all_data.dropna(subset=['unita_index', 'euro_index'])
median_data = filtered_data.groupby(['month', 'prod_typ']).agg({
    'unita_index': 'median',
    'euro_index': 'median'
}).reset_index()

median_data = median_data.rename(columns={'unita_index': 'median_unita_index', 'euro_index': 'median_euro_index'})


# Step 9: Merge the median values back into the all_data DataFrame
all_data = all_data.merge(median_data, on=['month', 'prod_typ'], how='left')

# Step 10: Replace 'unita_index' with 'median_unita_index' and 'euro_index' with 'median_euro_index'
all_data['unita_index'] = all_data['median_unita_index']
all_data['euro_index'] = all_data['median_euro_index']

# Step 11: Calculate 'tnobs_vend' as the count of distinct 'month' values for each 'pdf_phy_id'
all_data['tnobs_vend'] = all_data.groupby('pdf_phy_id')['month'].transform('nunique')

# Step 12: Group by 'pdf_phy_id', 'prod_typ', 'month' if further aggregation is needed
grouped_data = all_data.groupby(['pdf_phy_id', 'prod_typ', 'month']).sum().reset_index()


common_sample3b = all_data[['prod_typ', 'month', 'unita_index', 'euro_index']].copy()

# Group common_sample3b by 'prod_typ' and 'month'
common_sample3b = common_sample3b.groupby(['prod_typ', 'month']).first().reset_index()

#Calculate sum of unita_index and euro_index grouped by 'prod_typ'
sum_unita_index = common_sample3b.groupby('prod_typ')['unita_index'].sum().reset_index()
sum_euro_index = common_sample3b.groupby('prod_typ')['euro_index'].sum().reset_index()

#Merge sum_unita_index and sum_euro_index back into common_sample3b
common_sample3b = common_sample3b.merge(sum_unita_index, on='prod_typ', suffixes=('', '_sum'))
common_sample3b = common_sample3b.merge(sum_euro_index, on='prod_typ', suffixes=('', '_sum'))

#Calculate new unita_index and euro_index values
common_sample3b['unita_index'] = (common_sample3b['unita_index'] * 12) / common_sample3b['unita_index_sum']
common_sample3b['euro_index'] = (common_sample3b['euro_index'] * 12) / common_sample3b['euro_index_sum']

#Create common_sample3typ DataFrame with 'prod_typ', 'month', 'new_unita_index', 'new_euro_index'
common_sample3typ = common_sample3b[['prod_typ', 'month', 'unita_index', 'euro_index']].copy()

#Group common_sample3typ by 'prod_typ' and order by 'prod_typ' and 'month'
common_sample3typ = common_sample3typ.groupby('prod_typ').apply(lambda x: x.sort_values(by='month')).reset_index(drop=True)

sum_unita_index = common_sample3typ.groupby(['prod_typ'])['unita_index'].sum().reset_index()


# Ensure 'month' column has consistent data type across both DataFrames
common_sample3typ['month'] = common_sample3typ['month'].astype('int64')
sales3['month'] = sales3['month'].astype('int64')

# Count the number of distinct 'month' values in sales3 grouped by 'pdf_phy_id' and 'prod_typ'
tnobs_vend = sales3.groupby(['pdf_phy_id'])['month'].nunique().reset_index(name='tnobs_vend')

# Merge sales3 and common_sample3typ on 'prod_typ' and 'month'
merged_df = pd.merge(sales3, common_sample3typ, on=['prod_typ', 'month'], how='inner')

# Merge tnobs_vend with merged_df to add the 'tnobs_vend' column
merged_df = pd.merge(merged_df, tnobs_vend, on=['pdf_phy_id'], how='inner')

# Sort merged_df by 'pdf_phy_id' and 'prod_typ'
merged_df.sort_values(by=['pdf_phy_id', 'prod_typ'], inplace=True)

# Create sales4 DataFrame with required columns
sales4 = merged_df[['pdf_phy_id', 'prod_typ', 'month', 'unita_index', 'euro_index', 'tnobs_vend']]

# Now merge sales4 with sales3 to add 'unita' and 'euro' columns, including 'month' in the merge keys
sales4 = pd.merge(sales4, sales3[['pdf_phy_id', 'prod_typ', 'month', 'unita', 'euro', 'nobs_vend']], on=['pdf_phy_id', 'prod_typ', 'month'], how='left')

# Reorder columns as per the specified order
sales4 = sales4[['pdf_phy_id', 'prod_typ', 'month', 'unita', 'euro', 'nobs_vend', 'unita_index', 'euro_index', 'tnobs_vend']]


# Group by 'phy_id' and 'prdtyp' and aggregate using sum() and max() functions
sales5 = sales4.groupby(['pdf_phy_id', 'prod_typ']).agg({
    'unita': 'sum',
    'euro': 'sum',
    'unita_index': 'sum',
    'euro_index': 'sum',
    'tnobs_vend': 'max'
}).reset_index()


sales5 = sales5.sort_values(by='pdf_phy_id')

# Initialize lists to store processed data
unita_list = []
euro_list = []

# Iterate over each group defined by 'pdf_phy_id'
for phy_id, group in sales5.groupby('pdf_phy_id'):
    # Iterate over rows in the group
    for index, row in group.iterrows():
        unita = row['unita']
        euro = row['euro']
        unita_index = row['unita_index']
        euro_index = row['euro_index']
        nobs_vend = row['tnobs_vend']

        # Calculate adjusted values based on nobs_vend
        if nobs_vend < 10:
            unita = None
            euro = None
        else:
            unita = 12 * unita / unita_index
            euro = 12 * euro / euro_index

        # Append original and adjusted values to lists
        unita_list.append(unita)
        euro_list.append(euro)

# Ensure the lists are populated correctly
assert len(unita_list) == len(sales5), "unita_list length does not match sales5 length"
assert len(euro_list) == len(sales5), "euro_list length does not match sales5 length"

# Create sales6 DataFrame
sales6 = sales5.copy()  # Copy sales5 to retain original data
sales6['unita_orig'] = sales5['unita']  # Add original unita values
sales6['euro_orig'] = sales5['euro']  # Add original euro values
sales6['unita'] = unita_list  # Replace unita with adjusted values
sales6['euro'] = euro_list  # Replace euro with adjusted values


file_path = os.path.join(directory, file_name_part2)

# Ensure the directory exists
os.makedirs(directory, exist_ok=True)

# Save the DataFrame to a pickle file
sales6.to_pickle(file_path)
print(f"DataFrame saved to {file_path}")
print(sales6)
