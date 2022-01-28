'''Environment configurations and variables for Azure Cloud Environment'''


# Azure credentials
storageAccountName = 'your-acct-name'
storageAccountAccessKey = 'your-acct-key'
ContainerName = 'capstoneblob'

# Azure filepaths
base_path='/mnt/capstoneblob/Data'
raw_data_path=f'{base_path}/Raw/'
raw_data_file_path=[raw_data_path+'Cryptopunks',
                    raw_data_path+'Bored_apes',
                    raw_data_path+'Cool_cats',
                    raw_data_path+'Meebits',
                    raw_data_path+'Doodles']

preprocessed_data_path=f'{base_path}/Preprocessed/'
processed_data_path=f'{base_path}/Processed/'
structured_data_path=f'{base_path}/Structured/'
published_data_path=f'{base_path}/Published/'
