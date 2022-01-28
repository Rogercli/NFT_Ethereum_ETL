from API_config import nft_assets_url,nft_address
import requests
from requests.exceptions import HTTPError
import json
import requests




#Opensea API get request function
def Opensea_NFT_call(nft):
    try:
        response=requests.get(nft_assets_url,params={'asset_contract_addresses':nft_address[nft],'order_by':'sale_price','limit':'50'})
        response.raise_for_status()
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}') 
    except KeyError as key_err:
        print(f'Key error occurred: {key_err}') 
    else:
        nft_response=response.json()
        return nft_response['assets']


#writing json to local disk partitioned by NFT name 
def write_json(object,nft):
    with open(f'/home/roger/SB/Capstone/NFT_ETH_pipeline/Data/Raw_data/{nft}', 'w') as json_file:
        json.dump(object,json_file)
    return

json_doodles=Opensea_NFT_call('Doodles')
write_json(json_doodles,'Doodles')
json_punks=Opensea_NFT_call('Cryptopunks')
write_json(json_punks,'Cryptopunks')
json_apes=Opensea_NFT_call('Bored_apes')
write_json(json_apes,'Bored_apes')
json_cats=Opensea_NFT_call('Cool_cats')
write_json(json_cats,'Cool_cats')
json_Meebits=Opensea_NFT_call('Meebits')
write_json(json_Meebits,'Meebits')


