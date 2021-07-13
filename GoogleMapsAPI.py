import requests
from urllib.parse import urlencode,urlparse, parse_qsl
import pandas as pd
import time
import argparse



def addOptions():
    parser = argparse.ArgumentParser(description='Prepare input files to obtain calirbated values.')
    parser.add_argument('--key','-k', dest='key',type=str,\
                            help='Insert the google API key.',required = True)
    parser.add_argument('--csv-path','-c', dest='csv_path',type=str,\
			    help='Insert the path of the csv file.')
    parser.add_argument('--index-feature-name','-i', dest='index_feature',type=str,\
                            help='Insert the name of the index attribute in your csv')
    parser.add_argument('--address-feature-name','-a', dest='address_feature',type=str,\
                            help='Insert the name of the address attribute in your csv')
    parser.add_argument('--output-csv-path','-o', dest='output_path',type=str,\
			    help='Insert the path of the output csv file.')
    return parser

	

class GoogleMapsClient(object):
    lat= None
    lng = None
    data_type = 'json'
    location_query = None
    api_key= None
    
    def __init__(self,api_key=None,address_or_postalcode=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.location_query = address_or_postalcode
        if api_key == None:
            raise Exception("API key is required")
        self.api_key = api_key    
        if self.location_query != None :
            self.extract_lat_lng()
    
    def extract_lat_lng(self, location=None):
        loc_query = self.location_query
        if location!= None:
            loc_query = location
        endpoint = f"https://maps.googleapis.com/maps/api/geocode/{self.data_type}"
        params= {"address": loc_query, "key":self.api_key}
        url_params= urlencode(params)
        url = f"{endpoint}?{url_params}"
        r = requests.get(url)
        if r.status_code not in range(200, 299):
            return{}
        latlng={}
        try:
            latlng= r.json()['results'][0]['geometry']['location']
        except:
            pass
        #stampare messaggio errore
        lat, lng = latlng.get("lat"), latlng.get("lng")
        self.lat = lat
        self.lng = lng
        return lat, lng
    
    
    def search(self, keyword="Mexican food", radius=5000, location=None):
        lat, lng = self.lat, self.lng
        if location != None:
            lat, lng =  self.extract_lat_lng(location=location)
        endpoint = f"https://maps.googleapis.com/maps/api/place/nearbysearch/{self.data_type}"
        params ={
            "key" : self.api_key,
            "location" : f"{self.lat},{self.lng}",
            "radius" : radius,
            "keyword" : keyword
             }
        params_encoded = urlencode(params)
        places_url = f"{endpoint}?{params_encoded}"                     
        r = requests.get(places_url)
        if r.status_code not in range(200,299):
            return{}
        return r.json()
       
        
    def detail(self, place_id= "ChIJN1t_tDeuEmsRUsoyG83frY4", fields =["name","rating","rating","formatted_phone_number","formatted_address"]):
        detail_base_endpoint = f"https://maps.googleapis.com/maps/api/place/details/{self.data_type}"
        detail_params ={
         "place_id": f"{place_id}",
         "fields": ",".join(fields),
         "key": self.api_key
         }
        detail_params_encoded = urlencode(detail_params)
        detail_url = f"{detail_base_endpoint}?{detail_params_encoded}"
        r = requests.get(detail_url)
        if r.status_code not in range(200,299):
            return{}
        return r.json()
    
def main(args=None):
    argParser = addOptions()
    options = argParser.parse_args()
    #print(options)
       
                

    df = pd.read_csv (options.csv_path,sep=";")
    dt = pd.DataFrame(df, columns=[options.address_feature,options.index_feature])

    df['IND']=df[options.address_feature] +(',')+df[options.index_feature]
    lista_indirizzi = df['IND'].tolist()

    dt['lat'] = 0
    dt['long']=  0
    dt['lat'] = dt['lat'].astype(float)
    dt['long'] = dt['long'].astype(float)
       

    for index, row in df.iterrows(): 
        #print (lista_indirizzi[index])
        location_1 = GoogleMapsClient(api_key = options.key ,address_or_postalcode ='lista_indirizzi[index]')
    if location_1 is None:
        dt.loc[index, 'lat'] = 0
        dt.loc[index, 'long'] = 0
        #print(dt.iloc[index,:])
    else:
        location_1.extract_lat_lng(location=lista_indirizzi[index])  
        dt.loc[index, 'lat'] =location_1.lat
        dt.loc[index, 'long'] =location_1.lng
        time.sleep(1)

    dt.to_csv(options.output_path, encoding='utf-8')
    return
main()
        
    

       
