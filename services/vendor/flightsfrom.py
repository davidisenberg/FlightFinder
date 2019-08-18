import requests
import js2py
from bs4 import BeautifulSoup
from model.direct import Direct
import pandas as pd

class FlightFromApi:

    def get_directs(self,destination):
        url = self.get_request(destination)
        return self.call_api(url, destination)


    def get_request(self,destination):
        return "https://www.flightsfrom.com/" + destination + "/destinations"


    def call_api(self,url, destination):
        data = requests.get(url).text
        #data = open("FlightFromDirectFLR.txt", "r")
        #data = data.read()
        soup = BeautifulSoup(data, "html.parser")
        scripts = soup.find_all("script")
        directs = []
        for script in scripts:
            if "window.airport" in script.text:
                getRoutesJs = js2py.eval_js('function() {  var window = {airport:"", filter:"", routes:"",selected:"",activetab:""};' + script.text + ' return window.routes; }')  # js to esprima syntax tree
                for route in getRoutesJs().to_list():
                    directs.append(Direct(destination, route["iata_to"], route["airport"]["country"],route["city_name_en"],
                                          route["airport"]["display_name"],route["airport"]["latitude"], route["airport"]["longitude"],
                                          route["airport"]["no_routes"] ))
        directs = pd.DataFrame.from_records([direct.to_dict() for direct in directs])
        return directs







