import logging
logging.basicConfig(level=logging.DEBUG)

from spyne import Application, rpc, srpc, ServiceBase, \
    Integer, Unicode

from spyne import Iterable
import requests, collections
import json
from spyne.protocol.http import HttpRpc
from spyne.protocol.json import JsonDocument
import re
from collections import OrderedDict 
from spyne.server.wsgi import WsgiApplication

class CrimeSpottingService(ServiceBase):
    dataList = []
    #crimeList = []
    @srpc(float, float, float,_returns=Iterable(Unicode))
    def crimeReport(lon, lat, radius):
        param ={'lon':lon, 'lat':lat, 'radius':radius, 'key':'.'}
        data= requests.get("https://api.spotcrime.com/crimes.json", params=param)
        dataList = data.json()
        #yield dataList
        print len(dataList["crimes"])

        crimeList = []
        AND = "&"
        OF = "OF"
        addressList = []


        for i in dataList["crimes"]:
            if i["type"] in crimeList:
                pass
            else:
                crimeList.append(i["type"])
        
            completeAddress = i["address"]

        

            if(OF in completeAddress):
                firstStreet = completeAddress[completeAddress.index(OF) + 3 :]

                if firstStreet in addressList:
                    pass
                else:
                    addressList.append(firstStreet)
            elif(AND in completeAddress):
                secondStreet = completeAddress[:completeAddress.index(AND)]

                thirdStreet = completeAddress[:completeAddress.index(AND)+ 2 :]

                if secondStreet in addressList:
                    pass
                else:
                    addressList.append(thirdStreet)

            

        #print completeAddress      
        #crime period slots are fixed and there are 8 different time periods defined  
        crime_time_period = 8 * [0] 

        particularCrime = [0] * len(crimeList)
        streetcount = [0] * len(addressList)


        for i in dataList["crimes"]:
            if i["type"] in crimeList:
                particularCrime[crimeList.index(i["type"])] +=1

            event = 0
            hour = int(i["date"][9:11])*100
            minute = int(i["date"][12:14])  
            
            if(str(i["date"][15:17]=='PM') and (int(hour)!= 1200)):
                event +=1200
            if(str(i["date"][15:17]=='AM') and (int(hour)!= 1200)):
                event +=2400

            event += hour + minute


            val = 300
            for t in range(8):
                if(int(event) <= val):
                    crime_time_period[t] += 1
                    break
                val += 300

            for street in addressList:
                if street in i["address"]:
                    streetcount[addressList.index(street)] += 1

        total_crimes = len(dataList)

        print addressList

        print streetcount

        crimeDict = {}


        for j in range(len(crimeList)):
            crimeDict[str(crimeList[j])] = particularCrime[j]

        hazardousList = {}

        top = 0
        second = 0
        third= 0
        indexOne = 0
        indexTwo = 0
        indexThree = 0

        for r in range(len(streetcount)):
            if(streetcount[r] >= top):
                third = second
                second = top
                top = streetcount[r]
                indexThree = indexTwo
                indexTwo = indexOne
                indexOne = r
            elif(streetcount[r] >= second):
                third = second
                second = streetcount[r]
                indexThree = indexTwo
                indexTwo = r
            elif(streetcount >= third):
                third = streetcount[r]
                indexThree = r

        hazardousList[str(addressList[indexOne])] = streetcount[indexOne]
        hazardousList[str(addressList[indexTwo])] = streetcount[indexTwo]
        hazardousList[str(addressList[indexThree])] = streetcount[indexThree]


        print hazardousList

        report = OrderedDict()



        report = {"total+crimes" : len(dataList["crimes"]),
        "Most_Dangerous_Streets":hazardousList.keys(),
        "crime_count_type":crimeDict,
        "event_time_count":{
            "12:01am-3am" : crime_time_period[0],
            "3:01am-6am" : crime_time_period[1],
            "6:01am-9am" : crime_time_period[2],
            "9:01am-12noon" : crime_time_period[3],
            "12:01pm-3pm" : crime_time_period[4],
            "3:01pm-6pm" : crime_time_period[5],
            "6:01pm-9pm" : crime_time_period[6],
            "9:01pm-12midnight" : crime_time_period[7]
        }

        }

        report = collections.OrderedDict(report)

        yield report
        #yield dataList

        #yield crimeList    
        
        #yield particularCrime




application = Application([CrimeSpottingService],
    tns='spyne.example.com',
    in_protocol=HttpRpc(validator='soft'),
    out_protocol=JsonDocument()
)

if __name__ == '__main__':
    # You can use any Wsgi server. Here, we chose
    # Python's built-in wsgi server but you're not
    # supposed to use it in production.
    from wsgiref.simple_server import make_server

    wsgi_app = WsgiApplication(application)
    server = make_server('0.0.0.0', 8000, wsgi_app)
    server.serve_forever()