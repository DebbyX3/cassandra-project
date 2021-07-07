#TODO: magari prendere i nomi per riempire le mappe con una query, tipo: select distinct poi_name from entrance_by_poi;
# il problema è che poi dopo la matrice dei tempi sarebbe da aggiornare con le nuove combinazioni (che sono state calcolate a mano)

from cassandra.cluster import Cluster
import uuid

import datetime  

cluster = Cluster(protocol_version = 3)
session = cluster.connect('vrcard')


#mappa nome-indice
nameIndexDict = {
    "Centro Fotografia": 0,
    "Santa Anastasia": 1,
    "Museo Storia": 2,
    "Torre Lamberti": 3,
    "Arena": 4,
    "Palazzo della Ragione": 5,
    "Giardino Giusti": 6,
    "Sighseeing": 7,
    "Museo Conte": 8,
    "Castelvecchio": 9,
    "San Zeno": 10,
    "Museo Lapidario": 11,
    "Tomba Giulietta": 12,
    "Museo Radio": 13,
    "Duomo": 14,
    "San Fermo": 15,
    "Casa Giulietta": 16,
    "AMO": 17,
    "Teatro Romano": 18,
    "Museo Africano": 19,
    "Verona Tour": 20,
    "Museo Miniscalchi": 21
}

#dict che tiene le somme
permanenceTimePOISumDict = {
    "Centro Fotografia": 0,
    "Santa Anastasia": 0,
    "Museo Storia": 0,
    "Torre Lamberti": 0,
    "Arena": 0,
    "Palazzo della Ragione": 0,
    "Giardino Giusti": 0,
    "Sighseeing": 0,
    "Museo Conte": 0,
    "Castelvecchio": 0,
    "San Zeno": 0,
    "Museo Lapidario": 0,
    "Tomba Giulietta": 0,
    "Museo Radio": 0,
    "Duomo": 0,
    "San Fermo": 0,
    "Casa Giulietta": 0,
    "AMO": 0,
    "Teatro Romano": 0,
    "Museo Africano": 0,
    "Verona Tour": 0,
    "Museo Miniscalchi": 0
}

#dict che tiene il count
countVisitDict = {
    "Centro Fotografia": 0,
    "Santa Anastasia": 0,
    "Museo Storia": 0,
    "Torre Lamberti": 0,
    "Arena": 0,
    "Palazzo della Ragione": 0,
    "Giardino Giusti": 0,
    "Sighseeing": 0,
    "Museo Conte": 0,
    "Castelvecchio": 0,
    "San Zeno": 0,
    "Museo Lapidario": 0,
    "Tomba Giulietta": 0,
    "Museo Radio": 0,
    "Duomo": 0,
    "San Fermo": 0,
    "Casa Giulietta": 0,
    "AMO": 0,
    "Teatro Romano": 0,
    "Museo Africano": 0,
    "Verona Tour": 0,
    "Museo Miniscalchi": 0
}

#dict che tiene le medie in secondi
avreageStayInSeconds = {
    "Centro Fotografia": 0,
    "Santa Anastasia": 0,
    "Museo Storia": 0,
    "Torre Lamberti": 0,
    "Arena": 0,
    "Palazzo della Ragione": 0,
    "Giardino Giusti": 0,
    "Sighseeing": 0,
    "Museo Conte": 0,
    "Castelvecchio": 0,
    "San Zeno": 0,
    "Museo Lapidario": 0,
    "Tomba Giulietta": 0,
    "Museo Radio": 0,
    "Duomo": 0,
    "San Fermo": 0,
    "Casa Giulietta": 0,
    "AMO": 0,
    "Teatro Romano": 0,
    "Museo Africano": 0,
    "Verona Tour": 0,
    "Museo Miniscalchi": 0
}

card_serial = ''

# computedStayes<String, long>, contiene posto e permanenza in secondi
for val in session.execute("select computeStay(entrances) from grouped_entrances_by_card"):
    for key in val[0]: #val[0] contiene la mappa
        permanenceTimePOISumDict[key] = permanenceTimePOISumDict[key] + val[0][key] 
        countVisitDict[key] = countVisitDict[key] + 1
    

for key in permanenceTimePOISumDict:

    if countVisitDict[key] != 0:
        avreageStayInSeconds[key] = permanenceTimePOISumDict[key] // countVisitDict[key]

    print(key, avreageStayInSeconds[key], countVisitDict[key])
        

print("\n")

for key in avreageStayInSeconds:
    print(key, datetime.timedelta(seconds=avreageStayInSeconds[key]))



'''
 048667C27B3F80 | 2016-10-05 11:38:52.000000+0000 |   Castelvecchio |      2016-10-05 |        35 |   vrcard2-48
 048667C27B3F80 | 2016-10-05 12:14:29.000000+0000 |           Arena |      2016-10-05 |        25 |   vrcard2-48
 048667C27B3F80 | 2016-10-05 13:07:10.000000+0000 | Santa Anastasia |      2016-10-05 |        29 |   vrcard2-48
 048667C27B3F80 | 2016-10-05 13:32:57.000000+0000 |           Duomo |      2016-10-05 |        31 |   vrcard2-48
 048667C27B3F80 | 2016-10-05 14:34:46.000000+0000 |  Torre Lamberti |      2016-10-05 |        41 |   vrcard2-48
 048667C27B3F80 | 2016-10-05 15:02:54.000000+0000 |  Casa Giulietta |      2016-10-05 |        28 |   vrcard2-48

 04D12ABA7B3F80 | 2016-11-19 17:13:06.000000+0000 |   Teatro Romano |      2016-11-19 |        24 |   vrcard2-48
 04D12ABA7B3F80 | 2016-11-20 11:55:14.000000+0000 |   Castelvecchio |      2016-11-19 |        35 |   vrcard2-48
 04D12ABA7B3F80 | 2016-11-20 13:20:53.000000+0000 |        San Zeno |      2016-11-19 |        26 |   vrcard2-48
 04D12ABA7B3F80 | 2016-11-20 15:40:18.000000+0000 |           Duomo |      2016-11-19 |        31 |   vrcard2-48
 '''