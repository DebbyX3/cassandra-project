# magari prendere i nomi per riempire le mappe con una query, tipo: select distinct poi_name from entrance_by_poi;
# il problema è che poi dopo la matrice dei tempi sarebbe da aggiornare con le nuove combinazioni (che sono state calcolate a mano)

from cassandra.cluster import Cluster

import uuid
import datetime  
import time

startTime = time.time()

cluster = Cluster(protocol_version = 3)
session = cluster.connect('vrcard')

#matrice che contiene i tempi in secondi
seconds_distance_matrix = [
    [	0,	    180,	480,	60,	    540,	60,	    600,	60,	    60,	    600,	840,	600,	780,	780,	360,	360,	180,	180,	600,	660,	0,	240	],
    [	180,	0,	    480,	240,	540,	240,	600,	300,	120,	660,	900,	660,	780,	900,	180,	360,	360,	60,	    480,	660,	0,	300	],
    [	480,	480,	0,	    480,	360,	480,	480,	480,	600,	780,	1320,	480,	540,	480,	540,	180,	360,	420,	600,	480,	0,	720	],
    [	60,	    240,	480,	0,	    600,	60,	    660,	60,	    60,	    540,	780,	540,	840,	720,	360,	420,	120,	240,	660,	720,	0,	240	],
    [	540,	540,	360,	600,	0,	    540,	660,	540,	540,	360,	600,	60,	    420,	660,	600,	360,	480,	480,	720,	660,	0,	780	],
    [	60,	    240,	480,	60,	    540,	0,	    660,	60,	    120,	540,	780,	540,	840,	720,	360,	360,	60,	    240,	660,	720,	0,	240	],
    [	600,	600,	480,	660,	660,	660,	0,	    660,	540,	960,	1440,	720,	840,	300,	720,	360,	540,	600,	360,	240,	0,	840	],
    [	60,	    300,	480,	60,	    540,	60,	    660,	0,	    120,	540,	780,	540,	900,	720,	360,	420,	120,	240,	660,	720,	0,	180	],
    [	60,	    120,	600,	60,	    540,	120,	540,	120,	0,	    660,	900,	600,	720,	840,	240,	300,	180,	120,	540,	660,	0,	300	],
    [	600,	660,	780,	540,	360,	540,	960,	540,	660,	0,	    420,	300,	780,	840,	780,	540,	660,	780,	1020,	1020,	0,	600	],
    [	840,	900,	1320,	780,	600,	780,	1440,	780,	900,	420,	0,	    600,	1200,	1560,	1080,	1200,	900,	960,	1200,	1320,	0,	780	],
    [	600,	660,	480,	540,	60,	    540,	720,	540,	600,	300,	600,	0,	    540,	600,	660,	300,	420,	540,	840,	720,	0,	720	],
    [	780,	780,	540,	840,	420,	840,	840,	900,	720,	780,	1200,	540,	0,	    840,	720,	360,	480,	600,	960,	840,	0,	900	],
    [	780,	900,	480,	720,	660,	720,	300,	720,	840,	840,	1560,	600,	840,	0,	    960,	420,	600,	840,	780,	600,	0,	900	],
    [	360,	180,	540,	360,	600,	360,	720,	360,	240,	780,	1080,	660,	720,	960,	0,	    480,	540,	180,	420,	600,	0,	180	],
    [	360,	360,	180,	420,	360,	360,	360,	420,	300,	540,	1200,	300,	360,	420,	480,	0,	    240,	300,	600,	480,	0,	600	],
    [	180,	360,	360,	120,	480,	60,	    540,	120,	180,	660,	900,	420,	480,	600,	540,	240,	0,	    180,	600,	600,	0,	300	],
    [	180,	60,	    420,	240,	480,	240,	600,	240,	120,	780,	960,	540,	600,	840,	180,	300,	180,	0,	    360,	600,	0,	180	],
    [	600,	480,	600,	660,	720,	660,	360,	660,	540,	1020,	1200,	840,	960,	780,	420,	600,	600,	360,	0,	    240,	0,	480	],
    [	660,	660,	480,	720,	660,	720,	240,	720,	660,	1020,	1320,	720,	840,	600,	600,	480,	600,	600,	240,	0,	    0,	660	],
    [	0,	    0,	    0,	    0,	    0,	    0,	    0,	    0,	    0,	    0,	    0,	    0,	    0,	    0,	    0,	    0,	    0,	    0,	    0,	    0,	    0,	0	],
    [	240,	300,	720,	240,	780,	240,	840,	180,	300,	600,	780,	720,	900,	900,	180,	600,	300,	180,	480,	660,	0,	0	]
]

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

# card_serial    | entrance_timestamp              | poi_name              | activation_date | device_id | profile_type
for val in session.execute("select * from entrance_by_card;"):
    if(card_serial != val[0]): #primo record, tengo le info e skippo al prossimo
        card_serial = val[0]
        
        poi1 = val[2]
        entrance1 = val[1]
    else:
        poi2 = val[2]
        entrance2 = val[1]

        if(entrance2.date() == entrance1.date()): #non facciamo il calcolo se la data è diversa
            #prendi il valore attuale nella mappa, somma la differenza tra i due orari e togli il tempo di percorrenza da un poi all'altro
            permanenceTimePOISumDict[poi1] = permanenceTimePOISumDict[poi1]   \
                                                        + (entrance2 - entrance1).seconds   \
                                                        - seconds_distance_matrix[nameIndexDict[poi1]][nameIndexDict[poi2]]
            
            countVisitDict[poi1] = countVisitDict[poi1] + 1
            
        #aggiorna i valori
        poi1 = poi2
        entrance1 = entrance2

for key in permanenceTimePOISumDict:
    
    if countVisitDict[key] != 0:
        avreageStayInSeconds[key] = permanenceTimePOISumDict[key] // countVisitDict[key]

    print(key, avreageStayInSeconds[key], countVisitDict[key])
        

print("\n")

for key in avreageStayInSeconds:
    print(key, datetime.timedelta(seconds=avreageStayInSeconds[key]))

print("--- %s seconds ---" % (time.time() - startTime))

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