#TODO: magari prendere i nomi per riempire le mappe con una query, tipo: select distinct poi_name from entrance_by_poi;
# il problema è che poi dopo la matrice dei tempi sarebbe da aggiornare con le nuove combinazioni (che sono state calcolate a mano)

from cassandra.cluster import Cluster

import uuid
import csv
import time
import datetime  

startTime = time.time()

cluster = Cluster(protocol_version = 3)
session = cluster.connect('vrcard')

result = []

'''
f=open('D:\\Universita\\Cassandra project - basi di dati avanzate\\3 sol - entrate a due a due per card\\stayTimeForEachPOI.csv','w')

f.write("row_id,stay_time,poi" + "\n")
'''
#PROBLEMA: ESEGUIRE UNA UDF SU UNA TABELLA è MOLTO DISPENIOSO!
#uuid, stay_time udf, first_poi
for data in session.execute("""
    SELECT uuid(), computeStayTwoByTwo(first_entrance, second_entrance, first_poi, second_poi), first_poi
        FROM grouped_entrances_two_by_two_by_card;
    """):

    result.append(str(data[0]) + str(data[1]) + data[2])

'''
    #scrivo su file i dati letti in caso mi servissero, tolgo anche le righe con media = 0
    if data[1] != 0:
        f.write(str(data[0]) + "," + str(data[1])+ "," + data[2]+ "\n")

        #scrivo direttamente sulla tabella stay_time i nuovi dati

        #il piano era metterli direttamente in tabella con insert
        session.execute("""
        INSERT INTO stay_times(row_id, stay_time, poi)
        VALUES (%s, %s, %s)
        """,
        (data[0], data[1], data[2]))
        
f.close()
'''

#troppo lento
'''
with open('D:\\Universita\\Cassandra project - basi di dati avanzate\\3 sol - entrate a due a due per card\stayTimeForEachPOI.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')

    next(csv_reader, None) #skip header

    for data in csv_reader:
        session.execute("""
                INSERT INTO stay_times(row_id, stay_time, poi)
                VALUES (%s, %s, %s)
                """,
                (uuid.UUID(data[0]), int(data[1]), data[2]))
'''

#dopo aver fatto il copy del file generato, fare le medie
#fare un group by è impossibile, bisogna fare una query per ogni poi (sigh)
#prendo il nome dei poi e faccio una query per ognuno

pois = []

'''for data in session.execute("""
SELECT AVG(stay_time), poi 
FROM stay_times
GROUP BY poi;
"""):
    print(data.poi, datetime.timedelta(seconds=data[0]))
'''

for poiData in session.execute("""
    SELECT DISTINCT poi
        FROM stay_times;
    """):
    
        for data in session.execute("""
            SELECT AVG(stay_time), poi
                FROM stay_times
                WHERE poi = %s;
            """,
            (poiData[0], )
        ):
            print(data.poi, datetime.timedelta(seconds=data[0]))


print("--- %s seconds ---" % (time.time() - startTime))

    
'''
for key in permanenceTimePOISumDict:

    if countVisitDict[key] != 0:
        avreageStayInSeconds[key] = permanenceTimePOISumDict[key] // countVisitDict[key]

    print(key, avreageStayInSeconds[key], countVisitDict[key])
        

print("\n")

for key in avreageStayInSeconds:
    print(key, datetime.timedelta(seconds=avreageStayInSeconds[key]))
'''
