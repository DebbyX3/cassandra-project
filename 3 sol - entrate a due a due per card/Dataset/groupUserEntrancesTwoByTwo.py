import csv

with open('D:\\Universita\\basi di dati avanzate - Dataset veronacard\\2 sol - tutto raggruppato in card\\dati_2014 by user.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')

    card = ''
    activation = ''
    profile = ''
    final = []
    precPoi = ''
    precEntrance = ''

    final.append("card_serial,first_poi,first_entrance,second_poi,second_entrance")

    #entrance_timestamp,poi name,device_id,card_serial,activation_date,profile_type
    for data in csv_reader:        
        if data[3] != card: #card serial - sono alla prima str nuova
            card = data[3]        
            precPoi = data[1]
            precEntrance = data[0]    
        else:
            final.append(card + "," + precPoi + "," + precEntrance + "," + data[1] + "," + data[0])
            precPoi = data[1]
            precEntrance = data[0]

    print("\n".join(final))

    #f=open('D:\\Universita\\basi di dati avanzate - Dataset veronacard\\3 sol - entrate a due a due per card\\dati_2014 by user grouped two by two.csv','w')
    #for ele in final:
     #   f.write(ele+'\n')

    #f.close()
'''
2020-07-08 11:04:35,Castelvecchio,35,040017121B6785,2020-07-07,vrcard-48-2019
2020-07-07 15:50:50,Torre Lamberti,41,040017121B6785,2020-07-07,vrcard-48-2019
2020-07-07 15:16:38,Palazzo della Ragione,46,040017121B6785,2020-07-07,vrcard-48-2019
2020-07-07 11:44:35,Arena,25,040017121B6785,2020-07-07,vrcard-48-2019

2020-01-26 08:58:07,Castelvecchio,35,040018121B6785,2020-01-24,vrcard-48-2019
2020-01-25 16:54:08,San Zeno,26,040018121B6785,2020-01-24,vrcard-48-2019
2020-01-25 15:11:32,Teatro Romano,24,040018121B6785,2020-01-24,vrcard-48-2019
2020-01-24 16:20:04,Santa Anastasia,45,040018121B6785,2020-01-24,vrcard-48-2019
2020-01-24 12:21:46,Palazzo della Ragione,46,040018121B6785,2020-01-24,vrcard-48-2019
2020-01-24 11:56:13,Torre Lamberti,41,040018121B6785,2020-01-24,vrcard-48-2019
2020-01-24 11:22:55,Casa Giulietta,28,040018121B6785,2020-01-24,vrcard-48-2019
'''
