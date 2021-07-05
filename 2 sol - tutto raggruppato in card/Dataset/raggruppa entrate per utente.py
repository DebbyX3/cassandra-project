'''
#versione con anche device id e il resto

import csv

with open('D:\\Universita\\basi di dati avanzate - Dataset veronacard\\dati_2020 by user.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')

    line_count = 0

    card = ''
    activation = ''
    profile = ''
    final = []
    entrancePoiDevice = []

    #entrance_timestamp,poi name,device_id,card_serial,activation_date,profile_type
    for data in csv_reader:        
        if data[3] != card: #card serial
            final.append(card + "|{" + ','.join(entrancePoiDevice) + "}|" + activation + "|" + profile)

            entrancePoiDevice.clear()

            card = data[3]
            activation = data[4]
            profile = data[5]
        
        entrancePoiDevice.append(data[0] + ": {" + data[1] + ": " + data[2] + "}")

    #print("\n".join(final))

    f=open('D:\\Universita\\basi di dati avanzate - Dataset veronacard\\dati_2020 by user grouped more info.csv','w')
    for ele in final:
        f.write(ele+'\n')

    f.close()

    #{2020-11-03 15:25:08: {Torre Lamberti: 41}, 2020-11-03 16:25:08: {Arena: 80}}

    #2020-11-03 15:25:08,Torre Lamberti,41,0403A6F2185081,2020-11-03,vrcard-24-2019
    #2020-11-03 15:11:50,Casa Giulietta,28,0403A6F2185081,2020-11-03,vrcard-24-2019
    #2020-11-02 11:50:37,San Zeno,26,04EBA64A216280,2020-11-01,vrcard-48-2019
'''
#versione senza device id e il resto

import csv

with open('D:\\Universita\\basi di dati avanzate - Dataset veronacard\\2 sol - tutto raggruppato in card\\dati_2020 by user.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')

    line_count = 0

    card = ''
    activation = ''
    profile = ''
    final = []
    entrancePoiDevice = []

    #entrance_timestamp,poi name,device_id,card_serial,activation_date,profile_type
    for data in csv_reader:        
        if data[3] != card: #card serial
            final.append(card + "|{" + ', '.join(entrancePoiDevice) + "}")

            entrancePoiDevice.clear()

            card = data[3]
        
        entrancePoiDevice.append("'" + data[0] + "': '" + data[1] + "'")

    #print("\n".join(final))

    f=open('D:\\Universita\\basi di dati avanzate - Dataset veronacard\\2 sol - tutto raggruppato in card\\dati_2020 by user grouped.csv','w')
    for ele in final:
        f.write(ele+'\n')

    f.close()


