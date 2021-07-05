import csv

with open('D:\\Google Drive\\SCUOLA\\Università\\Magistrale\\Basi di dati avanzate\\Progetto Cassandra Verona Card\\3 sol - entrate a due a due per card\\stayTime Original.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter='|')

    dataToAdd = ""
    final = []
    lineCount = 0
    flagToSkip = False

    # uuid, stayTime, poi
    # metto pass solo per evidenziare che in quelle righe non voglio fare o appendere nulla alla lista

    for data in csv_reader:    
        if lineCount == 0:
            pass
        elif lineCount == 1:
            final.append("row_id|stayTime|poi")
        elif lineCount == 2:
            pass
        elif not ''.join(data).strip():
           break
        else:
            if data[1].strip() == "0":
                pass
            else:
                final.append(data[0].strip() + "|" + data[1].strip() + "|" + data[2].strip())
        
        lineCount = lineCount + 1

    #print("\n".join(final))

    f=open('D:\\Google Drive\\SCUOLA\\Università\\Magistrale\\Basi di dati avanzate\\Progetto Cassandra Verona Card\\3 sol - entrate a due a due per card\\stayTime modificato.csv','w')
    for ele in final:
       f.write(ele+'\n')

    f.close()