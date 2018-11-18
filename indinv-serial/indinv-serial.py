import codecs
from io import open
import csv
import timeit

frecs = {} #{ 'car': {17065: 1, 17066: 2}, 'bike': {17065: 5, 17066: 4, 17034: 6} }


def aggregate(word):
    frec = 0
    for docId, docFrec in frecs[word].iteritems():
        frec += docFrec
    return frec


with open('/opt/datasets/articles1.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    #Scrap header
    next(reader)
    docIds = []
    start_time = timeit.default_timer()
    for i in range(100):
        line = next(reader)

        docId = int(line[1])
        title = line[2].decode('utf-8')
        content = line[9].decode('utf-8')

        docIds.append(docId)

        allContent = title + ' ' + content
        sp = allContent.split(' ')
        #Remove empty strings
        sp = filter(None, sp)
        for word in sp:            
            #Strip unnecessary characters                                                                                                                                            
            word = word.strip(' ,.:;?!')
            #Make it lowercase
            word = word.lower()

            #Create dict for word in frecs
            if not word in frecs:
                frecs[word] = {}

            #Add frequency
            if docId in frecs[word]:
                frecs[word][docId] += 1
            else:
                frecs[word][docId] = 1
    print('Ellapsed: ' + str(int(round((timeit.default_timer() - start_time) * 1000))) + 'ms')
    print(docIds)
    print(len(frecs))
    print('a: ' + str(aggregate('a')))
    print('the: ' + str(aggregate('the')))
    print('house: ' + str(aggregate('house')))
