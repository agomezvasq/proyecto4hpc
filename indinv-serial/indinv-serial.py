import os.path
import codecs
from io import open
from backports import csv
import timeit
import sys

csv.field_size_limit(sys.maxsize)


frecs = {} #{ 'car': {17065: 1, 17066: 2}, 'bike': {17065: 5, 17066: 4, 17034: 6} }
titles = {}


def aggregate(word):
    frec = 0
    for docId, docFrec in frecs[word].iteritems():
        frec += docFrec
    return frec


start_time = timeit.default_timer()
tn_articles = 0
i = 1
while os.path.isfile('/opt/datasets/articles%d.csv' % i):
    with codecs.open('/opt/datasets/articles%d.csv' % i, 'rb',  encoding='utf-8') as csvfile:
        print('Reading file /opt/datasets/articles%d.csv' % i)
        reader = csv.reader(csvfile)
        #Scrap header
        next(reader)
        n_articles = 0
        file_start_time = timeit.default_timer()
        for row in reader:
            if not row or len(row) != 10:
                print('Malformed row: ' + str(row))
                continue
            docId = int(row[1])
            title = row[2]
            content = row[9]

            titles[docId] = title

            allContent = title + ' ' + content
            #Replacing weird quotes by normal quotes
            allContent = allContent.replace(u'\u2018', u'\'')
            allContent = allContent.replace(u'\u2019', u'\'')
            allContent = allContent.replace(u'\u201c', u'"')
            allContent = allContent.replace(u'\u201d', u'"')
            allContent = allContent.replace(u'\u2014', u'-')
            #Make it lowercase
            allContent = allContent.lower()

            sp = allContent.split(' ')
            #Remove empty strings
            sp = filter(None, sp)
            for word in sp:      
                #Strip unnecessary characters      
                word = word.strip(u' ,.:;?\'\"!()[]{}')

                if not word:
                    continue

                #Create dict for word in frecs
                if not word in frecs:
                    frecs[word] = {}

                #Add frequency
                if docId in frecs[word]:
                    frecs[word][docId] += 1
                else:
                    frecs[word][docId] = 1
            n_articles += 1

            if n_articles % 1000 == 0:
                print('Read ' + str(n_articles) + ' articles (' + str(round(timeit.default_timer() - file_start_time, 1)) + 's)')
        print('Read ' + str(n_articles) + ' articles in /opt/datasets/articles' + str(i) + '.csv')
        tn_articles += n_articles
    i += 1
                
print('Tiempo: ' + str(int(round((timeit.default_timer() - start_time) * 1000))) + 'ms')
print(frecs.keys())
print(str(len(frecs)) + ' palabras en ' + str(tn_articles) + ' articulos')
print('a: ' + str(aggregate('a')))
print('the: ' + str(aggregate('the')))
print('house: ' + str(aggregate('house')))

print('')

while True:
    word = raw_input('Entrar la palabra (\quit para salir): ')
    word = word.decode('utf-8')

    if word == '\quit':
        sys.exit(0)

    if word in frecs:
        #Sort article frequencies by greatest
        #Only 10 first items
        for docId, frec in sorted(frecs[word].iteritems(), key=lambda x: x[1], reverse=True)[:10]:
            print(str(frec) + ', ' + str(docId) + ', ' + str(titles[docId].encode('utf-8')))
    else:
        print('No existe la palabra en la base de datos!')
    print('')
