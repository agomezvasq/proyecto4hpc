import os.path
import timeit
import sys
import pandas as pd
import unicodedata
import re
import math

frecs = {} #{ 'car': {17065: 1, 17066: 2}, 'bike': {17065: 5, 17066: 4, 17034: 6} }
titles = {}

rx = re.compile(r'[.,:;?\"!()\[\]{}]')


def aggregate(word):
    frec = 0
    for docId, docFrec in frecs[word].items():
        frec += docFrec
    return frec


start_time = timeit.default_timer()
tn_articles = 0
i = 1
while os.path.isfile('/opt/datasets/articles%d.csv' % i):
    print('Reading file /opt/datasets/articles%d.csv' % i)
    df = pd.read_csv('/opt/datasets/articles%d.csv' % i, index_col='Unnamed: 0', encoding='utf-8')
    n_articles = 0
    file_start_time = timeit.default_timer()
    for row in df.itertuples():
        docId = int(row[1])
        title = unicodedata.normalize('NFKD', str(row[2])).strip(u'\n ')
        publication = unicodedata.normalize('NFKD', str(row[3]))
        author = unicodedata.normalize('NFKD', str(row[4]))
        date = str(row[5])
        if not math.isnan(row[6]):
            year = str(round(row[6]))
        else:
            year = ''
        content = unicodedata.normalize('NFKD', str(row[9]))

        titles[docId] = title

        allContent = ' '.join([title, publication, author, date, year, content])

        #Replacing weird quotes by normal quotes
        allContent = allContent.replace('\u2018', '\'')
        allContent = allContent.replace('\u2019', '\'')
        allContent = allContent.replace('\u201c', '"')
        allContent = allContent.replace('\u201d', '"')
        allContent = allContent.replace('\u2014', '-')

        #Unnecessary characters
        allContent = rx.sub(' ', allContent)
        
        #Make it lowercase
        allContent = allContent.lower()

        sp = allContent.split(' ')
        #Remove empty strings
        sp = filter(None, sp)
        for word in sp:      
            #Strip spaces      
            word = word.strip(' ')

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
            time = (timeit.default_timer() - file_start_time) * 1000
            print('Read ' + str(n_articles) + ' articles (' + str(round(time)) + 'ms)')
    time = (timeit.default_timer() - file_start_time) * 1000
    print('Read ' + str(n_articles) + ' articles in /opt/datasets/articles' + str(i) + '.csv (' \
          + str(round(time)) + 'ms)')
    tn_articles += n_articles
    i += 1
                
print('Tiempo: ' + str(int(round((timeit.default_timer() - start_time) * 1000))) + 'ms')
print(str(len(frecs)) + ' palabras en ' + str(tn_articles) + ' articulos')
#100 most used words
aggregated = {word: aggregate(word) for word, wordFrecs in frecs.items()}
print(sorted(aggregated.items(), key=lambda x: x[1], reverse=True)[:100])
print('a: ' + str(aggregate('a')))
print('the: ' + str(aggregate('the')))
print('house: ' + str(aggregate('house')))

print('')

while True:
    word = input('Entrar la palabra (\quit para salir): ')
    word = word.lower()

    if word == '\quit':
        sys.exit(0)

    if word in frecs:
        #Sort article frequencies by greatest
        #Only 10 first items
        for docId, frec in sorted(frecs[word].items(), key=lambda x: x[1], reverse=True)[:10]:
            print(str(frec) + ', ' + str(docId) + ', ' + str(titles[docId]))
    else:
        print('No existe la palabra en la base de datos!')
    print('')
