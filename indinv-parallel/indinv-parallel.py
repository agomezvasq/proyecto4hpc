import os.path
import timeit
import sys
import pandas as pd
import unicodedata
import re
from mpi4py import MPI
import numpy as np

comm = MPI.COMM_WORLD
rank = comm.rank
size = comm.size

if rank == 0:
    version = MPI.Get_version()
    print('MPI version is: ' + str(version))
    sys.stdout.flush()

    maxprocs = MPI.INFO_ENV.get('maxprocs')
    print('Maximum number of processes: ' + str(maxprocs))
    sys.stdout.flush()

frecs = {} #{ 'car': {17065: 1, 17066: 2}, 'bike': {17065: 5, 17066: 4, 17034: 6} }
titles = {}

rx = re.compile(r'[.,:;?\"!()\[\]{}]')
#Test regex
#print(rx.sub(u' ', u'(hola()) como [estas] hey? \'yo no se\' :v ;v hey!!'))


def aggregate(word):
    frec = 0
    for docId, docFrec in frecs[word].items():
        frec += docFrec
    return frec


print('Hello from rank ' + str(rank))
start_time = timeit.default_timer()
print('From rank ' + str(rank) + ":")
print('Loading files into memory...')
i = rank + 1
print('Reading file /opt/datasets/articles%d.csv' % i)
sys.stdout.flush()
df = pd.read_csv('/opt/datasets/articles%d.csv' % i, index_col='Unnamed: 0', encoding='utf-8')

print('Loaded into memory in ' + str(round((timeit.default_timer() - start_time) * 1000)) + 'ms')

#Number of articles
N = df.shape[0]
print('Rank ' + str(rank) + ' got ' + str(N) + ' articles')
sys.stdout.flush()

import math

if rank == 0:
    global_start_time = timeit.default_timer()
n_articles = 0
start_time = timeit.default_timer()
for row in df.itertuples():
    docId = int(row[1])
    title = unicodedata.normalize('NFKD', str(row[2])).strip('\n ')
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
        time = round((timeit.default_timer() - start_time) * 1000)
        print('Rank ' + str(rank) + ' read ' + str(n_articles) +  ' articles (' + str(time) + 'ms)')
        sys.stdout.flush()
print('Tiempo: ' + str(int(round((timeit.default_timer() - start_time) * 1000))) + 'ms')
print(str(len(frecs)) + ' palabras en ' + str(n_articles) + ' articulos')
sys.stdout.flush()



if rank != 0:
    comm.send({ 'frecs': frecs, 'titles': titles}, dest=0)
else:
    received_frecs_list = []
    received_titles_list = []
    for i in range(1, size):
        status = MPI.Status()
        data = comm.recv(source=MPI.ANY_SOURCE, status=status)
        received_frecs = data['frecs']
        received_titles = data['titles']
        source_rank = status.Get_source()
        print('I received ' + str(len(received_frecs.items())) + ' words from rank ' + str(source_rank))
        sys.stdout.flush()
        received_frecs_list.append(received_frecs)
        received_titles_list.append(received_titles)
    for received_frecs in received_frecs_list:
        for word, wordFrecs in received_frecs.items():
            if word in frecs:
                frecs[word].update(wordFrecs)
            else:
                frecs[word] = wordFrecs
    for received_titles in received_titles_list:
        titles.update(received_titles)

comm.Barrier()

if rank == 0:
    print('')
    print(str(round((timeit.default_timer() - global_start_time) * 1000)) + 'ms')
    sys.stdout.flush()

#100 most used words
aggregated = {word: aggregate(word) for word, wordFrecs in frecs.items()}
print(sorted(aggregated.items(), key=lambda x: x[1], reverse=True)[:100])
print('a: ' + str(aggregate('a')))
print('the: ' + str(aggregate('the')))
print('house: ' + str(aggregate('house')))

if rank == 0:
    while True:
        word = input('Entrar la palabra (\quit para salir): ')
    #    word = word.decode('utf-8')
        word = word.lower()

        if word == '\quit':
            sys.exit(0)

        if word in frecs:
            #Sort article frequencies by greatest
            #Only 10 first items
            for docId, frec in sorted(frecs[word].items(), key=lambda x: x[1], reverse=True)[:10]:
                print(str(frec) + ', ' + str(docId) + ', ' + titles[docId])
        else:
            print('No existe la palabra en la base de datos!')
        print('')
        sys.stdout.flush()
