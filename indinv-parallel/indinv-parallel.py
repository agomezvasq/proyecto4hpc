import os.path
import timeit
import sys
import pandas as pd
import unicodedata
import re
from mpi4py import MPI
import math
import numpy as np

comm = MPI.COMM_WORLD
rank = comm.rank
size = comm.size

#Has to be a multiple of 3
N_PROCS = size

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


#print('Hello from rank ' + str(rank))
start_time = timeit.default_timer()

if rank == 0:
    global_start_time = timeit.default_timer()

if rank in [0, 1, 2]:
    sent_phase_start_time = timeit.default_timer()
    print('From rank ' + str(rank) + ":")
    print('Loading files into memory...')
    i = rank + 1
    print('Reading file /opt/datasets/articles%d.csv' % i)
    sys.stdout.flush()
    #Read per 2000 articles and pass to workers
    reader = pd.read_csv('/opt/datasets/articles%d.csv' % i, index_col='Unnamed: 0', encoding='utf-8', chunksize=5000)
    parent = -1
    j = 1
    for worker_chunk in reader:
        #0, 1, 2, 0, 1, 2, 0, 1, 2...
        if rank + j * 3 >= N_PROCS:
            raise Exception('NOT ENOUGH PROCESSES, rank + j * 3 = ' + str(rank + j * 3))
        comm.send(worker_chunk, dest=rank + j * 3)
        j += 1

    n_workers = j - 1

    time = round((timeit.default_timer() - sent_phase_start_time) * 1000)
    #print('Successfully sent ' + str(j - 1) + ' chunks of size ' + str(int(150000 / N_PROCS)) + ' to workers from Rank ' + str(rank) + ' in ' + str(time) + 'ms')
    sys.stdout.flush()
else:
    parent = rank % 3
    chunk = comm.recv(source=parent)
    
    print('Received ' + str(chunk.shape[0]) + ' articles in Rank ' + str(rank) + ' from my parent ' + str(parent))
    sys.stdout.flush()

    n_articles = 0
    start_time = timeit.default_timer()
    #Iterate chunk
    for row in chunk.itertuples():
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
    #print(str(rank) + ': Tiempo: ' + str(int(round((timeit.default_timer() - start_time) * 1000))) + 'ms')
    print(str(rank) + ': ' + str(len(frecs)) + ' palabras en ' + str(n_articles) + ' articulos (' + str(round((timeit.default_timer() - start_time) * 1000)) + 'ms)')
    sys.stdout.flush()

#Report
if rank not in [0, 1, 2]:
    comm.send({ 'frecs': frecs, 'titles': titles }, dest=parent, tag=0)
else:
    if rank == 1:
        st = timeit.default_timer()
    #Iterate number of workers
    for i in range(n_workers):
        status = MPI.Status()
        data = comm.recv(source=MPI.ANY_SOURCE, status=status, tag=0)
        received_frecs = data['frecs']
        received_titles = data['titles']
        source_rank = status.Get_source()
        print('I received ' + str(len(received_frecs.items())) + ' words in Rank ' + str(rank) + ' from my worker ' + str(source_rank))
        sys.stdout.flush()
        for word, wordFrecs in received_frecs.items():
            if word in frecs:
                frecs[word].update(wordFrecs)
            else:
                frecs[word] = wordFrecs
        titles.update(received_titles)

    if rank == 1:
        print('agg time: ' + str(round((timeit.default_timer() - st) * 1000)) + 'ms')
    print('I got ' + str(len(frecs.keys())) + ' words from ' + str(len(titles)) + ' articles in Rank ' + str(rank))
    sys.stdout.flush()

sys.stdout.flush()

#Now lets wait for 0 to query a word
if rank in [1, 2]:
    #Ready signal
    comm.send(None, dest=0, tag=1)

    while True:
        word = comm.recv(source=0)
        wordFrecs = {}
        wordTitles = {}
        if word in frecs:
            wordFrecs = frecs[word]
            for docId in wordFrecs.keys():
                wordTitles[docId] = titles[docId]
        comm.send({ 'frecs': wordFrecs, 'titles': wordTitles }, dest=0)


def search(word):
    wordFrecs = {}
    wordTitles = {}
    if word in frecs:
        #Copy frecs
        for docId, docFrec in frecs[word].items():
            wordFrecs[docId] = docFrec
            wordTitles[docId] = titles[docId]
    #Copy frecs from the other mates
    for i in [1, 2]:
        comm.send(word, dest=i)
        data = comm.recv(source=i)
        wordFrecs.update(data['frecs'])
        wordTitles.update(data['titles'])
    return wordFrecs, wordTitles


if rank == 0:
    for i in range(2):
        comm.recv(source=MPI.ANY_SOURCE, tag=1)
    print('')
    print(str(round((timeit.default_timer() - global_start_time) * 1000)) + 'ms')
    sys.stdout.flush()

if rank == -1:
    #100 most used words
    aggregated = {word: aggregate(word) for word, wordFrecs in frecs.items()}
    print(sorted(aggregated.items(), key=lambda x: x[1], reverse=True)[:100])
    print('a: ' + str(aggregate('a')))
    print('the: ' + str(aggregate('the')))
    print('house: ' + str(aggregate('house')))

    print('')
    sys.stdout.flush()

if rank == 0:
    while True:
        word = input('Entrar la palabra (\quit para salir): ')
    #    word = word.decode('utf-8')
        word = word.lower()

        if word == '\quit':
            MPI.Finalize()
            sys.exit(0)

        wordFrecs, wordTitles = search(word)
        if wordFrecs:
            #Sort article frequencies by greatest
            #Only 10 first items
            for docId, frec in sorted(wordFrecs.items(), key=lambda x: x[1], reverse=True)[:10]:
                print(str(frec) + ', ' + str(docId) + ', ' + wordTitles[docId])
        else:
            print('No existe la palabra en la base de datos!')
        print('')
        sys.stdout.flush()
else:
    MPI.Finalize()
    sys.exit(0)
