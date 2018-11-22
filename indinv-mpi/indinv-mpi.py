import timeit
import sys
import pandas as pd
import unicodedata
import re
from mpi4py import MPI
import math

comm = MPI.COMM_WORLD
rank = comm.rank
size = comm.size

#Has to be a multiple of 3. N_PROCS - 3 corresponds to the number of workers
N_PROCS = size

#Chunk size * (N_PROCS - 3) must be > than the total number of articles
CHUNK_SIZE = 5000

#Useful data structs
frecs = {} #{ 'car': {17065: 1, 17066: 2}, 'bike': {17065: 5, 17066: 4, 17034: 6} }
titles = {} #{ 17065: 'Title 1', 17066: 'Title 2' }


def aggregate(word):
    frec = 0
    for docId, docFrec in frecs[word].items():
        frec += docFrec
    return frec


if rank == 0:
    print('Working with ' + str(N_PROCS) + ' processes. ' + str(CHUNK_SIZE) + ' articles per worker')
    print('Worker sets: ')
    print('0 -> ' + ', '.join([str(x) for x in range(3, N_PROCS, 3)]))
    print('1 -> ' + ', '.join([str(x) for x in range(4, N_PROCS, 3)]))
    print('2 -> ' + ', '.join([str(x) for x in range(5, N_PROCS, 3)]))
    sys.stdout.flush()

    global_start_time = timeit.default_timer()

comm.Barrier()

#Send phase
if rank in [0, 1, 2]:
    i = rank + 1
    print(str(rank) + ': Reading file /opt/datasets/articles%d.csv' % i)
    sys.stdout.flush()

    #Read per CHUNK_SIZE articles and pass to workers
    reader = pd.read_csv('/opt/datasets/articles%d.csv' % i, index_col='Unnamed: 0', encoding='utf-8', chunksize=CHUNK_SIZE)
    j = 1
    for worker_chunk in reader:
        #0, 1, 2, w0, w1, w2, w0, w1, w2...
        if rank + j * 3 >= N_PROCS:
            #Increase the number of processes or CHUNK_SIZE or the analysis will be incomplete
            raise Exception('NOT ENOUGH PROCESSES, rank + j * 3 = ' + str(rank + j * 3))
        comm.send(worker_chunk, dest=rank + j * 3)
        j += 1

    n_workers = j - 1
else:
    parent = rank % 3
    #Receive chunk from parent
    chunk = comm.recv(source=parent)
    
    print('Received ' + str(chunk.shape[0]) + ' articles in Rank ' + str(rank) + ' from my parent ' + str(parent))
    sys.stdout.flush()

    #Process phase
    rx = re.compile(r'[.,:;?\"!()\[\]{}]')

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
    time = round((timeit.default_timer() - start_time) * 1000)
    print('Processed ' + str(len(frecs)) + ' words in ' + str(n_articles) + ' articles in Rank ' + str(rank) + ' (' + str(time) + 'ms)')
    sys.stdout.flush()

#Communicate phase
if rank not in [0, 1, 2]:
    #Send results from the workers to parents
    comm.send({ 'frecs': frecs, 'titles': titles }, dest=parent, tag=0)
else:
    #Receive results, iterate number of workers
    for i in range(n_workers):
        status = MPI.Status()
        data = comm.recv(source=MPI.ANY_SOURCE, status=status, tag=0)
        source_rank = status.Get_source()
        
        received_frecs = data['frecs']
        received_titles = data['titles']
        print('I received ' + str(len(received_frecs.items())) + ' words in Rank ' + str(rank) + ' from my worker ' + str(source_rank))
        sys.stdout.flush()
        
        for word, wordFrecs in received_frecs.items():
            if word in frecs:
                frecs[word].update(wordFrecs)
            else:
                frecs[word] = wordFrecs
        titles.update(received_titles)

    print('I got ' + str(len(frecs.keys())) + ' words from ' + str(len(titles)) + ' articles from my workers in Rank ' + str(rank))
    sys.stdout.flush()

sys.stdout.flush()


#This is for optimization. 0, 1 and 2 have their own databases. So 0 searches in his own and queries 1 and 2's databases to search for the actual word
def search(word):
    wordFrecs = {}
    wordTitles = {}

    #Search in his own database
    if word in frecs:
        #Copy frecs
        for docId, docFrec in frecs[word].items():
            wordFrecs[docId] = docFrec
            wordTitles[docId] = titles[docId]

    #And query from the other databases
    for i in [1, 2]:
        comm.send(word, dest=i)
        data = comm.recv(source=i)

        wordFrecs.update(data['frecs'])
        wordTitles.update(data['titles'])
    return wordFrecs, wordTitles


#Now lets wait for 0 to query words
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

#Search phase
if rank == 0:
    #I need my 2 mates to be ready
    for i in range(2):
        comm.recv(source=MPI.ANY_SOURCE, tag=1)
    
    print('')
    print('Total time: ')
    print(str(round((timeit.default_timer() - global_start_time) * 1000)) + 'ms')
    print('')
    sys.stdout.flush()

    while True:
        word = input('Enter query (CTRL-C to exit): ')
        word = word.lower()

        #This doesn't exit the process for some reason
        if word == '\quit':
            break

        #From rank 0, search the word in 0, 1 and 2's databases
        wordFrecs, wordTitles = search(word)
        if wordFrecs:
            #Sort article frequencies by greatest
            #Only 10 first items
            for docId, frec in sorted(wordFrecs.items(), key=lambda x: x[1], reverse=True)[:10]:
                print(str(frec) + ', ' + str(docId) + ', ' + wordTitles[docId])
        else:
            print('The word doesn\'t exist in the database')
        print('')
        sys.stdout.flush()

sys.exit(0)
MPI.Finalize()
