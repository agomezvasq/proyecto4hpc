# HPC

Este proyecto es una implementación tanto serial como paralelizada de la creación de un 'Índice inverso' para indexar un conjunto de palabras y su frecuencia en cada uno de los artículos de una base de datos estructurada descargada de kaggle:

https://www.kaggle.com/snapcrack/all-the-news


# Datos

Un dataset conformado por 3 archivos csv, cada uno de ~50.000 artículos organizados por propiedades como título, autor, fecha, datos... Algunos de estos campos a partir de los cuáles se generará el índice inverso, por palabra y su frecuencia en cada artículo.

La lectura y partición de los archivos csv se realiza con la librería pandas.

# Ejecución serial

La ejecución serial en el dataset, ejecutada en una máquina con 16 procesadores Intel Xeon E6-2650 (8 cores, 16 threads c/u) tarda alrededor de 150s, o 50s por cada archivo, o un promedio de 20ms por cada artículo.

**Requisitos:** Python >3.6, requirements.txt

Comando:

```
python indinv-serial.py
```

**(DCA)**

```
/opt/anaconda3/bin/python indinv-serial.py
```


# Ejecución paralela (MPI)

La versión paralela se implementó con el paradigma de MPI (Message Passing Interface) utilizando la librería de Python mpi4py. Está configurada para 3 nodos o 'hosts' principales (leer hosts_mpi) y la creación de un número ilimitado de sub workers para ayudar a los nodos principales a contar palabras.

El mejor rendimiento en tests con la arquitectura descrita en el documento pcam.md en el mismo computador con 16 procesadores Intel Xeon E6-2650 (8 cores, 16 threads c/u) fue de 12x la velocidad de la versión serial, tardando 15s para el completo procesamiento del dataset, utilizando chunks de 5000 artículos y un total de 33 procesos: 3 nodos principales + 10 * 3 workers.

**Requisitos:** Python >3.6, requirements.txt, gcc y una instalación funcional de MPI

Comando:

```
mpiexec -f ../hosts_mpi -n 33 /opt/anaconda3/bin/python indinv-mpi.py
```

Si se ejecuta el programa con menos de 33 procesos y el mismo chunksize de 5000 se producirá una excepción en varios de los nodos que advierte 'NOT ENOUGH PROCESSES'.

# Referencias

https://en.wikipedia.org/wiki/Inverted_index

https://mpi4py.readthedocs.io/en/stable/tutorial.html

https://info.gwdg.de/~ceulig/docs-dev/doku.php?id=en:services:application_services:high_performance_computing:mpi4py

https://pandas.pydata.org/pandas-docs/stable/

https://cmdlinetips.com/2018/01/how-to-load-a-massive-file-as-small-chunks-in-pandas/

https://docs.python.org/3/

https://www.mcs.anl.gov/~itf/dbpp/text/node15.html

https://stackoverflow.com


