# HPC

Este proyecto es una implementación tanto serial como paralelizada de la creación de un 'Índice inverso' para indexar un conjunto de palabras y su frecuencia en cada uno de los artículos de una base de datos estructurada descargada de kaggle:

https://www.kaggle.com/snapcrack/all-the-news


# Datos

Un dataset conformado por 3 archivos csv, cada uno de ~50.000 artículos organizados por propiedades como título, autor, fecha, datos... Algunos de estos campos a partir de los cuáles se generará el índice inverso, por palabra y su frecuencia en cada artículo.


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

Cada nodo se encarga de cada uno de los archivos, aprovechando que son 3 nodos y 3 archivos csv, estos con 
```rank``` 0, 1 y 2. Cada uno de estos nodos utiliza los procesos subsiguientes cuyo ```rank``` es > 2 y cuyo módulo 3 corresponde a su propio ```rank```.

Ejemplo:

**Procesos:**

0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11...

**Asignación de 'tareas':**

0 -> 3, 6, 9...
1 -> 4, 7, 10...
2 -> 5, 8, 11...

Por eso el número total de procesos debe ser un múltiplo de 3.

Cada uno de los nodos principales 0, 1, 2 envía chunks de 5000 artículos a cada uno de los workers asignados usando la regla ```rank + j * 3```.

El mejor rendimiento en tests se encontró utilizando chunks de 5000 artículos y distribuyéndolas hacia 10 workers para cada nodo, lo cual da un total de 33 procesos: 3 principales + 10 * 3 nodos principales. El diseño del algoritmo bajo la metodología pcam descrita en el documento pcam.md arrojó resultados de 12x más velocidad que la versión serial, tardando solo 15s para el procesamiento completo del dataset, de los cuales más o menos la mitad es invertida en la comunicación entre nodos y concatenación de los resultados.

Los nodos se encargan de recibir los resultados de los workers e irlos agregando a su base de datos de 'frecuencias'. 

En el proceso de querying, el nodo 0 envía un query a sus dos compañeros, 1 y 2, para buscar la palabra en sus bases de datos de frecuencias y en la suya propia, y sumariza los resultados para imprimir organizadamente los 10 primeros con las mayores frecuencias de la palabra en cuestión.

Como 5.000 * 30 = 150.000, el número de procesos de acuerdo al número de chunks por proceso asegura que todas las líneas sean procesadas.

**Requisitos:** Python >3.6, requirements.txt, gcc y una instalación funcional de MPI

Comando:

```
mpiexec -f ../hosts_mpi -n 33 /opt/anaconda3/bin/python indinv-mpi.py
```

Si se ejecuta el programa con menos de 33 procesos y el mismo chunksize de 5000 se producirá una excepción en varios de los nodos que advierte 'NOT ENOUGH PROCESSES' y el comportamiento a partir de ahí es indefinido...
