# PCAM

**Partition:**

El particionamiento decidido se ha hecho a través de los artículos a procesar por worker. A cada worker se le envían un número de artículos definidos por el campo CHUNK_SIZE. Cuando estos artículos terminan de procesarse el worker envía sus resultados al nodo 'padre'.

**Communicate:**

Una implementación utilizando MPI + OpenMP adicionalmente podría resultar más eficiente pero se utiliza solamente MPI exclusivamente con memoria distribuída y comunicación por red. Los nodos 'workers' asignados a cada 'padre' se comunican para recibir los 'raw' artículos y devolver los resultados con el análisis de frecuencias de cada palabra por artículo.

**Agglomerate & Map**

Se asume que los hosts son máquinas virtuales en el mismo equipo, y como todos los nodos se están comunicando por red no hay muchas maneras obvias de optimizar la comunicación, así que en este paso se asume que todas las comunicaciones son rápidas, y esto fue comprobado dado que la velocidad de conexión del equipo de prueba ronda los 300Mbit/s

# Proceso serial

Cada artículo analizado se utiliza para completar un diccionario de 'frecuencias' y un diccionario útil de títulos. El diccionario de 'frecuencias' se estructura de la siguiente manera:

![Latex](https://i.imgur.com/VLV8Fvq.gif)

De esta manera se facilita el acceso al 'índice inverso' y la búsqueda de cada palabra y sus respectivas frecuencias en cada artículo es veloz y computacionalmente eficiente.

# Arquitectura final de Tareas y Datos

La versión paralela se implementó con el paradigma de MPI (Message Passing Interface) utilizando la librería de Python mpi4py. La arquitectura se define con 3 nodos o hosts principales (direcciones especificadas en hosts_mpi) y la creación de un número ilimitado de workers para ayudar a los nodos principales contando palabras.

Cada nodo principal lee cada uno de los archivos por 'chunks' de tamaño definido. Los nodos principales tienen asignados los ```ranks``` 0, 1 y 2. A partir de ahí, los procesos subsiguientes con ```rank``` > 2 se asignan a cada nodo como workers para el conteo. El módulo 3 del ```rank``` de un worker corresponde al ```rank``` de su nodo 'padre'. De tal manera que el principal envía chunks a los workers con regla ```rank + j * 3```. Los workers envían sus resultados al final y los nodos agregan estos a su propio diccionario de frecuencias. 

**Procesos:**

```0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11...```

**Workers:**

```0 -> 3, 6, 9...```

```1 -> 4, 7, 10...```

```2 -> 5, 8, 11...```

En querying, el nodo 0 que imprime el prompt para recibir el input del usuario envía la palabra a sus compañeros, 1 y 2, para buscar en sus diccionarios y en el suyo propio, y agrega todos los resultados para imprimir los 10 primeros artículos con la mayor frecuencia de aparición de la palabra en cuestión.

Más o menos la mitad del tiempo en tests de procesamiento es invertida en la comunicación entre nodos y entre la concatenación de los resultados. El máximo rendimiento logrado es de un aumento de 12x la velocidad de la versión serial, tardando solamente 15 segundos el procesamiento completo del dataset.

Por default el ```CHUNK_SIZE = 5000``` y el ```N_PROCS = 33``` asignado (el que arrojó mejor rendimiento en los tests) asegura que el número total de artículos (~150.000) sea procesado. Si esta regla no se cumple una excepción advierte 'NOT ENOUGH PROCESSES'.
