# PCAM

# Partition

El particionamiento decidido se ha hecho a través de los artículos a procesar por worker. A cada worker se le envían un número de artículos definidos por el campo CHUNK_SIZE. Cuando estos artículos terminan de procesarse el worker envía sus resultados al nodo 'padre'.

# Communicate

Una implementación utilizando MPI + OpenMP adicionalmente podría resultar más eficiente pero se utiliza solamente MPI exclusivamente con memoria distribuída y comunicación por red. Los nodos 'workers' asignados a cada 'padre' se comunican para recibir los 'raw' artículos y devolver los resultados con el análisis de frecuencias de cada palabra por artículo.

# Agglomerate & Map

Se asume que los hosts son máquinas virtuales en el mismo equipo, y como todos los nodos se están comunicando por red no hay muchas maneras obvias de optimizar la comunicación, así que en este paso se asume que todas las comunicaciones son rápidas, y esto fue comprobado dado que la velocidad de conexión del equipo de prueba ronda los 300Mbit/s
