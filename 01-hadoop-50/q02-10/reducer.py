import sys
#
# >>> Escriba el codigo del reducer a partir de este punto <<<
#
#%%writefile reducer.py

import sys
import itertools

class Reducer:

    def __init__(self, stream):
        self.stream = stream

    def emit(self, key, value):
        sys.stdout.write("{}\t{}\n".format(key, value))

    def reduce(self):
        ##
        ## Esta funcion reduce los elementos que
        ## tienen la misma clave
        ##
        for key, group in itertools.groupby(self, lambda x: x[0]):
            
            
            maximo = 0
            for _, val in group:
                if val < maximo:
                    maximo = maximo
                else:
                    maximo = val
                

            self.emit(key=key, value=maximo)

    def __iter__(self):

        for line in self.stream:
#             sys.stdout.write(line)
#             sys.stdout.write(line.split(',')[0])
            ##
            ## Lee el stream de datos y lo parte
            ## en (clave, valor)
            ##
            key, val = line.split('\t')
            val = int(val)

            ##
            ## retorna la tupla (clave, valor)
            ## como el siguiente elemento del ciclo for
            ##
            yield (key, val)


if __name__ == '__main__':

    reducer = Reducer(sys.stdin)
    reducer.reduce()
