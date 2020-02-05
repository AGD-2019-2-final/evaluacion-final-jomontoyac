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
        sys.stdout.write("{},{}\n".format(key, value))

    def reduce(self):
        ##
        ## Esta funcion reduce los elementos que
        ## tienen la misma clave
        ##
#         keys = []
#         values = []
        dict_tabla = {}
        for key, group in itertools.groupby(self, lambda x: x[0]):
#             sys.stdout.write(key)
#             keys.append(key)
            
            for _, val in group:
#                 values.append(val)
                dict_tabla[key] = val
                
        for k, v in sorted(dict_tabla.items(), key=lambda item: item[1]):
            self.emit(key=k, value=v)
#         sys.stdout.write(str(values[1]))
                

#             self.emit(key=key, value=maximo)

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