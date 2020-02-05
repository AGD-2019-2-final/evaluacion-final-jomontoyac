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

    def emit(self, key, maxvalue, minvalue):
        sys.stdout.write("{}\t{}\t{}\n".format(key, maxvalue, minvalue))

    def reduce(self):
        ##
        ## Esta funcion reduce los elementos que
        ## tienen la misma clave
        ##
#         keys = []
#         values = []
#         dict_tabla = {}
        for key, group in itertools.groupby(self, lambda x: x[0]):
#             sys.stdout.write(key)
#             keys.append(key)
            minimo = 10e6
            maximo = 0
            for _, val in group:
                if minimo > val:
                    minimo = val
                else:
                    minimo = minimo
                
                if maximo < val:
                    maximo = val
                else:
                    maximo = maximo

            
            self.emit(key = key, maxvalue = maximo, minvalue = minimo)
# #                 values.append(val)
#                 dict_tabla[key] = val
                
#         for k, v in sorted(dict_tabla.items(), key=lambda item: item[1]):
#             
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
            val = float(val)

            ##
            ## retorna la tupla (clave, valor)
            ## como el siguiente elemento del ciclo for
            ##
            yield (key, val)


if __name__ == '__main__':

    reducer = Reducer(sys.stdin)
    reducer.reduce()