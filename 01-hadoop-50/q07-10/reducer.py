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
        sys.stdout.write("{}\t{}\t{}\n".format(key, value[0], value[1]))

    def reduce(self):
        ##
        ## Esta funcion reduce los elementos que
        ## tienen la misma clave
        ##
#         keys = []
#         values = []
        
        for key, group in itertools.groupby(self, lambda x: x[0]):
#             sys.stdout.write(key)
#             keys.append(key)
            dict_tabla = {}
            for _, val in group:
                dict_tabla[val[0]] = val[1]
            
            for k, v in sorted(dict_tabla.items(), key=lambda item: item[1]):
                self.emit(key=key, value=(k,v))

            
#             self.emit(key = key, maxvalue = maximo, minvalue = minimo)
# #                 values.append(val)
#                 dict_tabla[key] = val
                

#         sys.stdout.write(str(values[1]))
                

#             

    def __iter__(self):

        for line in self.stream:
#             sys.stdout.write(line)
#             sys.stdout.write(line.split(',')[0])
            ##
            ## Lee el stream de datos y lo parte
            ## en (clave, valor)
            ##
            key, val = line.split('\t')
#             sys.stdout.write(val[1:-2])            
            val = tuple(val[1:-2].strip().split(','))
#             sys.stdout.write(int(val[1]))    
    
            val = tuple([val[0].strip()[1:-1],int(val[1].strip()[1:-1])])
#             sys.stdout.write(str(val))

            ##
            ## retorna la tupla (clave, valor)
            ## como el siguiente elemento del ciclo for
            ##
            yield (key, val)


if __name__ == '__main__':

    reducer = Reducer(sys.stdin)
    reducer.reduce()