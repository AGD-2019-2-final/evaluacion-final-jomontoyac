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
#         keys = []
#         values = []
#         dict_tabla = {}
#         value = 10e6
#         max_counter = 6
        counter = 0
        
        for key, group in itertools.groupby(self, lambda x: x[0]):
#             sys.stdout.write(key)
#             keys.append(key)

#             suma = 0
#             promedio = 0
#             counter = 0
            container = []
            for _, val in group:
                container.append(val)
            
            val_ordenado = ','.join([str(i) for i in sorted(container)])
            self.emit(key = key, value = val_ordenado)
            
#                 dict_tabla[key] = (val[0],val[1])
#                 suma = suma + val[1]
#                 counter += 1
#             promedio = suma/counter
            
#         for orden in sorted(container, key=lambda item: item[2]):
#             self.emit(key = orden[0], value = (orden[1],orden[2]))

#             counter += 1
#             if counter == max_counter:
#                 break

            

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
            key = key.strip()
            val = int(val.strip())
#             sys.stdout.write(val[1:-2])            
#             val = tuple(val[1:-2].strip().split(','))
#             sys.stdout.write(int(val[1]))    
    
#             val = tuple([val[0].strip()[1:-1],int(val[1].strip()[1:-1])])
#             sys.stdout.write(str(val))

            ##
            ## retorna la tupla (clave, valor)
            ## como el siguiente elemento del ciclo for
            ##
            yield (key, val)


if __name__ == '__main__':

    reducer = Reducer(sys.stdin)
    reducer.reduce()