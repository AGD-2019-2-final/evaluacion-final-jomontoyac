#import sys
#
# >>> Escriba el codigo del mapper a partir de este punto <<<
#
#%%writefile mapper.py
##
## Esta es la funcion que mapea la entrada a parejas (clave, valor)
##
#%%writefile mapper.py
#! /usr/bin/env python

##
## Esta es la funcion que mapea la entrada a parejas (clave, valor)
##
import sys


##
## Se usa una clase iterable para implementar el mapper.
##

class Mapper:

    def __init__(self, stream):
        ##
        ## almacena el flujo de entrada como una
        ## variable del objeto
        ##
        self.stream = stream

    def emit(self, key, value):
        ##
        ## escribe al flujo estandar de salida
        ##
        sys.stdout.write("{}\t{}\n".format(key, value))


    def status(self, message):
        ##
        ## imprime un reporte en el flujo de error
        ## no se debe usar el stdout, ya que en este
        ## unicamente deben ir las parejas (key, value)
        ##
        sys.stderr.write('reporter:status:{}\n'.format(message))


    def counter(self, counter, amount=1, group="ApplicationCounter"):
        ##
        ## imprime el valor del contador
        ##
        sys.stderr.write('reporter:counter:{},{},{}\n'.format(group, counter, amount))

    def map(self):

        word_counter = 0

        ##
        ## imprime un mensaje a la entrada
        ##
#         self.status('Iniciando procesamiento ')

        for word in self:
            ##
            ## cuenta la cantidad de palabras procesadas
            ##
            word_counter += 1

            ##
            ## por cada palabra del flujo de datos
            ## emite la pareja (word, 1)
            ##
            self.emit(key=word[0].strip(), value=word[1])

        ##
        ## imprime un mensaje a la salida
        ##
#         self.counter('num_words', amount=word_counter)
#         self.status('Finalizadno procesamiento ')




    def __iter__(self):
        ##
        ## itera sobre cada linea de codigo recibida
        ## a traves del flujo de entrada
            
        
        for line in self.stream:
#             sys.stdout.write(line)
            ##
            ## itera sobre cada palabra de la linea
            ## (en los ciclos for, retorna las palabras
            ## una a una)
            ##
            word = line.split(',')[3]
            value = line.split(',')[4]
#             for word in line.split():
#                 sys.stdout.write(word)
                ##
                ## retorna la palabra siguiente en el
                ## ciclo for
                ##
#             self.emit(key = line, value = 1)
            yield (word, value)


if __name__ == "__main__":
    ##
    ## inicializa el objeto con el flujo de entrada
    ##
    mapper = Mapper(sys.stdin)

    ##
    ## ejecuta el mapper
    ##
    mapper.map()