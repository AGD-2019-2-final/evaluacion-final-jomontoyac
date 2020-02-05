-- Pregunta
-- ===========================================================================
-- 
-- Ordene el archivo `data.tsv`  por letra y valor (3ra columna).
-- Escriba el resultado a la carpeta `output` del directorio actual.
-- 
fs -rm -f -r output;
-- 
--  >>> Escriba el codigo del mapper a partir de este punto <<<
-- 

u = LOAD 'data.tsv' USING PigStorage('\t')
    AS (letter:CHARARRAY,
        date:CHARARRAY,
       num:INT);

y = ORDER u BY $0,$2;
STORE y INTO 'output';
