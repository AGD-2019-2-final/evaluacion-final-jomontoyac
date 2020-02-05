-- Pregunta
-- ===========================================================================
-- 
-- Para el archivo `data.tsv` genere una tabla que contenga la primera columna,
-- la cantidad de elementos en la columna 2 y la cantidad de elementos en la 
-- columna 3 separados por coma. La tabla debe estar ordenada por las 
-- columnas 1, 2 y 3.
-- 
-- Escriba el resultado a la carpeta `output` del directorio actual.
-- 
fs -rm -f -r output;
--
-- >>> Escriba su respuesta a partir de este punto <<<
--
tabla = LOAD 'data.tsv' USING PigStorage('\t')
    AS (f1:CHARARRAY
        ,f2:BAG{t:TUPLE(p:CHARARRAY)}
        ,f3:MAP[]
        );

y = FOREACH tabla
        GENERATE f1 AS letra,
        COUNT(f2) AS num_second_column,
        SIZE(f3) AS num_map
;
orden = ORDER y BY letra,num_second_column,num_map;        
STORE orden INTO 'output' USING PigStorage(',');