-- Pregunta
-- ===========================================================================
-- 
-- Para el archivo `data.tsv` compute la cantidad de registros por letra de la 
-- columna 2 y clave de al columna 3; esto es, por ejemplo, la cantidad de 
-- registros en tienen la letra `b` en la columna 2 y la clave `jjj` en la 
-- columna 3 es:
-- 
--   ((b,jjj), 216)
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

x = FOREACH tabla GENERATE FLATTEN(f2), FLATTEN(f3);
f = FOREACH x GENERATE $0,$1;
grouped = GROUP f BY ($0,$1);
conteo = FOREACH grouped GENERATE $0, COUNT(f);      
STORE conteo INTO 'output' USING PigStorage('\t');