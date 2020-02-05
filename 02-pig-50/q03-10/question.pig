-- Pregunta
-- ===========================================================================
-- 
-- Obtenga los cinco (5) valores más pequeños de la 3ra columna.
-- Escriba el resultado a la carpeta `output` del directorio actual.
-- 
fs -rm -f -r output;
--
-- >>> Escriba su respuesta a partir de este punto <<<
--
u = LOAD 'data.tsv' USING PigStorage('\t')
    AS (letter:CHARARRAY,
        date:CHARARRAY,
       num:INT);

y = ORDER u BY $2;
z = FOREACH (LIMIT y 5) GENERATE $2;
STORE z INTO 'output';