--
-- Pregunta
-- ===========================================================================
-- 
-- Para responder la pregunta use el archivo `data.csv`.
-- 
-- Escriba el código equivalente a la siguiente consulta en SQL.
-- 
--    SELECT
--        color
--    FROM 
--        u 
--    WHERE 
--        color 
--    LIKE 'b%';
-- 
-- Escriba el resultado a la carpeta `output` del directorio actual.
-- 
fs -rm -f -r output;
--
u = LOAD 'data.csv' USING PigStorage(',') 
    AS (id:int, 
        firstname:CHARARRAY, 
        surname:CHARARRAY, 
        birthday:CHARARRAY, 
        color:CHARARRAY, 
        quantity:INT);
--
-- >>> Escriba su respuesta a partir de este punto <<<
--
b = FILTER (FOREACH u GENERATE color)
    BY SUBSTRING(color,0,1) == 'b';

STORE b INTO 'output' USING PigStorage(',');