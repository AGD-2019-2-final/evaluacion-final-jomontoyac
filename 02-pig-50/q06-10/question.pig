-- 
-- Pregunta
-- ===========================================================================
-- 
-- Para el archivo `data.tsv` Calcule la cantidad de registros por clave de la 
-- columna 3. En otras palabras, cuántos registros hay que tengan la clave 
-- `aaa`?
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
filtro = FOREACH tabla GENERATE FLATTEN(f3);
grouped = GROUP filtro by $0;
conteo = FOREACH grouped GENERATE $0, COUNT(filtro);
STORE conteo INTO 'output' USING PigStorage(',');