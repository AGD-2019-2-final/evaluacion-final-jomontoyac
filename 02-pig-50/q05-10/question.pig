-- 
-- Pregunta
-- ===========================================================================
-- 
-- Para el archivo `data.tsv` compute Calcule la cantidad de registros en que 
-- aparece cada letra minúscula en la columna 2.
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

filtro = FOREACH tabla GENERATE FLATTEN(f2);
grouped = GROUP filtro by $0;
conteo = FOREACH grouped GENERATE $0, COUNT(filtro);
STORE conteo INTO 'output' USING PigStorage('\t');