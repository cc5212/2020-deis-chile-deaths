raw = LOAD 'hdfs://cm:9000/uhadoop2020/patosdepana/data-deis-100k.tsv' USING PigStorage('\t') AS 
    (id_fallecido, anho_def, fecha_def, glosa_sexo, 
    edad_cant, glosa_edad_tipo, glosa_est_civil, glosa_nivel_ins,
    glosa_ocupacion, glosa_local_def, glosa_reg_res, glosa_comuna_residencia,
    glosa_categoria_diag1, glosa_capitulo_diag1);

-- [1-12), [12-18), [18-30), [30-50), 50-65, 65-XXX
anhos = FILTER raw BY (glosa_edad_tipo == 'Edad en años') AND (anho_def > 1997);

anho_edad = FOREACH anhos GENERATE anho_def, edad_cant, glosa_categoria_diag1;

edad_causa_grouped = GROUP anho_edad BY (edad_cant, glosa_categoria_diag1);
edad_causa_count = FOREACH edad_causa_grouped GENERATE COUNT($1) AS count_causa, group AS edad_causa_pair;

count_edad_causa = FOREACH edad_causa_count GENERATE $0, $1.edad_cant as edad, $1.glosa_categoria_diag1 as causa;
edad_grouped = GROUP count_edad_causa BY edad;


ninhos = FILTER anho_edad BY (edad_cant < 12);
ninhos_grouped = GROUP ninhos BY edad_cant;
ninhos_count = FOREACH ninhos_grouped GENERATE COUNT($1) AS anho_count, group AS edad_pair;


adolescentes = FILTER anho_edad BY (edad_cant >= 12) AND  (edad_cant < 18);
adolescentes_grouped = GROUP adolescentes BY edad_cant;
adolescentes_count = FOREACH adolescentes_grouped GENERATE COUNT($1) AS anho_count, group AS edad_pair;


adulto_joven = FILTER anho_edad BY (edad_cant >= 18) AND (edad_cant < 30);
adulto_joven_grouped = GROUP adulto_joven BY edad_cant;
adulto_joven_count = FOREACH adulto_joven_grouped GENERATE COUNT($1) AS anho_count, group AS edad_pair;


adulto = FILTER anho_edad BY (edad_cant >= 30) AND (edad_cant < 50);
adulto_grouped = GROUP adulto BY edad_cant;
adulto_count = FOREACH adulto_grouped GENERATE COUNT($1) AS anho_count, group AS edad_pair;


adulto_no_tan_mayor = FILTER anho_edad BY (edad_cant >= 50) AND (edad_cant < 65);
adulto_no_tan_mayor_grouped = GROUP adulto_no_tan_mayor BY edad_cant;
adulto_no_tan_mayor_count = FOREACH adulto_no_tan_mayor_grouped GENERATE COUNT($1) AS anho_count, group AS edad_pair;

tatas = FILTER anho_edad BY edad_cant >= 65;
tatas_grouped = GROUP tatas BY edad_cant;
tatas_count = FOREACH tatas_grouped GENERATE COUNT($1) AS anho_count, group AS edad_pair;


max_edad_causa = FOREACH edad_grouped {
    ord = ORDER count_edad_causa BY count_causa DESC;
    top = LIMIT ord 1;
    GENERATE FLATTEN(top);
};

ninhos_count_causa = JOIN ninhos_count BY edad_pair, max_edad_causa BY edad;
ninhos_count_causa_por_edad = FOREACH ninhos_count_causa GENERATE $1, $0, ($4, $2), CONCAT((chararray)ROUND((float) $2/$0 * 100), '%');

adolescentes_count_causa = JOIN adolescentes_count BY edad_pair, max_edad_causa BY edad;
adolescentes_count_causa_por_edad = FOREACH adolescentes_count_causa GENERATE $1, $0, ($4, $2), CONCAT((chararray)ROUND((float) $2/$0 * 100), '%');

adulto_joven_count_causa = JOIN adulto_joven_count BY edad_pair, max_edad_causa BY edad;
adulto_joven_count_causa_por_edad = FOREACH adulto_joven_count_causa GENERATE $1, $0, ($4, $2), CONCAT((chararray)ROUND((float) $2/$0 * 100), '%');

adulto_count_causa = JOIN adulto_count BY edad_pair, max_edad_causa BY edad;
adulto_count_causa_por_edad = FOREACH adulto_count_causa GENERATE $1, $0, ($4, $2), CONCAT((chararray)ROUND((float) $2/$0 * 100), '%');

adulto_no_tan_mayor_count_causa = JOIN adulto_no_tan_mayor_count BY edad_pair, max_edad_causa BY edad;
adulto_no_tan_mayor_count_causa_por_edad = FOREACH adulto_no_tan_mayor_count_causa GENERATE $1, $0, ($4, $2), CONCAT((chararray)ROUND((float) $2/$0 * 100), '%');

tatas_count_causa = JOIN tatas_count BY edad_pair, max_edad_causa BY edad;
tatas_count_causa_por_edad = FOREACH tatas_count_causa GENERATE $1, $0, ($4, $2), CONCAT((chararray)ROUND((float) $2/$0 * 100), '%');


STORE anho_causa_count INTO '/uhadoop2020/patosdepana/rangosetarios/';