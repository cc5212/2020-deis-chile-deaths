-- Genera el top de causas de muerte por rango etario. 
-- [1-12) niños
-- [12-18) adolescentes
-- [18-30) adulto joven
-- [30-50) adulto
-- [50-65) adulto no tan mayor
-- [65-...) adulto mayor
raw = LOAD 'hdfs://cm:9000/uhadoop2020/patosdepana/data-deis.tsv' USING PigStorage('\t') AS 
    (id_fallecido, anho_def, fecha_def, glosa_sexo, 
    edad_cant, glosa_edad_tipo, glosa_est_civil, glosa_nivel_ins,
    glosa_ocupacion, glosa_local_def, glosa_reg_res, glosa_comuna_residencia,
    glosa_categoria_diag1, glosa_capitulo_diag1);


anhos = FILTER raw BY (glosa_edad_tipo == 'Edad en anhos') AND (anho_def >= 1997);

anho_edad = FOREACH anhos GENERATE anho_def, edad_cant, glosa_categoria_diag1; --(año, edad, categoria)

ninhos = FILTER anho_edad BY (edad_cant < 12);
ninhos_causa_order = FOREACH (GROUP ninhos ALL) GENERATE COUNT(ninhos) as ninhos_total; --(total_ninhos) 
ninhos_grouped = GROUP ninhos BY glosa_categoria_diag1; 
ninhos_causa_count = FOREACH ninhos_grouped GENERATE COUNT($1) AS causa_count, group AS causa; --(cant, causa)
ninhos_causa_ordered = ORDER ninhos_causa_count BY causa_count DESC;
max_ninhos_causa = LIMIT ninhos_causa_ordered 1;
ninhos_final = CROSS ninhos_causa_order, max_ninhos_causa;


adolescentes = FILTER anho_edad BY (edad_cant >=12) AND (edad_cant < 18);
adolescentes_causa_order = FOREACH (GROUP adolescentes ALL) GENERATE COUNT(adolescentes) as adolescentes_total; --(total_adolescentes) 
adolescentes_grouped = GROUP adolescentes BY glosa_categoria_diag1; 
adolescentes_causa_count = FOREACH adolescentes_grouped GENERATE COUNT($1) AS causa_count, group AS causa; --(cant, causa)
adolescentes_causa_ordered = ORDER adolescentes_causa_count BY causa_count DESC;
max_adolescentes_causa = LIMIT adolescentes_causa_ordered 1;
adolescentes_final = CROSS adolescentes_causa_order, max_adolescentes_causa;


adulto_joven = FILTER anho_edad BY (edad_cant >= 18) AND (edad_cant < 30);
adulto_joven_causa_order = FOREACH (GROUP adulto_joven ALL) GENERATE COUNT(adulto_joven) as adulto_joven_total; --(total_adulto_joven) 
adulto_joven_grouped = GROUP adulto_joven BY glosa_categoria_diag1; 
adulto_joven_causa_count = FOREACH adulto_joven_grouped GENERATE COUNT($1) AS causa_count, group AS causa; --(cant, causa)
adulto_joven_causa_ordered = ORDER adulto_joven_causa_count BY causa_count DESC;
max_adulto_joven_causa = LIMIT adulto_joven_causa_ordered 1;
adulto_joven_final = CROSS adulto_joven_causa_order, max_adulto_joven_causa;


adultos = FILTER anho_edad BY (edad_cant >= 30) AND (edad_cant < 50);
adultos_causa_order = FOREACH (GROUP adultos ALL) GENERATE COUNT(adultos) as adultos_total; --(total_adultos) 
adultos_grouped = GROUP adultos BY glosa_categoria_diag1; 
adultos_causa_count = FOREACH adultos_grouped GENERATE COUNT($1) AS causa_count, group AS causa; --(cant, causa)
adultos_causa_ordered = ORDER adultos_causa_count BY causa_count DESC;
max_adultos_causa = LIMIT adultos_causa_ordered 1;
adultos_final = CROSS adultos_causa_order, max_adultos_causa;


adulto_no_tan_mayor = FILTER anho_edad BY (edad_cant >= 50) AND (edad_cant < 65);
adulto_no_tan_mayor_causa_order = FOREACH (GROUP adulto_no_tan_mayor ALL) GENERATE COUNT(adulto_no_tan_mayor) as adulto_no_tan_mayor_total; --(total_adulto_no_tan_mayor) 
adulto_no_tan_mayor_grouped = GROUP adulto_no_tan_mayor BY glosa_categoria_diag1; 
adulto_no_tan_mayor_causa_count = FOREACH adulto_no_tan_mayor_grouped GENERATE COUNT($1) AS causa_count, group AS causa; --(cant, causa)
adulto_no_tan_mayor_causa_ordered = ORDER adulto_no_tan_mayor_causa_count BY causa_count DESC;
max_adulto_no_tan_mayor_causa = LIMIT adulto_no_tan_mayor_causa_ordered 1;
adulto_no_tan_mayor_final = CROSS adulto_no_tan_mayor_causa_order, max_adulto_no_tan_mayor_causa;


adultos_mayor = FILTER anho_edad BY (edad_cant >= 65);
adultos_mayor_causa_order = FOREACH (GROUP adultos_mayor ALL) GENERATE COUNT(adultos_mayor) as adultos_mayor_total; --(total_adultos_mayor) 
adultos_mayor_grouped = GROUP adultos_mayor BY glosa_categoria_diag1; 
adultos_mayor_causa_count = FOREACH adultos_mayor_grouped GENERATE COUNT($1) AS causa_count, group AS causa; --(cant, causa)
adultos_mayor_causa_ordered = ORDER adultos_mayor_causa_count BY causa_count DESC;
max_adultos_mayor_causa = LIMIT adultos_mayor_causa_ordered 1;
adultos_mayor_final = CROSS adultos_mayor_causa_order, max_adultos_mayor_causa;


STORE ninhos_final INTO '/uhadoop2020/patosdepana/results/edad/ninhos/';
STORE adolescentes_final INTO '/uhadoop2020/patosdepana/results/edad/adolescentes/';
STORE adulto_joven_final INTO '/uhadoop2020/patosdepana/results/edad/adulto_joven/';
STORE adultos_final INTO '/uhadoop2020/patosdepana/results/edad/adultos/';
STORE adulto_no_tan_mayor_final INTO '/uhadoop2020/patosdepana/results/edad/adultos_no_tan_mayor/';
STORE adultos_mayor_final INTO '/uhadoop2020/patosdepana/results/edad/adultos_mayor/';