-- Top de causa de muerte en las comunas de la Región Metropolitana.
raw = LOAD 'hdfs://cm:9000/uhadoop2020/patosdepana/data-deis.tsv' USING PigStorage('\t') AS 
    (id_fallecido, anho_def, fecha_def, glosa_sexo, 
    edad_cant, glosa_edad_tipo, glosa_est_civil, glosa_nivel_ins,
    glosa_ocupacion, glosa_local_def, glosa_reg_res, glosa_comuna_residencia,
    glosa_categoria_diag1, glosa_capitulo_diag1);


data = FILTER raw BY (anho_def >= 1997) AND (glosa_reg_res == 'Metropolitana de Santiago');

comuna = FOREACH data GENERATE glosa_comuna_residencia, glosa_categoria_diag1;

comuna_grouped = GROUP comuna BY glosa_comuna_residencia;

comuna_count = FOREACH comuna_grouped GENERATE COUNT($1) AS comuna_count, group AS comuna_pair;

comuna_causa_grouped = GROUP comuna BY (glosa_comuna_residencia, glosa_categoria_diag1);

comuna_causa_count = FOREACH comuna_causa_grouped GENERATE COUNT($1) AS count_causa, group AS comuna_causa_pair;

count_causa_comuna = FOREACH comuna_causa_count GENERATE $0, $1.glosa_comuna_residencia as comuna, $1.glosa_categoria_diag1 as causa;

comuna_causa_grouped = GROUP count_causa_comuna BY comuna;

max_comuna_causa = FOREACH comuna_causa_grouped {
    ord = ORDER count_causa_comuna BY count_causa DESC;
    top = LIMIT ord 1;
    GENERATE FLATTEN(top);
};

comuna_count_causa = JOIN comuna_count BY comuna_pair, max_comuna_causa BY comuna;

count_causa_por_comuna = FOREACH comuna_count_causa GENERATE $1, $0, ($4, $2), CONCAT((chararray)ROUND((float) $2/$0 * 100), '%');

comuna_final = ORDER count_causa_por_comuna BY $0 ASC;

STORE comuna_final INTO '/uhadoop2020/patosdepana/results/comunas/';