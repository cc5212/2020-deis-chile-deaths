-- Cantidad de muertos entre 18 y 40 años, agrupados por su estado civil
raw = LOAD 'hdfs://cm:9000/uhadoop2020/patosdepana/data-deis.tsv' USING PigStorage('\t') AS 
    (id_fallecido, anho_def, fecha_def, glosa_sexo, 
    edad_cant, glosa_edad_tipo, glosa_est_civil, glosa_nivel_ins,
    glosa_ocupacion, glosa_local_def, glosa_reg_res, glosa_comuna_residencia,
    glosa_categoria_diag1, glosa_capitulo_diag1);

anhos = FILTER raw BY (glosa_edad_tipo == 'Edad en años') AND (edad_cant >= 18) AND (edad_cant < 40) AND (anho_def >= 1997);

data = FOREACH anhos GENERATE glosa_est_civil, glosa_categoria_diag1;

estadocivil_grouped = GROUP data BY glosa_est_civil;

estadocivil_count = FOREACH estadocivil_grouped GENERATE COUNT($1) AS estadocivil_count, group AS estadocivil_pair;

estadocivil_causa_grouped = GROUP data BY (glosa_est_civil, glosa_categoria_diag1);

estadocivil_causa_count = FOREACH estadocivil_causa_grouped GENERATE COUNT($1) AS count_causa, group AS estadocivil_causa_pair;

count_causa_estadocivil = FOREACH estadocivil_causa_count GENERATE $0, $1.glosa_est_civil as estadocivil, $1.glosa_categoria_diag1 as causa;

estadocivil_causa_grouped = GROUP count_causa_estadocivil BY estadocivil;

max_estadocivil_causa = FOREACH estadocivil_causa_grouped {
    ord = ORDER count_causa_estadocivil BY count_causa DESC;
    top = LIMIT ord 1;
    GENERATE FLATTEN(top);
};

estadocivil_count_causa = JOIN estadocivil_count BY estadocivil_pair, max_estadocivil_causa BY estadocivil;

count_causa_por_estadocivil = FOREACH estadocivil_count_causa GENERATE $1, $0, ($4, $2), CONCAT((chararray)ROUND((float) $2/$0 * 100), '%');

estadocivil_final = ORDER count_causa_por_estadocivil BY $0 ASC;

STORE estadocivil_final INTO '/uhadoop2020/patosdepana/results/estadocivil/';