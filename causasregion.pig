raw = LOAD 'hdfs://cm:9000/uhadoop2020/patosdepana/data-deis.tsv' USING PigStorage('\t') AS 
    (id_fallecido, anho_def, fecha_def, glosa_sexo, 
    edad_cant, glosa_edad_tipo, glosa_est_civil, glosa_nivel_ins,
    glosa_ocupacion, glosa_local_def, glosa_reg_res, glosa_comuna_residencia,
    glosa_categoria_diag1, glosa_capitulo_diag1);

data = FILTER raw BY anho_def >= 1997;

region_causa = FOREACH data GENERATE glosa_reg_res, glosa_categoria_diag1;

region_causa_grouped = GROUP region_causa BY (glosa_reg_res, glosa_categoria_diag1);

region_causa_count = FOREACH region_causa_grouped GENERATE COUNT($1) AS count_causa, group AS region_causa_pair; 

count_causa_region = FOREACH region_causa_count GENERATE $1.glosa_reg_res as region, $0, $1.glosa_categoria_diag1 as causa; -- (count_causa, region , causa)

region_grouped = GROUP count_causa_region BY region; 

max_region_causa = FOREACH region_grouped {
    ord = ORDER count_causa_region BY count_causa DESC;
    top = LIMIT ord 3;
    GENERATE FLATTEN(top);
};

STORE max_region_causa INTO '/uhadoop2020/patosdepana/results/causasregion';