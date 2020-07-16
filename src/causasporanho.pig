-- Top 3 de causas de muerte por año
raw = LOAD 'hdfs://cm:9000/uhadoop2020/patosdepana/data-deis.tsv' USING PigStorage('\t') AS 
    (id_fallecido, anho_def, fecha_def, glosa_sexo, 
    edad_cant, glosa_edad_tipo, glosa_est_civil, glosa_nivel_ins,
    glosa_ocupacion, glosa_local_def, glosa_reg_res, glosa_comuna_residencia,
    glosa_categoria_diag1, glosa_capitulo_diag1);

anho_causa = FOREACH raw GENERATE anho_def, glosa_categoria_diag1;

anho_causa_grouped = GROUP anho_causa BY (anho_def, glosa_categoria_diag1);

anho_causa_count = FOREACH anho_causa_grouped GENERATE COUNT($1) AS count_causa, group AS anho_causa_pair; 

count_causa_anho = FOREACH anho_causa_count GENERATE $0, $1.anho_def as anho, $1.glosa_categoria_diag1 as causa; -- (count_causa, anho , causa)

anho_grouped = GROUP count_causa_anho BY anho; -- (año, {(count_causa, anho , causa)})

max_anho_causa = FOREACH anho_grouped {
    ord = ORDER count_causa_anho BY count_causa DESC;
    top = LIMIT ord 3;
    GENERATE FLATTEN(top);
};

STORE max_anho_causa INTO '/uhadoop2020/patosdepana/results/causasporanho/';
