
raw = LOAD 'hdfs://cm:9000/uhadoop2020/patosdepana/data-deis.tsv' USING PigStorage('\t') AS 
    (id_fallecido, anho_def, fecha_def, glosa_sexo, 
    edad_cant, glosa_edad_tipo, glosa_est_civil, glosa_nivel_ins,
    glosa_ocupacion, glosa_local_def, glosa_reg_res, glosa_comuna_residencia,
    glosa_categoria_diag1, glosa_capitulo_diag1);


bebes = FILTER raw BY glosa_edad_tipo != 'Edad en años';

edad = FOREACH bebes GENERATE anho_def, edad_cant, glosa_categoria_diag1;

bebes_grouped = GROUP edad BY anho_def;

bebes_count = FOREACH bebes_grouped GENERATE COUNT($1) AS anho_count, group AS edad_pair;

anho_causa_grouped = GROUP edad BY (anho_def, glosa_categoria_diag1);

anho_causa_count = FOREACH anho_causa_grouped GENERATE COUNT($1) AS count_causa, group AS anho_causa_pair;

count_causa_anho = FOREACH anho_causa_count GENERATE $0, $1.anho_def as anho, $1.glosa_categoria_diag1 as causa;

anho_grouped = GROUP count_causa_anho BY anho;

max_anho_causa = FOREACH anho_grouped {
    ord = ORDER count_causa_anho BY count_causa DESC;
    top = LIMIT ord 1;
    GENERATE FLATTEN(top);
};

bebes_count_causa = JOIN bebes_count BY edad_pair, max_anho_causa BY anho;

bebes_count_causa_por_anho = FOREACH bebes_count_causa GENERATE $1, $0, ($4, $2), CONCAT((chararray)ROUND((float) $2/$0 * 100), '%');

STORE bebes_count INTO '/uhadoop2020/patosdepana/results/bebes/';