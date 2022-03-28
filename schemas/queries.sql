select * from tmp_categoria;
select * from tmp_assunto;
select * from assunto;
select * from categoria;
select * from saneantes;

DROP TABLE tmp_categoria;
DROP TABLE tmp_assunto;
DROP TABLE assunto;
DROP TABLE categoria;
DROP TABLE saneantes;


INSERT INTO assunto
SELECT tmp_assunto.*
FROM   tmp_assunto
LEFT   JOIN assunto USING (assunto_id)
WHERE  assunto.assunto_id IS NULL;

INSERT INTO categoria
SELECT tmp_categoria.*
FROM   tmp_categoria
LEFT   JOIN categoria USING (categoria_id)
WHERE  categoria.categoria_id IS NULL;

