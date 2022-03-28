BEGIN;

CREATE TABLE IF NOT EXISTS public.saneantes
(
    "RESOLUCAO" text NOT NULL,
    "EMPRESA" text NOT NULL,
    "AUTORIZACAO" character varying(12) NOT NULL,
    "NOME_PRODUTO_MARCA" character varying(200) NOT NULL,
    "NUMERO_PROCESSO" character varying(25),
    "NUMERO_REGISTRO" character varying(20),
    "VENDA_EMPREGO" text,
    "VENCIMENTO" date,
    "APRESENTACAO" text,
    "VALIDADE_PRODUTO" character varying(15),
    "CATEGORIA_ID" character varying(10),
    "ASSUNTO_ID" character varying(10),
    "EXPEDIENTE_PETICAO" character varying(18),
    "VERSAO" character varying(50),
    "INDEX" serial NOT NULL,
    PRIMARY KEY ("INDEX")
);

CREATE TABLE IF NOT EXISTS public.categoria
(
    assunto_id character varying(10) NOT NULL,
    descricao text NOT NULL,
    PRIMARY KEY (assunto_id)
);

CREATE TABLE IF NOT EXISTS public.assunto
(
    descricao_id character varying(10) NOT NULL,
    descricao text NOT NULL,
    PRIMARY KEY (descricao_id)
);

ALTER TABLE public.saneantes
    ADD FOREIGN KEY ("CATEGORIA_ID")
    REFERENCES public.categoria (descricao_id)
    NOT VALID;


ALTER TABLE public.saneantes
    ADD FOREIGN KEY ("ASSUNTO_ID")
    REFERENCES public.assunto (assunto_id)
    NOT VALID;

END;