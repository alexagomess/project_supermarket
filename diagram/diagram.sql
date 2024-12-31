CREATE TABLE IF NOT EXISTS "market" (
  "nome" text,
  "cnpj" text PRIMARY KEY,
  "inscricao_estadual" text,
  "uf" text,
  "created_at" timestamp,
  "updated_at" timestamp
);

CREATE TABLE IF NOT EXISTS "nfe_information" (
  "nome" text,
  "cnpj" text,
  "inscricao_estadual" text,
  "uf" text,
  "destino_da_operacao" text,
  "consumidor_final" text,
  "presenca_do_comprador" text,
  "modelo" text,
  "serie" text,
  "numero" integer,
  "data_emissao" timestamp,
  "valor_total_do_servico" double precision,
  "base_de_calculo_icms" double precision,
  "valor_icms" double precision,
  "protocolo" text,
  "created_at" timestamp,
  "updated_at" timestamp,
  "chave_de_acesso" text PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS "products" (
  "uid" text PRIMARY KEY,
  "codigo" text,
  "descricao" text,
  "descricao_completa" text,
  "marca" text,
  "categoria" text,
  "sub_categoria" text,
  "tipo_produto" text,
  "ean" text,
  "created_at" timestamp,
  "updated_at" timestamp
);

CREATE TABLE IF NOT EXISTS "shopping" (
  "uid" text PRIMARY KEY,
  "index" integer,
  "descricao" text,
  "codigo" text,
  "quantidade" double precision,
  "unidade" text,
  "valor_unitario" double precision,
  "reference_date" timestamp,
  "created_at" timestamp,
  "updated_at" timestamp,
  "chave_de_acesso" text
);

ALTER TABLE "shopping" ADD FOREIGN KEY ("chave_de_acesso") REFERENCES "nfe_information" ("chave_de_acesso");
ALTER TABLE "products" ADD CONSTRAINT "products_codigo_unique" UNIQUE ("codigo");
ALTER TABLE "nfe_information" ADD FOREIGN KEY ("cnpj") REFERENCES "market" ("cnpj");
ALTER TABLE "shopping" ADD FOREIGN KEY ("codigo") REFERENCES "products" ("codigo");