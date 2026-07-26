CREATE TABLE IF NOT EXISTS "market" (
  "nome" text,
  "cnpj" text PRIMARY KEY CHECK (char_length("cnpj") = 14),
  "inscricao_estadual" text,
  "uf" text CHECK ("uf" IS NULL OR char_length("uf") = 2),
  "created_at" timestamptz DEFAULT now(),
  "updated_at" timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS "nfe_information" (
  "cnpj" text,
  "destino_da_operacao" text,
  "consumidor_final" text,
  "presenca_do_comprador" text,
  "modelo" text,
  "serie" text,
  "numero" integer,
  "data_emissao" timestamptz,
  "valor_total" numeric(12, 2),
  "base_de_calculo_icms" numeric(12, 2),
  "valor_icms" numeric(12, 2),
  "protocolo" text,
  "chave_de_acesso" text PRIMARY KEY CHECK (char_length("chave_de_acesso") = 44),
  "created_at" timestamptz DEFAULT now(),
  "updated_at" timestamptz DEFAULT now()
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
  "created_at" timestamptz DEFAULT now(),
  "updated_at" timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS "shopping" (
  "uid" text PRIMARY KEY,
  "item_index" integer,
  "product_uid" text,
  "descricao" text,
  "codigo" text,
  "quantidade" numeric(12, 3),
  "unidade" text,
  "valor_unitario" numeric(12, 2),
  "reference_date" timestamptz,
  "chave_de_acesso" text CHECK (char_length("chave_de_acesso") = 44),
  "created_at" timestamptz DEFAULT now(),
  "updated_at" timestamptz DEFAULT now()
);

ALTER TABLE "nfe_information"
  ADD FOREIGN KEY ("cnpj") REFERENCES "market" ("cnpj");

ALTER TABLE "shopping"
  ADD FOREIGN KEY ("chave_de_acesso") REFERENCES "nfe_information" ("chave_de_acesso");

ALTER TABLE "shopping"
  ADD FOREIGN KEY ("product_uid") REFERENCES "products" ("uid");

CREATE INDEX IF NOT EXISTS "idx_nfe_cnpj" ON "nfe_information" ("cnpj");
CREATE INDEX IF NOT EXISTS "idx_nfe_data_emissao" ON "nfe_information" ("data_emissao");
CREATE INDEX IF NOT EXISTS "idx_shopping_chave" ON "shopping" ("chave_de_acesso");
CREATE INDEX IF NOT EXISTS "idx_shopping_codigo" ON "shopping" ("codigo");
CREATE INDEX IF NOT EXISTS "idx_shopping_product_uid" ON "shopping" ("product_uid");
CREATE INDEX IF NOT EXISTS "idx_shopping_reference_date" ON "shopping" ("reference_date");
