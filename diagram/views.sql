CREATE OR REPLACE VIEW "vw_compras" AS
SELECT
  s.uid,
  s.reference_date,
  s.reference_date::date AS data_compra,
  date_trunc('month', s.reference_date)::date AS mes_compra,
  s.chave_de_acesso,
  m.cnpj,
  m.nome AS mercado,
  m.uf,
  s.codigo,
  s.descricao,
  p.tipo_produto,
  p.marca,
  p.categoria,
  p.sub_categoria,
  s.quantidade,
  s.unidade,
  s.valor_unitario,
  (s.quantidade * s.valor_unitario) AS valor_total_item,
  p.ean
FROM shopping s
LEFT JOIN products p ON s.product_uid = p.uid
LEFT JOIN nfe_information n ON s.chave_de_acesso = n.chave_de_acesso
LEFT JOIN market m ON n.cnpj = m.cnpj;

CREATE OR REPLACE VIEW "vw_gasto_por_categoria" AS
SELECT
  categoria,
  tipo_produto,
  count(*) AS itens,
  sum(quantidade) AS qtd_total,
  sum(valor_total_item) AS gasto_total
FROM vw_compras
GROUP BY categoria, tipo_produto;

CREATE OR REPLACE VIEW "vw_preco_por_mercado" AS
SELECT
  coalesce(nullif(ean, ''), descricao) AS produto,
  max(descricao) AS descricao,
  max(ean) AS ean,
  mercado,
  count(*) AS compras,
  min(valor_unitario) AS preco_min,
  round(avg(valor_unitario), 2) AS preco_medio,
  max(valor_unitario) AS preco_max,
  max(reference_date) AS ultima_compra
FROM vw_compras
GROUP BY coalesce(nullif(ean, ''), descricao), mercado;

CREATE OR REPLACE VIEW "vw_recorrencia" AS
SELECT
  descricao,
  count(DISTINCT data_compra) AS vezes_comprado,
  min(reference_date) AS primeira_compra,
  max(reference_date) AS ultima_compra,
  sum(quantidade) AS qtd_total,
  sum(valor_total_item) AS gasto_total
FROM vw_compras
GROUP BY descricao;

CREATE OR REPLACE VIEW "vw_gasto_mensal" AS
SELECT
  mes_compra,
  mercado,
  count(DISTINCT chave_de_acesso) AS notas,
  sum(valor_total_item) AS gasto_total
FROM vw_compras
GROUP BY mes_compra, mercado;
