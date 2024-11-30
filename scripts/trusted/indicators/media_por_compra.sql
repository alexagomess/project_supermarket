WITH nfe AS (
	SELECT data_emissao, valor_total_do_servico, chave_de_acesso
	FROM supermarket.nfe_information
	WHERE true
)
, shopping AS (
	SELECT COUNT(uid) qtd_produtos, reference_date, chave_de_acesso
	FROM supermarket.shopping
	WHERE true 
	GROUP BY reference_date, chave_de_acesso
	ORDER BY reference_date
)
SELECT 
	s.reference_date, 
	s.qtd_produtos, 
	n.valor_total_do_servico, 
	n.valor_total_do_servico/s.qtd_produtos AS media_valor
FROM shopping s
LEFT JOIN nfe n ON n.chave_de_acesso = s.chave_de_acesso
