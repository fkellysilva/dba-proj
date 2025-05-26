USE DW_Varejo;

-- Desabilitar verificação de chaves estrangeiras temporariamente
SET FOREIGN_KEY_CHECKS = 0;

-- Dropar todas as tabelas
DROP TABLE IF EXISTS fato_vendas;
DROP TABLE IF EXISTS fato_precos;
DROP TABLE IF EXISTS fato_estoque;
DROP TABLE IF EXISTS dim_tempo;
DROP TABLE IF EXISTS dim_produto;
DROP TABLE IF EXISTS dim_categoria;
DROP TABLE IF EXISTS dim_loja;
DROP TABLE IF EXISTS dim_cliente;

-- Reabilitar verificação de chaves estrangeiras
SET FOREIGN_KEY_CHECKS = 1; 