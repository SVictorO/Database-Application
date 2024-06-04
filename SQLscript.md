## Consultas SQL para Pedidos e Gastos
### Cliente que Mais Fez Pedidos por Ano

SELECT ano, cliente, num_pedidos
FROM (
  SELECT 
    YEAR(ms.data_hora_entrada) AS ano, 
    cl.nome_cliente AS cliente, 
    COUNT(*) AS num_pedidos,
    ROW_NUMBER() OVER (PARTITION BY YEAR(ms.data_hora_entrada) ORDER BY COUNT(*) DESC) AS rn
  FROM tb_pedido pd
  JOIN tb_mesa ms ON pd.codigo_mesa = ms.codigo_mesa
  JOIN tb_cliente cl ON ms.id_cliente = cl.id_cliente
  GROUP BY ano, clientes
) AS ranked
WHERE rn = 1;

 

### Cliente que Mais Gastou em Todos os Anos

SELECT cliente, total_gasto
FROM (
  SELECT 
    cl.nome_cliente AS cliente, 
    SUM(pd.quantidade_pedido * p.preco_unitario_prato) AS total_gasto,
    ROW_NUMBER() OVER (ORDER BY SUM(pd.quantidade_pedido * p.preco_unitario_prato) DESC) AS rn
  FROM tb_pedido pd
  JOIN tb_prato p ON pd.codigo_prato = p.codigo_prato
  JOIN tb_mesa ms ON pd.codigo_mesa = ms.codigo_mesa
  JOIN tb_cliente cl ON ms.id_cliente = cl.id_cliente
  GROUP BY cliente
) AS ranked
WHERE rn = 1;