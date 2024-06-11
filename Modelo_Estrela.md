# Modelo Estrela para Análise de Pedidos

## Tabela de Fatos

### `tb_pedido`
- **Colunas**:
  - `codigo_pedido` (chave primária)
  - `codigo_mesa` (chave estrangeira referenciando `tb_mesa`)
  - `codigo_prato` (chave estrangeira referenciando `tb_prato`)
  - `quantidade_pedido`
  - `valor_unitario_prato`
  - `valor_total_pedido`
  - `data_pedido`

## Tabelas de Dimensão

### `tb_cliente`
- **Colunas**:
  - `id_cliente` (chave primária)
  - `cpf_cliente`
  - `nome_cliente`
  - `email_cliente`
  - `telefone_cliente`

### `tb_mesa`
- **Colunas**:
  - `codigo_mesa` (chave primária)
  - `id_cliente` (chave estrangeira referenciando `tb_cliente`)
  - `num_pessoa_mesa`
  - `data_hora_entrada`
  - `data_hora_saida`

### `tb_prato`
- **Colunas**:
  - `codigo_prato` (chave primária)
  - `codigo_tipo_prato` (chave estrangeira referenciando `tb_tipo_prato`)
  - `nome_prato`
  - `preco_unitario_prato`

### `tb_tipo_prato`
- **Colunas**:
  - `codigo_tipo_prato` (chave primária)
  - `nome_tipo_prato`

### `tb_situacao_pedido`
- **Colunas**:
  - `codigo_situacao_pedido` (chave primária)
  - `nome_situacao_pedido`

### `tb_data`
- **Colunas**:
  - `data_pedido` (chave primária)
  - `ano`
  - `mes`
  - `dia`

## Modelo Estrela

```plaintext
                              +------------------+
                              |   tb_cliente     |
                              +------------------+
                                     |
                                     v
                              +------------------+
                              |    tb_mesa       |
                              +------------------+
                                     |
                                     v
+------------------+        +------------------+        +----------------------+
|  tb_tipo_prato   |<------>|     tb_prato     |<------>|      tb_pedido       |
+------------------+        +------------------+        +----------------------+
                                     |                   |     ^     ^     ^
                                     |                   |     |     |     |
                              +------------------+       |     |     |     |
                              |  tb_situacao_pedido|     |     |     |     |
                              +------------------+       |     |     |     |
                                                         |     |     |     |
                                                         |     |     |     |
                                                         |     |     |     |
                                                         |     |     |     |
                                                  +------------------+
                                                  |    tb_data       |
                                                  +------------------+
```
A tabela principal, chamada de tabela de fatos (tb_pedido), armazena as informações mais importantes sobre os pedidos, como quantidade, valor e datas. Já as outras tabelas, chamadas de tabelas de dimensão (tb_cliente, tb_mesa, tb_prato, tb_tipo_prato, tb_situacao_pedido, tb_data), fornecem informações adicionais sobre clientes, mesas, tipos de pratos, entre outros, que ajudam a entender melhor os pedidos.
