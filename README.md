# Sistema de Varejo com Data Warehouse

Este projeto implementa um sistema de varejo com integração entre MySQL, MongoDB, ZODB e um Data Warehouse para análise de dados.

## Pré-requisitos

- Docker
- Docker Compose
- Git

## Estrutura do Projeto

O projeto utiliza os seguintes componentes:
- MySQL: Banco de dados principal (VarejoBase) e Data Warehouse (DW_Varejo)
- MongoDB: Banco de dados NoSQL para dados complementares (Comentários de clientes e imagens de produtos)
- ZODB: Banco de dados orientado a objetos para Python (para gerenciar produtos e suas características)
- Python: API e scripts de ETL (Extract, Transform, Load)

## Arquitetura do Data Warehouse

O DW_Varejo utiliza uma combinação de esquemas Estrela (Star Schema) e Floco de Neve (Snowflake Schema) para otimizar consultas analíticas e manter a flexibilidade do modelo de dados.

### Esquema Estrela
- **Tabela Fato**: Vendas
  - Contém métricas como quantidade vendida, valor total, desconto
  - Conecta-se diretamente às dimensões principais

- **Dimensões Principais**:
  - Tempo (dim_tempo)
  - Produto (dim_produto)
  - Cliente (dim_cliente)
  - Loja (dim_loja)

### Esquema Floco de Neve
Algumas dimensões são normalizadas em múltiplos níveis para reduzir redundância:

- **Hierarquia de Produto**:
  - dim_produto → dim_categoria → dim_departamento
  - Permite análises em diferentes níveis de granularidade

- **Hierarquia de Localização**:
  - dim_loja → dim_cidade → dim_estado → dim_regiao
  - Facilita análises geográficas detalhadas

### Benefícios desta Arquitetura
1. **Esquema Estrela**:
   - Queries mais simples e rápidas
   - Facilita a criação de cubos OLAP
   - Melhor performance para agregações

2. **Esquema Floco de Neve**:
   - Economia de espaço em dimensões hierárquicas
   - Maior normalização dos dados
   - Flexibilidade para análises multinível

## Passo a Passo para Execução

### 1. Inicialização dos Containers

```bash
# Inicia todos os serviços em background
docker compose up -d

# Aguarde alguns (10) segundos para garantir que os bancos de dados estejam prontos
```

### 2. Verificação dos Bancos de Dados

Para verificar se os bancos foram criados corretamente:

```bash
# Verificar bancos MySQL
docker exec -i mysql_db mysql -u root -prootpassword -e "SHOW DATABASES;"

# Você deve ver tanto VarejoBase quanto DW_Varejo na lista
```

### 3. Configuração do Banco VarejoBase

```bash
# Aplicar a estrutura do banco VarejoBase
docker exec -i mysql_db mysql -u root -prootpassword VarejoBase < estrutura-varejo-base.txt

# Inserir dados iniciais
docker exec -i mysql_db mysql -u root -prootpassword VarejoBase < inserts-varejo-base.txt
```

### 4. Migração de Produtos para ZODB

```bash
# Executar script de migração
docker compose exec python python scripts/migrate_products.py
```

### 5. Povoando MongoDB com Dados Complementares

```bash
# Executar script de seed do MongoDB
docker compose exec python python scripts/seed_mongodb.py
```

Este script irá:
- Gerar comentários de clientes para cada produto
- Criar URLs de imagens para os produtos
- Estabelecer índices para otimização de consultas

Para verificar os dados no MongoDB:
```bash
# Acessar o shell do MongoDB
docker exec -it mongodb mongosh -u admin -p adminpassword

# Listar comentários de um produto específico
use varejo
db.product_comments.find({ "product_code": "ELET003" })

# Listar imagens de um produto específico
db.product_images.find({ "product_code": "ELET003" })
```

### 6. Configuração do Data Warehouse

```bash
# Criar estrutura do DW
docker exec -i mysql_db mysql -u root -prootpassword DW_Varejo < estrutura-dw-varejo.txt

# Executar processo ETL
docker compose exec python python scripts/etl_dw.py

# Executar análise OLAP
docker compose exec python python scripts/olap_analysis.py
```

## Manutenção e Troubleshooting

### Reiniciar do Zero

Se precisar reiniciar todo o ambiente:

```bash
# Para todos os containers e remove volumes
docker compose down -v

# Reinicia os serviços
docker compose up -d
```

### Logs

Para verificar logs dos serviços:

```bash
# Logs do MySQL
docker logs mysql_db

# Logs da aplicação Python
docker logs python_app

# Logs do MongoDB
docker logs mongodb
```

## Endpoints da API

- `GET /api/produtos`: Lista todos os produtos (dados do MySQL)
- `GET /api/produtos/{codigo}`: Retorna detalhes de um produto específico

### Testando a Integração com Bancos de Dados

1. **Integração com MySQL**:
   - Acesse http://localhost:3333/api/produtos
   - Este endpoint retorna a lista completa de produtos diretamente do banco MySQL (VarejoBase)
   - Os dados mostrados incluem informações como código, nome, preço e categoria dos produtos

2. **Integração com ZODB**:
   - Acesse http://localhost:3333/api/produtos/[CODIGO]
   - Exemplo: http://localhost:3333/api/produtos/ELET003
   - Este endpoint mostra os detalhes expandidos do produto, incluindo dados migrados para o ZODB

3. **Integração com MongoDB**:
   - Para ver comentários: http://localhost:3333/api/produtos/[CODIGO]/comentarios
   - Para ver imagens: http://localhost:3333/api/produtos/[CODIGO]/imagens
   - Exemplo: http://localhost:3333/api/produtos/ELET003/comentarios

## Arquivos de Configuração Importantes

- `estrutura-varejo-base.txt`: Contém a estrutura do banco VarejoBase
- `inserts-varejo-base.txt`: Contém os dados iniciais do VarejoBase
- `estrutura-dw-varejo.txt`: Contém a estrutura do Data Warehouse
- `scripts/migrate_products.py`: Script de migração para ZODB
- `scripts/etl_dw.py`: Script de ETL para o Data Warehouse
- `scripts/olap_analysis.py`: Script de análise OLAP

## Observações Importantes

1. Certifique-se de que as portas 3306 (MySQL), 27017 (MongoDB) e 3333 (API) estejam disponíveis em seu sistema.
2. Os scripts devem ser executados na ordem especificada acima.
3. Aguarde alguns segundos após iniciar os containers para garantir que os serviços estejam totalmente prontos.
