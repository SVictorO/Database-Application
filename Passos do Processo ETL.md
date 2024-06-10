# Processo ETL com Python e MySQL

Para realizar o processo de ETL (Extract, Transform, Load) com seus arquivos CSV no MySQL, você pode usar Python com a biblioteca `pandas` para extrair e transformar os dados, e depois carregar esses dados no MySQL.

## Passos do Processo ETL

1. **Extrair os dados dos arquivos CSV**
2. **Transformar os dados**
3. **Carregar os dados no MySQL**

## Pré-requisitos

- Python instalado
- Bibliotecas `pandas` e `mysql-connector-python` instaladas
- Um servidor MySQL em execução

## 1. Extrair os dados dos arquivos CSV

Crie um script Python para carregar os dados dos arquivos CSV usando `pandas`.

```python
import pandas as pd

# Extrair dados dos arquivos CSV
heroes_df = pd.read_csv('csv/heroes.csv')
hero_stats_df = pd.read_csv('csv/hero_stats.csv')
```

## 2. Transformar os dados
Transforme os dados conforme necessário. Neste caso, vamos simplificar as transformações.

```python
# Transformar os dados
# Exemplo: Renomear colunas para consistência
heroes_df.columns = ['Hero_ID', 'Hero_Name']
hero_stats_df.columns = [
    'Name', 'Primary_Attribute', 'Attack_Type', 'Attack_Range',
    'Roles', 'Total_Pro_Wins', 'Times_Picked', 'Times_Banned',
    'Win_Rate', 'Niche_Hero'
]

# Mapear herói pelo nome para obter o Hero_ID
hero_stats_df = hero_stats_df.merge(heroes_df, left_on='Name', right_on='Hero_Name')
```

## 3. Carregar os dados no MySQL
Configure a conexão MySQL e carregue os dados nas tabelas.
```python
import mysql.connector

# Conectar ao MySQL
conn = mysql.connector.connect(
    host='127.0.0.1',
    user='root',
    password='123',
    database='conexaodbmysql'
)
cursor = conn.cursor()

# Criar tabelas, se ainda não existirem
cursor.execute('''
CREATE TABLE IF NOT EXISTS Dim_Hero (
    Hero_ID INT PRIMARY KEY,
    Hero_Name VARCHAR(255)
);
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Dim_Attribute (
    Attribute_ID INT PRIMARY KEY AUTO_INCREMENT,
    Primary_Attribute VARCHAR(255)
);
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Dim_Attack_Type (
    Attack_Type_ID INT PRIMARY KEY AUTO_INCREMENT,
    Attack_Type VARCHAR(255)
);
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Fato_Hero_Performance (
    Performance_ID INT PRIMARY KEY AUTO_INCREMENT,
    Hero_ID INT,
    Attribute_ID INT,
    Attack_Type_ID INT,
    Attack_Range INT,
    Roles VARCHAR(255),
    Total_Pro_Wins INT,
    Times_Picked INT,
    Times_Banned INT,
    Win_Rate FLOAT,
    Niche_Hero BOOLEAN,
    FOREIGN KEY (Hero_ID) REFERENCES Dim_Hero(Hero_ID),
    FOREIGN KEY (Attribute_ID) REFERENCES Dim_Attribute(Attribute_ID),
    FOREIGN KEY (Attack_Type_ID) REFERENCES Dim_Attack_Type(Attack_Type_ID)
);
''')

# Inserir dados nas tabelas dimensionais
for _, row in heroes_df.iterrows():
    cursor.execute('''
    INSERT INTO Dim_Hero (Hero_ID, Hero_Name)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE Hero_Name = VALUES(Hero_Name);
    ''', (int(row['Hero_ID']), row['Hero_Name']))

# Inserir dados na tabela de atributos
for attribute in hero_stats_df['Primary_Attribute'].unique():
    cursor.execute('''
    INSERT INTO Dim_Attribute (Primary_Attribute)
    VALUES (%s)
    ON DUPLICATE KEY UPDATE Primary_Attribute = VALUES(Primary_Attribute);
    ''', (attribute,))

# Inserir dados na tabela de tipos de ataque
for attack_type in hero_stats_df['Attack_Type'].unique():
    cursor.execute('''
    INSERT INTO Dim_Attack_Type (Attack_Type)
    VALUES (%s)
    ON DUPLICATE KEY UPDATE Attack_Type = VALUES(Attack_Type);
    ''', (attack_type,))

# Inserir dados na tabela fato
for _, row in hero_stats_df.iterrows():
    # Obter IDs das tabelas dimensionais
    cursor.execute('SELECT Attribute_ID FROM Dim_Attribute WHERE Primary_Attribute = %s', (row['Primary_Attribute'],))
    attribute_id = cursor.fetchone()[0]

    cursor.execute('SELECT Attack_Type_ID FROM Dim_Attack_Type WHERE Attack_Type = %s', (row['Attack_Type'],))
    attack_type_id = cursor.fetchone()[0]

    cursor.execute('''
    INSERT INTO Fato_Hero_Performance (
        Hero_ID, Attribute_ID, Attack_Type_ID, Attack_Range,
        Roles, Total_Pro_Wins, Times_Picked, Times_Banned,
        Win_Rate, Niche_Hero
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    ''', (
        int(row['Hero_ID']), attribute_id, attack_type_id, int(row['Attack_Range']),
        row['Roles'], int(row['Total_Pro_Wins']), int(row['Times_Picked']),
        int(row['Times_Banned']), float(row['Win_Rate']), row['Niche_Hero']
    ))

# Commit e fechar a conexão
conn.commit()
cursor.close()
conn.close()

```   
