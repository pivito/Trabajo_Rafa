import csv
import mysql.connector
import json

def read_csv_file(filename):
    data = []
    with open(filename, 'r', encoding="utf-8") as file:
        reader = csv.DictReader(file)  # Usa DictReader para convertir las filas en diccionarios
        for row in reader:
            data.append(dict(row))  # Convierte cada fila en un diccionario
    return data

        

def read_csv_file(filename):
    data =[]
    with open(filename, 'r') as file:
        reader= csv.reader(file)
        for element in reader:
            data.append(element)        
    return data

class Database:
    def __init__(self, host, user, password, database,port):
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            port=port,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()

    def create_table(self,stringCreate):
        self.cursor.execute(stringCreate)
        self.connection.commit()

    def insert_data(self, query,params):
        self.cursor.execute(query, params)
        self.connection.commit()
        
    def fetch_one(self, query, params):
        self.cursor.execute(query, params)
        return self.cursor.fetchone()
        
def cargar_datos_mysql(db):
    # Leer archivos JSON
    with open('MySQL/locations.json', 'r', encoding='utf-8') as file:
        locations = json.load(file)
    print(f"Leídos {len(locations)} registros de locations.json")

    with open('MySQL/skills.json', 'r', encoding='utf-8') as file:
        skills = json.load(file)
    print(f"Leídos {len(skills)} registros de skills.json")

    with open('MySQL/has_skill.json', 'r', encoding='utf-8') as file:
        has_skill = json.load(file)
    print(f"Leídos {len(has_skill)} registros de has_skill.json")

    with open('MySQL/pokemon.json', 'r', encoding='utf-8') as file:
        pokemon = json.load(file)
    print(f"Leídos {len(pokemon)} registros de pokemon.json")

    create_tables_queries = [
        """
    CREATE TABLE IF NOT EXISTS locations (
        id INT PRIMARY KEY,
        name VARCHAR(255),
        city VARCHAR(255)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS skills (
        id INT PRIMARY KEY,
        name VARCHAR(255)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS has_skill (
        person_id INT,
        skill_id INT,
        proficiency VARCHAR(255),
        PRIMARY KEY (person_id, skill_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pokemon (
        pokemon_id INT PRIMARY KEY,
        description VARCHAR(255),
        pokeGame VARCHAR(255)
    )
    """
    ]
    for query in create_tables_queries:
        try:
            db.create_table(query)
            print("Tabla creada con éxito")
        except mysql.connector.Error as err:
            print(f"Error al crear tabla: {err}")

    for location in locations:
        if not db.fetch_one("SELECT * FROM locations WHERE id = %s", (location["id"],)):
            db.insert_data("INSERT INTO locations (id, name, city) VALUES (%s, %s, %s)", (location["id"], location["name"], location["city"]))
        else:
            print(f"Registro duplicado en 'locations': {location}")

    for skill in skills:
        if not db.fetch_one("SELECT * FROM skills WHERE id = %s", (skill["id"],)):
            db.insert_data("INSERT INTO skills (id, name) VALUES (%s, %s)", (skill["id"], skill["name"]))
        else:
            print(f"Registro duplicado en 'skills': {skill}")

    for hs in has_skill:
        if not db.fetch_one("SELECT * FROM has_skill WHERE person_id = %s AND skill_id = %s", (hs["person_id"], hs["skill_id"])):
            db.insert_data("INSERT INTO has_skill (person_id, skill_id, proficiency) VALUES (%s, %s, %s)", (hs["person_id"], hs["skill_id"], hs["proficiency"]))
        else:
            print(f"Registro duplicado en 'has_skill': {hs}")

    for poke in pokemon:
        if not db.fetch_one("SELECT * FROM pokemon WHERE pokemon_id = %s", (poke["pokemon_id"],)):
            db.insert_data("INSERT INTO pokemon (pokemon_id, description, pokeGame) VALUES (%s, %s, %s)", (poke["pokemon_id"], poke["description"], poke["pokeGame"]))
        else:
            print(f"Registro duplicado en 'pokemon': {poke}")
    print("Datos insertados en MySQL")            
            
#readerlocations= read_csv_file("MySQL/locations.json")
#readerskills=read_csv_file("MySQL/skills.json")
#readerhas_skill=read_csv_file("MySQL/has_skill.json")
#readerpokemon=read_csv_file("MySQL/pokemon.json")

DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "migue"
DB_DATABASE = "Datos"
DB_PORT= "6969"

db = Database(DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE,DB_PORT)

#db.create_table(create_table_querylocations)
#db.create_table(create_table_queryskills)
#db.create_table(create_table_queryhas_skills)
#db.create_table(create_table_querypokemon)



'''        
for element in readerlocations[1:]:
    insert_query = "INSERT INTO locations (id,name,city) VALUES (%s, %s, %s)"
    data= (element[0],element[1],element[2])
    db.insert_data(insert_query,data)
    
for element in readerskills[1:]:
    insert_query = "INSERT INTO skills (id,name,person_id,skill_id,proficiency)(id, car_id,customer_id,rental_date) VALUES (%s, %s, %s, %s, %s)"
    data= (element[0],element[1],element[2],element[3],element[4])
    db.insert_data(insert_query,data)
    
for element in readerhas_skill[1:]:
    insert_query = "INSERT INTO has_skills (person_id,skill_id,proficiency) VALUES (%s, %s, %s)"
    data= (element[0],element[1],element[2])
    db.insert_data(insert_query,data)
    
for element in readerpokemon[1:]:
    insert_query = "INSERT INTO pokemon (pokemon_id,description,pokeGame) VALUES (%s, %s, %s)"
    data= (element[0],element[1],element[2])
    db.insert_data(insert_query,data)
'''