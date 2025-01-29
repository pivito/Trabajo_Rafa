from pymongo import MongoClient
import csv

def read_csv_file(filename):
    data = []
    with open(filename, 'r', encoding="utf-8") as file:
        reader = csv.DictReader(file)  # Usa DictReader para convertir las filas en diccionarios
        for row in reader:
            data.append(dict(row))  # Convierte cada fila en un diccionario
    return data
    
class MongoDBOperations:
    def __init__(self, database_name, port,username=None, password=None):
        if username and password:
            self.client = MongoClient(f'mongodb://{username}:{password}@localhost:{port}/')
        else:
            self.client = MongoClient(f'mongodb://localhost:{port}/')
        self.db = self.client[database_name]
        
    def create_person(self, collection_name,data):
        self.collection = self.db[collection_name]
        result = self.collection.insert_one(data)
        return result
    
projects=read_csv_file("Mongo/projects.csv")
teams=read_csv_file("Mongo/teams.csv")
works_in_team=read_csv_file("Mongo/works_in_team.csv")
favourite_pokemon=read_csv_file("Mongo/favourite_pokemon.csv")

mongo_operations = MongoDBOperations("Acomodations", "7777", "mongoadmin", "secret")

# Insertar los datos en MongoDB
def insert_data(collection_name, data_list):
    if isinstance(data_list, list):  # Asegurarse de que sea una lista de diccionarios
        for data in data_list:
            if isinstance(data, dict):  # Comprobar que cada elemento sea un diccionario
                mongo_operations.create_person(collection_name, data)
            else:
                print(f"Elemento inválido en {collection_name}: {data}")
    else:
        print(f"El archivo para {collection_name} no contiene una lista válida.")

insert_data("projects", projects)
insert_data("teams", teams)
insert_data("works_in_team", works_in_team)
insert_data("favourite_pokemon", favourite_pokemon)


#def consulta4(self, equipo):
#        with self._driver.session() as session:
#            return session.execute_write(self._consulta4, equipo)
          
#@staticmethod
#def _consulta4(tx, equipo):
#    query = (
#             f"""
#                SELECT 
#                    wt.person_id AS PersonaID,
#                    wt.role AS Funcion,
#                    t.team_name AS Equipo
#                FROM 
#                    works_in_team wt
#                JOIN 
#                    teams t ON wt.team_id = t.team_id
#                WHERE 
#                    t.team_name = %s;
#                """
#    )
#    result= tx.run(query)
#    return [record for record in result]