from neo4j import GraphDatabase
import csv
def read_csv_file(filename):
    data =[]
    with open(filename, 'r',encoding="utf-8") as file:
        reader= csv.reader(file)
        for element in reader:
            data.append(element)        
    return data

class Neo4jCRUD:
    def __init__(self, uri, user, password):
        self._uri = uri
        self._user = user
        self._password = password
        self._driver = None
        self._connect()

    def _connect(self):
        self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))

    def close(self):
        if self._driver is not None:
            self._driver.close()

    def create_node(self, label, properties):
        with self._driver.session() as session:
            result = session.execute_write(self._create_node, label, properties)
            return result

    @staticmethod
    def _create_node(tx, label, properties):
        query = (
            f"CREATE (n:{label} $props) "
            "RETURN n"
        )
        result = tx.run(query, props=properties)
        

    def consulta1(self, empresa):
        with self._driver.session() as session:
            return session.execute_write(self._consulta1, empresa)
          
    @staticmethod
    def _consulta1(tx, empresa):
        query = (
            f"""
            MATCH (p:persons)-[r:TRABAJA]->(c:empresas) WHERE c.name = '{empresa}' RETURN p.name AS Persona, r.rol AS Rol, c.name AS Empresa
            """
        )
        result= tx.run(query)
        return [record for record in result]
    
    def consulta2(self, rol):
        with self._driver.session() as session:
            return session.execute_write(self._consulta2, rol)
          
    @staticmethod
    def _consulta2(tx, rol):
        query = (
            f"""
            MATCH (p:persons)-[r:TRABAJA]->(c:empresas)
            WHERE r.rol = '{rol}'
            WITH p, r.rol AS Rol, COUNT(DISTINCT c) AS NumEmpresas
            WHERE NumEmpresas > 1
            MATCH (p)-[r2:TRABAJA]->(c2:empresas)
            RETURN p.name AS Persona, Rol, c2.name AS Empresa
            ORDER BY Persona, Rol, Empresa;
            """
        )
        result= tx.run(query)
        return [record for record in result]
    
    def consulta3(self):
        with self._driver.session() as session:
            return session.execute_write(self._consulta3)
          
    @staticmethod
    def _consulta3(tx):
        query = (
            f"""
            MATCH (p1:persons)-[:TRABAJA]->(c:empresas)<-[:TRABAJA]-(p2:persons)
            WHERE p1 <> p2
            RETURN p1.name AS Persona1, p2.name AS Persona2, c.name AS Empresa_Comun
            ORDER BY Persona1, Persona2, Empresa_Comun;
            """
        )
        result= tx.run(query)
        return [record for record in result]
    
    def create_relationship(self,labelOrigin,propertyOrigin,labelEnd,propertyEnd,relationshipName,rol):
         with self._driver.session() as session:
            result = session.execute_write(self._create_relationship, labelOrigin,propertyOrigin,labelEnd,propertyEnd,relationshipName,rol)
            return result
        
    @staticmethod
    def _create_relationship(tx, labelOrigin,propertyOrigin,labelEnd,propertyEnd,relationshipName,rol):
        query = (
            f"MATCH (n:{labelOrigin}),(c:{labelEnd}) "
            f"WHERE n.id='{propertyOrigin}' and c.id='{propertyEnd}' " 
            f"CREATE (n)-[:{relationshipName} {{rol:'{rol}'}} ]->(c)"
        )
        result = tx.run(query)
        return result
    
uri = "bolt://localhost:7687"  
user = "neo4j"
password = "my-secret-pw"
neo4j_crud = Neo4jCRUD(uri, user, password)


readerempresas= read_csv_file("Neo4j/empresas.csv")
readerpersons=read_csv_file("Neo4j/persons.csv")
readerworks_at=read_csv_file("Neo4j/works_at.csv")

for element in readerempresas[1:]:
    node_properties = {
        "id": element[0], 
        "name": element[1],
        "sector":element[2],
                }
    neo4j_crud.create_node("empresas", node_properties)

for element in readerpersons[1:]:
    node_properties = {
        "id": element[0], 
        "name": element[1],
        "age":element[2],
         }
    neo4j_crud.create_node("persons", node_properties)

for element in readerworks_at[1:]:
    neo4j_crud.create_relationship("persons",element[0], "empresas",element[2],"TRABAJA", element[1])
    