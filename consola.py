from mainNeo4j import Neo4jCRUD
from mainMongo import MongoDBOperations
from mainMySQL import Database, cargar_datos_mysql


uri = "bolt://localhost:7687"  
user = "neo4j"
password = "my-secret-pw"
neo4j = Neo4jCRUD(uri, user, password)

mongo_operations = MongoDBOperations("Acomodations", "7777", "mongoadmin", "secret")

DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "migue"
DB_DATABASE = "Datos"
DB_PORT= "6969"
db = Database(DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE, DB_PORT)
#db = Database('localhost', 'root', 'migue', 'Datos', '6969')

cargar_datos_mysql(db)

while True:
    print("\nMenu:")
    print("1. Personas y sus roles en una empresa concreta.")
    print("2. Personas con el mismo rol en diferentes empresas.")
    print("3. Empresas comunes entre dos personas.")
    print("4. Personas y sus funciones en un equipo específico.")
    print("5. Muestra todos los equipos con el número de personas que los componen.")
    print("6. Muestra los equipos con el número total de proyectos a los que están asociados.")
    print("7. Obtener todas las skills en las que una persona tiene al menos un nivel específico de proficiency.")
    print("8. Encontrar todas las personas que tienen skill en al menos una skill en común con otra persona.")
    print("9. Encontrar el proyecto que tenga más personas en el equipo cuyos pokemons favoritos son de diferente tipo, mostrar todos los tipos distintos.")
    print("10. Dado una ubicación, obtén la lista de equipos que están ubicados allí junto con información de las personas que trabajan en ese equipo y los proyectos asociados.")
    print("11. Exit")
    choice = input("Enter your choice (1/2/3/4/5/6/7/8/9/10/11): ")
    
    if choice == '1':
        empresa = input("Introduce el nombre de la empresa: ")
        consulta = neo4j.consulta1(empresa)
        for record in consulta:
            print(f"{record[0]} trabaja en {empresa} como {record[1]}")
               
    elif choice == '2':
        rol = input("Introduce el rol: ")
        consulta = neo4j.consulta2(rol)
        for record in consulta:
            print(f"{record[0]} trabaja en {record[1]} en la empresa {record[2]}")
            #print(record)
            
    elif choice == '3':
        consulta = neo4j.consulta3()
        for record in consulta:
            print(f"{record[0]} trabaja en {record[1]} en la empresa {record[2]}")
            #print(record)
            
    elif choice == '4':
        
        equipo_nombre = input("Introduce el nombre del equipo: ")

        # Consulta en MongoDB
        pipeline = [
        {
            "$lookup": {
                "from": "teams", 
                "localField": "team_id",
                "foreignField": "team_id",
                "as": "team_info"
            }
        },
        {
            "$unwind": "$team_info"
        },
        {
            "$match": {
                "team_info.name": equipo_nombre
            }
        },
        {
            "$project": {
                "_id": 0,
                "PersonaID": "$person_id",
                "Rol": "$rol",
                "Equipo": "$team_info.name"
            }
        }
        ]

        resultados_mongo = mongo_operations.db['works_in_team'].aggregate(pipeline)
        personas_mongo = list(resultados_mongo)

        if not personas_mongo:
            print(f"No se encontraron datos para el equipo '{equipo_nombre}' en MongoDB.")
            continue

        # Obtener detalles de las personas desde Neo4j
        personas_ids = [str(persona['PersonaID']) for persona in personas_mongo]
        ids_para_consulta = ",".join(f"'{id}'" for id in personas_ids)

        # Consulta en Neo4j
        query_neo4j = f"""
            MATCH (p:persons)
            WHERE p.id IN [{ids_para_consulta}]
            RETURN p.id AS PersonaID, p.name AS Nombre, p.age AS Edad
        """
        resultados_neo4j = neo4j._driver.session().run(query_neo4j)
        personas_neo4j = {record['PersonaID']: record for record in resultados_neo4j}

        # Datos combinados
        print("\nPersonas y sus funciones en el equipo:")
        for persona in personas_mongo:
            persona_id = str(persona['PersonaID'])
            datos_persona = personas_neo4j.get(persona_id, {})
            print(f"PersonaID: {persona_id}, Nombre: {datos_persona.get('Nombre', 'Desconocido')}, "
                f"Edad: {datos_persona.get('Edad', 'Desconocida')}, Rol: {persona['Rol']}, Equipo: {persona['Equipo']}")
            
    
    elif choice == '5':
        
    # Obtener todos los equipos desde MongoDB
        pipeline_mongo = [
            {
                "$lookup": {  # Unir equipos con works_in_team
                    "from": "works_in_team",
                    "localField": "team_id",
                    "foreignField": "team_id",
                    "as": "personas"
                }
            },
            {
                "$project": {  # Proyectar los campos necesarios
                    "_id": 0,
                    "EquipoID": "$team_id",
                    "NombreEquipo": "$name",
                    "PersonasIDs": {
                        "$map": {  # Extraemos solo los IDs de las personas
                            "input": "$personas",
                            "as": "persona",
                            "in": "$$persona.person_id"
                        }
                    }
                }
            }
        ]

        equipos_mongo = list(mongo_operations.db['teams'].aggregate(pipeline_mongo))

        if not equipos_mongo:
            print("No se encontraron equipos en MongoDB.")
            continue

        # Obtener información de las personas desde Neo4j
        print("\nEquipos y número de personas que los componen:")
        for equipo in equipos_mongo:
            personas_ids = equipo['PersonasIDs']
            if not personas_ids:
                print(f"Equipo: {equipo['NombreEquipo']}, Número de Personas: 0")
                continue

            # Convertir los IDs en un formato adecuado para Neo4j
            ids_para_consulta = ",".join(f"'{id}'" for id in personas_ids)

            query_neo4j = f"""
                MATCH (p:persons)
                WHERE p.id IN [{ids_para_consulta}]
                RETURN COUNT(p) AS NumPersonas
            """
            resultado_neo4j = neo4j._driver.session().run(query_neo4j)
            num_personas = resultado_neo4j.single().get("NumPersonas", 0)

            # Combinar resultados
            print(f"Equipo: {equipo['NombreEquipo']}, Número de Personas: {num_personas}")


    elif choice == '6':
        
        pipeline = [
            {
                "$lookup": {  
                    "from": "projects",  
                    "localField": "project_id",  
                    "foreignField": "project_id",  
                    "as": "proyectos_info"
                }
            },
            {
                "$project": {  
                    "_id": 0,
                    "EquipoID": "$team_id",
                    "NombreEquipo": "$name",
                    "NumProyectos": {"$size": "$proyectos_info"}  
                }
            },
            {
                "$sort": {  
                    "NumProyectos": -1
                }
            }
        ]

        resultados = mongo_operations.db['teams'].aggregate(pipeline)

        print("\nEquipos y número total de proyectos asociados:")
        for resultado in resultados:
            print(f"Equipo: {resultado['NombreEquipo']}, Número de Proyectos: {resultado['NumProyectos']}")


    elif choice == '7':

        proficiency_level = input("Introduce el nivel de proficiency (e.g., Beginner, Intermediate, Expert): ")

        query = """
            SELECT 
                hs.person_id AS PersonaID,
                s.name AS Skill
            FROM 
                has_skill hs
            JOIN 
                skills s ON hs.skill_id = s.id
            WHERE 
                hs.proficiency = %s;
        """

        cursor = db.connection.cursor()
        cursor.execute(query, (proficiency_level,))
        resultados = cursor.fetchall()

        if resultados:
            print("\nPersonas con habilidades en el nivel de proficiency solicitado:")
            for resultado in resultados:
                print(f"PersonaID: {resultado[0]}, Skill: {resultado[1]}")
        else:
            print(f"No se encontraron personas con habilidades en el nivel de proficiency '{proficiency_level}'.")

        
    elif choice == '8':
        
        query = """
    SELECT 
        DISTINCT p1.person_id AS Persona1,
        p2.person_id AS Persona2,
        s.name AS Skill
    FROM 
        has_skill p1
    JOIN 
        has_skill p2 ON p1.skill_id = p2.skill_id AND p1.person_id != p2.person_id
    JOIN 
        skills s ON p1.skill_id = s.id
    ORDER BY 
        p1.person_id, p2.person_id;
    """

        cursor = db.connection.cursor()
        cursor.execute(query)
        resultados = cursor.fetchall()

        if resultados:
            print("\nPersonas con habilidades en común:")
            for resultado in resultados:
                print(f"Persona1: {resultado[0]}, Persona2: {resultado[1]}, Skill: {resultado[2]}")
        else:
            print("No se encontraron personas con habilidades en común.")

    elif choice == '9':
        
        from pokeapi import fetch_pokemon_data      
          
        
        pipeline_mongo = [
            {
                "$lookup": {  # Unimos teams con projects
                    "from": "teams",
                    "localField": "project_id",
                    "foreignField": "project_id",
                    "as": "teams_info"
                }
            },
            {
                "$unwind": "$teams_info"  
            },
            {
                "$lookup": {  # Unimos works_in_team con teams
                    "from": "works_in_team",
                    "localField": "teams_info.team_id",
                    "foreignField": "team_id",
                    "as": "personas_info"
                }
            },
            {
                "$unwind": "$personas_info"  
            },
            {
                "$lookup": {  # Unimos favourite_pokemon con personas
                    "from": "favourite_pokemon",
                    "localField": "personas_info.person_id",
                    "foreignField": "person_id",
                    "as": "pokemon_info"
                }
            },
            {
                "$unwind": "$pokemon_info" 
            },
            {
                "$project": {  # Seleccionamos los campos que queremos
                    "_id": 0,
                    "project_id": 1,
                    "project_name": "$name",
                    "person_id": "$personas_info.person_id",
                    "pokemon_name": "$pokemon_info.pokemon_id"  
                }
            }
        ]

        
        resultados_mongo = list(mongo_operations.db['projects'].aggregate(pipeline_mongo))

        # Procesamos los datos y consultamos la API
        proyectos_tipos = {}

        for registro in resultados_mongo:
            project_id = registro["project_id"]
            project_name = registro["project_name"]
            pokemon_name = registro["pokemon_name"] 

            # Consultamos la API para obtener los tipos del Pokémon
            pokemon_data = fetch_pokemon_data(pokemon_name)

            if isinstance(pokemon_data, dict) and "Types" in pokemon_data:
                tipos = set(pokemon_data["Types"])  # Convertir los tipos en un conjunto
            else:
                print(f"Error al obtener datos del Pokémon: {pokemon_name}")
                continue

            # Agrupamos los datos por proyecto
            if project_id not in proyectos_tipos:
                proyectos_tipos[project_id] = {
                    "name": project_name,
                    "personas": set(),
                    "tipos": set()
                }

            proyectos_tipos[project_id]["personas"].add(registro["person_id"])
            proyectos_tipos[project_id]["tipos"].update(tipos)

        # Identificamos el proyecto con más tipos únicos
        proyecto_con_mas_tipos = max(
            proyectos_tipos.items(),
            key=lambda x: len(x[1]["tipos"]),
            default=None
        )

        
        if proyecto_con_mas_tipos:
            project_id, data = proyecto_con_mas_tipos
            print("\nProyecto con más tipos de Pokémon favoritos:")
            print(f"ProyectoID: {project_id}")
            print(f"Nombre: {data['name']}")
            print(f"Número de Personas: {len(data['personas'])}")
            print(f"Tipos únicos de Pokémon: {len(data['tipos'])}")
            print(f"Lista de Tipos: {', '.join(data['tipos'])}")
        else:
            print("No se encontraron proyectos con tipos de Pokémon favoritos.")


        
    elif choice == '10':
        
        
        location_name = input("Introduce el nombre de la ubicación: ")

        # Paso 1: Consulta en MySQL para obtener el ID de la ubicación
        query_mysql = """
            SELECT id
            FROM locations
            WHERE name = %s;
        """
        cursor = db.connection.cursor()
        cursor.execute(query_mysql, (location_name,))
        location_result = cursor.fetchone()

        if not location_result:
            print(f"No se encontró la ubicación '{location_name}' en la base de datos MySQL.")
        else:
            location_id = location_result[0]

            # Paso 2: Consulta en MongoDB para obtener equipos, personas y proyectos asociados
            pipeline = [
                {
                    "$match": {  # Filtrar los equipos por location_id
                        "location_id": location_id
                    }
                },
                {
                    "$lookup": {  # Relacionar equipos con works_in_team
                        "from": "works_in_team",
                        "localField": "team_id",
                        "foreignField": "team_id",
                        "as": "personas_info"
                    }
                },
                {
                    "$lookup": {  # Relacionar equipos con proyectos
                        "from": "projects",
                        "localField": "project_id",
                        "foreignField": "project_id",
                        "as": "project_info"
                    }
                },
                {
                    "$unwind": "$personas_info"  # Descomponer las personas
                },
                {
                    "$unwind": "$project_info"  # Descomponer los proyectos
                },
                {
                    "$project": {  # Seleccionar solo los campos necesarios
                        "_id": 0,
                        "Equipo": "$name",
                        "PersonaID": "$personas_info.person_id",
                        "Proyecto": "$project_info.name"
                    }
                }
            ]

            resultados_mongo = list(mongo_operations.db['teams'].aggregate(pipeline))

            # Paso 3: Mostrar los resultados
            if resultados_mongo:
                print(f"\nEquipos en la ubicación '{location_name}':")
                for resultado in resultados_mongo:
                    print(f"Equipo: {resultado['Equipo']}, PersonaID: {resultado['PersonaID']}, Proyecto: {resultado['Proyecto']}")
            else:
                print(f"No se encontraron equipos asociados a la ubicación '{location_name}'.")


    elif choice == '11':
        break
    else:
        print("Invalid choice. Please select a valid option.")
