# Proyecto-nsql

# Este proyecto tiene como objetivo que a traves de 3 bases de datos guardemos todo lo que sucede en una aplicacion de e-commerce

# La base de datos de cassandra registra toda la interaccion del ususario con la pagina como son los anuncions vistos, errores por sesion, visitas de marca entre otros

# La base de datos de Dgraph crea relaciones de los usuarios con los productos, como los usuarios VIO tales productos o COMPRO productos, y hace las relaciones con las marcas y a que categorias pertenecen

# La base de datos de Mongo maneja datos de manera flexible del catlogo y usuarios, realiza busquedas avanzadas con indices filtros por categoria y pipelines de agregacion.

# Para llenar las bases de datos es necesario con correr solo el archivo de main ya que en ese archivo estamos importando el archivo de populate entonces a la hora de importar ya corre el archivo populate creando y llenando lo correspondiente

# Se necesita tener un contendero corriendo de Mongo, cassandra y dgraph, puedes tener uno con ratel para poder visualiar los datos de dgraph.

# Correr este comando para que funcione mongo 
# python -m uvicorn main_mongo:app --reload (Levanta el servidor para la api de mongo)

# Solo es necesario correr python main.py dentro de la carpeta madre del proyecto
# Te mostrara un menu para ver que base de datos quieres usar y a partir de esa seleccion te manda a otro menu de la base de datos. 

# Para salir de las bases de datos hay una opcion dentro del menu, que te regresa al menu principal con las bases de datos, y para salir del programa solo tecleas la opcion de salir dentro del menu.

# Miguel Angel Franco Diaz 749169
# Karen Elizabeth Gonzalez Santana 752913
# Angel Aceves Manzo 751658
