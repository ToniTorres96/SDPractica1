import xmlrpc
import xmlrpc.client
#python -m http.server 8000 
def start_connection():
    server = xmlrpc.client.ServerProxy("http://localhost:11000/", allow_none=True)
    ans=True
    while ans:
        print ("""
        1.Workers
        2.Tareas
        3.Salir
        """)
        ans=input("Que opción te gustaria escojer? ") 
        if ans=="1": 
            print ("""
            1.Crear un nuevo worker
            2.Eliminar un worker
            3.Listar los workers
            """)
            ans=input("Que opción te gustaria escojer? ") 
            if ans=="1":
                ans=input("Cuantos workers quieres crear? ") 
                print('CREANDO UN NUEVO WORKER')
                server.crea_workers(int(ans)) #mirar per crear un worker solament
            if ans=="2":
                print('BORRANDO UN WORKER')
                server.esborra_worker()
            if ans=="3":
                print('WORKERS LIST')
                print(server.llista_workers())

        elif ans == "2":
            print ("""
            1.Wordcount
            2.Countwords(no funciona)
            """)
            ans=input("Que opción te gustaria escojer? ") 
            if ans=="1":
                file_name=input("De que fichero? ") 
                file_name = file_name.split(',')
                i = 0
                for file in file_name:
                    if(i==0):
                        argument = 'wordcount,' '[http://localhost:8000/' + file
                    else:
                        argument = argument + ',' + 'http://localhost:8000/' + file
                    i=i+1
                argument = argument + ']'
                print(argument)
                print(server.add_task(argument))
            if ans=="2":
                file_name=input("De que fichero? ")
                #no funciona
                argument = 'countwords' + ',' + '[http://localhost:8000/' + file_name + ']'
                print(server.add_task(argument))


if __name__ == '__main__':
    start_connection()
