from xmlrpc.server import SimpleXMLRPCServer
from socketserver import ThreadingMixIn
import multiprocessing
import redis
import requests
from collections import defaultdict #aixo pel countwords a veure que tal
import time
from aux_functions import *
#   id_to_work: per controlar quants fitxers pendents queden per assignar a un worker
#   id_work_completed: tarea completada

proc = []
# Creem conexio amb redis
try:
    r = redis.StrictRedis(host='localhost', port=6379, password='')
except Exception as e:
    print(e)


#  Creem un worker i li assignem una tarea o esperem a que se li pugui assignar
def crea_worker(index):
    #Si el worker esta actiu el podrem agafar per a que faci una tarea
    while r.lindex('workersList', index).decode('ascii') == 'True':
        time.sleep(0.5)
        #si hi han tareas a la llista de tareas n'assignem una al worker actiu
        if r.llen('tasksList') > 0:
            
            #agafem una nova tarea i la decodifiquem a ascii (format - > 'id','tarea','fitxer')
            work = r.lpop('tasksList')
            if work:
                work = work.decode('ascii').split(',')

                # cogemos la longitud de la tarea spliteada, para saber si se trata de una tarea simple o múltiple


                tam = len(work)
                id = work.pop(0)
                task = work.pop(0)
                #print(id)
                #print(task)
                #print(work)
                
                
                if tam > 3:
                    # si hi ha mes de 3 parametres vol dir que hi ha me de un path de fitxer
                    url = work.pop(0)
                    url2 = work.pop(0)

                    url2 = url2.replace('[', '')
                    url2 = url2.replace(']', '')
                    # a new_task anira la nova tarea assignada a un altre worker, necesitem r.lpush de nova tarea
                    new_task = id + ',' + task + ',' + url2
                    id_newtask = id + '_to_work'
                    # li diem a la llista de la id d'aquesta tarea que encara queda un fitxer per contar
                    if not r.exists(id_newtask):
                        id_work = id + '_work'
                        r.rpush(id_newtask, tam - 2)
                        r.rpush(id_work, tam - 2)
                    # treiem la tarea original i la sustituim per una de nova amb un fitxer menys per analitzar
                    r.lpush('tasksList', new_task)
                    task_wordcount(url, task, id, 0)

                elif tam == 3:
                    url = work.pop(0)
                    id_newtask = id + '_to_work'

                    # si ja exisita una llista amb aquest task id es perque tenia mes d'un fitxer
                    # crear una nova tarea amb 2 parametres que indicaran que aquell worker ha de sumar els resultats dels altres
                    if r.exists(id_newtask):
                        r.lpush('tasksList', id + ',' + task)
                        task_wordcount(url, task, id, 0)
                    # si no, simplemente quitamos la tarea de la lista
                    else:
                        task_wordcount(url, task, id, 1)

                #El worker que estigui aqui esperara a que s'acabi de contar les paraules dels fitxers que has introduit i sumara 
                #els resultats
                elif tam == 2:
                    
                    # mirem com van els workers amb els seus respectius fitxers
                    id_newtask = id + '_to_work'
                    task_pending = r.lpop(id_newtask)
                    # esperem a que els altres workers vaigin pujant els seus resultats per sumar-los
                    while not task_pending:
                        task_pending = r.lpop(id_newtask)
                        
                    if task_pending:
                        task_pending = task_pending.decode('ascii')
                        task_pending = int(task_pending)
                    
                    if task == 'wordcount':
                        #li fico un work_is_completed per a que no torni a sortir a la llista que no se eliminarla de moment
                        id_tasks = id + '_work_completed'
                        task_completed = 0
                        #print("test final 2")
                        # comprobamos hasta que esten todas completadas, número tareas a completar = tareas completadas
                        while task_pending > task_completed:
                            task_completed = r.llen(id_tasks)
                            values = r.lrange(id_tasks, 0, task_pending - 1)
                            print("test final 1")
                            counter = 0
                        for value in values:
                            counter = counter + int(value.decode('ascii'))

                        id_newtask = id + '_ready'
                        r.rpush(id_newtask, counter)
                        
                    elif task == 'countwords':
                        #print("¯\_(ツ)_/¯")
                        print("No funciona")

def task_wordcount(url, task, id, op):
    url = url.replace('[', '')
    url = url.replace(']', '')

    request = requests.get(url, allow_redirects=True)
    work_string = request.content.decode(request.encoding)
    if task == 'wordcount':
        words = len(work_string.split())
        if op:
            id_newtask = id + '_ready'
            r.rpush(id_newtask, words)
        else:
            id_newtask = id + '_work_completed'
            r.rpush(id_newtask, words)

#creem tots els workers demanats al menu
def crea_workers(n_workers):
    global proc
    for value in range(n_workers):
        process = multiprocessing.Process(target=crea_worker, args=(r.llen('workersList'),))
        proc.append(process)
        r.rpush('workersList', 'True')
        process.start()



def esborra_worker(index):
    if r.llen('workersList') != 0:
        r.lset('workersList', index, 'False')
    proc[index].join()


def llista_workers():
    workers = []
    for worker in range(r.llen('workersList')):
        workers.append("ID:" + str(worker) + " ESTADO: " + r.lindex('workersList', worker).decode('ascii')+ '\n')

    return workers


class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


def add_task(task):
    id_num = 0
    if r.exists('task_ids'):
        id_num = str(r.lpop('task_ids').decode('ascii'))
    else:
        id_num = 0
    #print("TEST2")
    print('Nueva tarea:' + task + ' id: ' + str(id_num))
    task_do = task.split(',')
    task_do = task_do[0]
    task = str(id_num) + ',' + task
    r.lpush('tasksList', task)
    id = str(id_num) + '_ready'
    id_num = int(id_num) + 1
    r.rpush('task_ids', id_num)
    #print("TEST1")
    while not r.exists(id):
        time.sleep(0.1)
    
    value = r.lpop(id)
    return value


# run server
def run_server(host="localhost", port=11000):
    r.flushall()
    crea_workers(3)
    server_addr = (host, port)
    server = SimpleThreadedXMLRPCServer(server_addr, allow_none=True)
    server.register_function(crea_workers, 'crea_workers')
    server.register_function(esborra_worker, 'esborra_worker')
    server.register_function(llista_workers, 'llista_workers')
    server.register_function(add_task, 'add_task')
    

    print('listening on {} port {}'.format(host, port))

    server.serve_forever()


if __name__ == '__main__':
    run_server()
