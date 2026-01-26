import sys
import threading
import queue
import time
import os
#em cima estão as bibliotecas usadas

# Criação da Thread 1 que identifica todas as compras caras
def thread_comprascaras(nome_cliente, q):   #funçao recebe uma string que vai guardar qual o nome do cliente e q que e a fila da qual a thread vai retirar oq tem que processar
    while True:    #thread vai estar sempre a rodar enquanto nao recever None
        elemento = q.get()     #thread pega elemento da fila
        if elemento is None:   #se esse elemento tiver a mensagem None acaba ali pois ja nao ha mais elementos para processar
            q.task_done()      # informa que o elemento foi  processado na fila
            break     #thread termina
        data, hora, produto, preco = elemento     #o elemento tirado da fila e separado em 4 variaveis
        if preco > 1000.0:    #se o preço e superior a 1000 euros
            print(f"[{nome_cliente}:T1] Compra cara: {produto} -> {preco:.2f}€", flush=True)     #imprime a mensagem padrao designado a esta thread
        time.sleep(0.01)     #sempre depois de analisar um elemento faz uma pausa
        q.task_done()        #informa que o elemento foi  processado na fila



# Cria a Thread 2 que soma o valor total gasto pelo cliente em todas as compras
def thread_totalglobal(nome_cliente, q):      #funçao recebe uma string que vai guardar qual o nome do cliente e q que e a fila da qual a thread vai retirar oq tem que processar
    total_global = 0.0       #inicia a var a zeros que vai guardar o gasto total
    while True:      #thread vai estar sempre a rodar enquanto nao recever None
        elemento = q.get()      #thread pega elemento da fila
        if elemento is None:    #se esse elemento tiver a mensagem None acaba ali pois ja nao ha mais elementos para processar
            q.task_done()       #informa que o elemento foi  processado na fila
            break      #thread termina
        data, hora, produto, preco = elemento       #o elemento tirado da fila e separado em 4 variaveis
        total_global += preco      #adiciona o preco de cada compra na var total_global
        time.sleep(0.01)           #sempre depois de analisar um elemento faz uma pausa
        q.task_done()          #informa que o elemento foi  processado na fila
    print(f"[{nome_cliente}:T2] Total gasto: {total_global:.2f}€", flush=True)         #imprime a mensagem padrao designado a esta thread
#print no final porque ele so imprime no final depois de somar tudo e na 1 e 3 imprime cada vez que enconta 


# cria a Thread 3 que identifica todas as comprasfeitas nos dias especiais
def thread_analisetemporal(nome_cliente, q):         #funçao recebe uma string que vai guardar qual o nome do cliente e q que e a fila da qual a thread vai retirar oq tem que processar
    while True:         #thread vai estar sempre a rodar enquanto nao recever None
        elemento = q.get()         #thread pega elemento da fila
        if elemento is None:       #se esse elemento tiver a mensagem None acaba ali pois ja nao ha mais elementos para processar
            q.task_done()          #informa que o elemento foi  processado na fila
            break         #thread termina
        data, hora, produto, preco = elemento       #o elemento tirado da fila e separado em 4 variaveis
        dia = data[8:10]       #tira o dia do mes a partir da string data, tira se os dois ultimos digitos
        if dia in ["29","30","31"]:      #se o valor guardado na variavel dia for 29,30,31 e considerado uma compra especial
            print(f"[{nome_cliente}:T3] Compra dia especial: {produto} -> {data}", flush=True)      #imprime a mensagem padrao designado a esta thread
        time.sleep(0.01)      #sempre depois de analisar um elemento faz uma pausa
        q.task_done()         #informa que o elemento foi  processado na fila


#main vai ser a funçao principal pois e aqui o ponto de partida do programa python
def main():
    if len(sys.argv) != 2:        #se o tamanho da lista dos argumentos passados na linha de comandos for diferende de 2
        print("Uso: python3 analisar_cliente.py <cliente.csv>")      #mostra como deve escrever
        sys.exit(1)       #erro e termina
    csv = sys.argv[1]      #csv vai guardar o caminho do ficheiro.csv
    cliente = os.path.splitext(os.path.basename(csv))[0]          #no cliente é posto apenas o nome do cliente que e usado para identificar nasmensagens 
                                                                  #os.path.basename(csv) tira o caminho e fica só com o nome e depois os.path.splitext(...)[0] remove a extensão .csvficando apenas o nome do cliente
    print(f"[{cliente}:main] Análise iniciada.", flush=True)      #imprime que a analise do cliente descoberto anteriormente ja comecou

#cria se 3 filas para cada thread usar uma diferente, maxsize limita a fila a no maximo 5 elementos de cada vez para thread nao ficar cheia 
    q1 = queue.Queue(maxsize=5)
    q2 = queue.Queue(maxsize=5)
    q3 = queue.Queue(maxsize=5)


#aqui cria se as threads e iniciam se, fazendo a associaçao de cada funçao a cada thread e passando os argumentos para as funlçoes
    t1 = threading.Thread(target=thread_comprascaras, args=(cliente, q1))
    t2 = threading.Thread(target=thread_totalglobal, args=(cliente, q2))
    t3 = threading.Thread(target=thread_analisetemporal, args=(cliente, q3))
    t1.start()
    t2.start()
    t3.start()


    with open(csv, "r") as f:        #vai se abrir o ficheiro csv para leitura 
        next(f)                      #salta se a primeira linha que pertence ao cabeçalho
        for line in f:           #para cada linha do ficheiro
            line = line.strip()     #remove espaços
            if not line:          #sao passadas a frente as linhas vazias 
                continue
            partes = line.split(",")        #divide se a linha pelas virgulas
            if len(partes) != 4:
                continue                #se ao dividirmos a linha nao estiver 4 elementos passa sea frente
            data, hora, produto, preco = partes         #aqui estao os elemntos de cada linha 
            preco = float(preco)                 #convertimos o preço para float
            q1.put((data, hora, produto, preco))
            q2.put((data, hora, produto, preco))
            q3.put((data, hora, produto, preco))
#manda para cada fila os elementos de cada fila todos separados 



#aqui serve para aviusar as threads que terminem pois recebem None
    q1.put(None)
    q2.put(None)
    q3.put(None)



# Espera-se que todos as tarefas das filas sejam  marcadas como concluidos pela q.task_done()
    q1.join()
    q2.join()
    q3.join()



#espera se que threads terminem por completo antes de se continuar
    t1.join()
    t2.join()
    t3.join()

    print(f"[{cliente}:main] Análise concluída.", flush=True)          #print que e mostrado a dizer que a analise terminou

if __name__ == "__main__":
    main()   

