import sys
import time
import os
import signal
import pickle
from multiprocessing import Process, Semaphore, Lock, Array, Value

# CONFIGURAÇÕES GERAIS

BUFFER_SIZE = 5
MAX_STRING_LEN = 250
LEDGER_FILE = "ledger.bin"

# Identificadores de Recursos
R_STOCK = 0
R_DELIVERY = 1

# Identificadores de Processos (para o Algoritmo do Banqueiro)
ID_EXPENSIVE = 0
ID_SPECIAL = 1

# ALGORITMO DO BANQUEIRO (DETEÇÃO DE ESTADO E PREVENÇÃO)

def imprimir_estado(nome, available, allocation, safety_status):
    """
    Imprime o estado atual dos recursos e se o estado é SAFE ou UNSAFE.
    """
    stock_livre = available[R_STOCK]
    deliv_livre = available[R_DELIVERY]

    # Quem tem o quê
    exp_stock = allocation[ID_EXPENSIVE * 2 + R_STOCK]
    exp_deliv = allocation[ID_EXPENSIVE * 2 + R_DELIVERY]
    spec_stock = allocation[ID_SPECIAL * 2 + R_STOCK]
    spec_deliv = allocation[ID_SPECIAL * 2 + R_DELIVERY]

    print(f"[{nome}] Estado: Stock={stock_livre} Delivery={deliv_livre} | "
          f"Exp:[{exp_stock},{exp_deliv}] Spec:[{spec_stock},{spec_deliv}] -> [{safety_status}]")

def verificar_safety(available, allocation):
    """
    Simula se o estado atual permite que todos os processos terminem (SAFE).
    """
    work = [available[0], available[1]]
    finish = [False, False] # 0: Expensive, 1: Special

    # Matriz de Alocação Atual
    alloc = [
        [allocation[ID_EXPENSIVE * 2 + R_STOCK], allocation[ID_EXPENSIVE * 2 + R_DELIVERY]],
        [allocation[ID_SPECIAL * 2 + R_STOCK], allocation[ID_SPECIAL * 2 + R_DELIVERY]]
    ]

    # Matriz de Necessidade (Need = Max - Allocation). Max é sempre 1.
    need = [
        [1 - alloc[0][0], 1 - alloc[0][1]],
        [1 - alloc[1][0], 1 - alloc[1][1]]
    ]

    while True:
        found = False
        for i in range(2):
            if not finish[i]:
                # Se as necessidades podem ser satisfeitas com o 'work' atual
                if work[0] >= need[i][0] and work[1] >= need[i][1]:
                    work[0] += alloc[i][0]
                    work[1] += alloc[i][1]
                    finish[i] = True
                    found = True
        if not found:
            break

    return "SAFE STATE" if finish[0] and finish[1] else "UNSAFE STATE"

def adquirir_recurso_seguro(proc_id, res_id, nome_proc, available, allocation, mutex_state, sem_recurso_real):
    """
    Tenta adquirir um recurso usando Banker's Algorithm + Non-blocking acquire.
    """
    while True:
        with mutex_state:

            status_atual = verificar_safety(available, allocation)
            imprimir_estado(nome_proc, available, allocation, f"REQ_TRY -> {status_atual}")

            # Verifica a disponibilidade nas estruturas de controlo
            if available[res_id] > 0:
                # Simula a alocação
                available[res_id] -= 1
                allocation[proc_id * 2 + res_id] += 1

                # Verifica a segurança
                status = verificar_safety(available, allocation)

                if status == "SAFE STATE":
                    # Tenta obter o semáforo real
                    got = sem_recurso_real.acquire(block=False)
                    if got:
                        imprimir_estado(nome_proc, available, allocation, status)
                        return # Sucesso: temos o recurso e o estado reflete isso
                    else:
                        # Falha rara na aquisição real (race condition), reverte simulação
                        available[res_id] += 1
                        allocation[proc_id * 2 + res_id] -= 1
                else:
                    # UNSAFE: Reverte a simulação
                    imprimir_estado(nome_proc, available, allocation, status)
                    available[res_id] += 1
                    allocation[proc_id * 2 + res_id] -= 1

        # Espera antes de tentar novamente
        time.sleep(0.1)

def libertar_recurso(proc_id, res_id, nome_proc, available, allocation, mutex_state, sem_recurso_real):
    """
    Liberta o recurso físico e atualiza as estruturas de controlo.
    """
    try:
        sem_recurso_real.release()
    except ValueError:
        pass # Ignorar se já estiver livre

    with mutex_state:
        available[res_id] += 1
        allocation[proc_id * 2 + res_id] -= 1
        status = verificar_safety(available, allocation)
        imprimir_estado(nome_proc, available, allocation, status)

# LEDGER & BUFFER

def registar_no_ledger(dicionario, mutex_ledger):
    with mutex_ledger:
        try:
            with open(LEDGER_FILE, "ab") as f:
                pickle.dump(dicionario, f)
        except Exception as e:
            print(f"Erro no Ledger: {e}")

def escrever_buffer(buffer, index, texto):
    start = index * MAX_STRING_LEN
    b_texto = texto.encode('utf-8')[:MAX_STRING_LEN]
    # Escrever bytes
    for i, byte in enumerate(b_texto):
        buffer[start + i] = byte
    # Limpar resto do slot com zeros
    for i in range(len(b_texto), MAX_STRING_LEN):
        buffer[start + i] = 0

def ler_buffer(buffer, index):
    start = index * MAX_STRING_LEN
    # Ler bytes puros
    raw = bytes(buffer[start:start + MAX_STRING_LEN])
    try:
        return raw.split(b'\0')[0].decode('utf-8')
    except:
        return ""

def parse_linha(linha):
    try:
        partes = linha.split(',')
        if len(partes) < 4: return None

        # Formato específico
        data = partes[0].strip()
        hora = partes[1].strip()
        produto = partes[2].strip()
        preco = float(partes[3].strip())

        return data, hora, produto, preco
    except:
        return None

# PROCESSOS WORKERS

def worker_expensive(nome, buffer_dados, leituras_slot, sem_meu, sem_empty, mutex_slots,
                     r_stock, r_delivery, available, allocation, mutex_state, mutex_ledger):
    idx = 0
    while True:
        sem_meu.acquire() # Espera notificação do produtor

        # Leitura segura
        linha = ler_buffer(buffer_dados, idx)

        # Lógica de libertação do slot
        # Deve ser feito ANTES de verificar se é FIM para libertar o produtor
        with mutex_slots:
            leituras_slot[idx] += 1
            if leituras_slot[idx] == 3: # Último a ler liberta o slot
                leituras_slot[idx] = 0
                sem_empty.release()

        if linha == "FIM": break

        dados = parse_linha(linha)
        if dados:
            data, hora, produto, preco = dados
            if preco > 1000.0:
                # Ordem: Stock -> Delivery
                adquirir_recurso_seguro(ID_EXPENSIVE, R_STOCK, nome, available, allocation, mutex_state, r_stock)
                time.sleep(0.001)
                adquirir_recurso_seguro(ID_EXPENSIVE, R_DELIVERY, nome, available, allocation, mutex_state, r_delivery)

                print(f"[{nome}] PROCESSADO: {produto} ({preco}€)")
                registar_no_ledger({"nome": produto, "price": preco, "expensive": True}, mutex_ledger)
                time.sleep(0.05)

                libertar_recurso(ID_EXPENSIVE, R_DELIVERY, nome, available, allocation, mutex_state, r_delivery)
                libertar_recurso(ID_EXPENSIVE, R_STOCK, nome, available, allocation, mutex_state, r_stock)

        idx = (idx + 1) % BUFFER_SIZE

def worker_special(nome, buffer_dados, leituras_slot, sem_meu, sem_empty, mutex_slots,
                   r_stock, r_delivery, available, allocation, mutex_state, mutex_ledger):
    idx = 0
    while True:
        sem_meu.acquire()

        linha = ler_buffer(buffer_dados, idx)

        with mutex_slots:
            leituras_slot[idx] += 1
            if leituras_slot[idx] == 3:
                leituras_slot[idx] = 0
                sem_empty.release()

        if linha == "FIM": break

        dados = parse_linha(linha)
        if dados:
            data, hora, produto, preco = dados
            try:
                # Data tem o formato YYYY-MM-DD
                dia = int(data.split('-')[2])
                if dia >= 29:
                    # Ordem INVERSA: Delivery -> Stock (Risco de deadlock gerido pelo Banker's)
                    adquirir_recurso_seguro(ID_SPECIAL, R_DELIVERY, nome, available, allocation, mutex_state, r_delivery)
                    time.sleep(0.001)
                    adquirir_recurso_seguro(ID_SPECIAL, R_STOCK, nome, available, allocation, mutex_state, r_stock)

                    print(f"[{nome}] PROCESSADO: {produto} em {data}")
                    registar_no_ledger({"nome": produto, "price": preco, "special": True}, mutex_ledger)
                    time.sleep(0.05)

                    libertar_recurso(ID_SPECIAL, R_STOCK, nome, available, allocation, mutex_state, r_stock)
                    libertar_recurso(ID_SPECIAL, R_DELIVERY, nome, available, allocation, mutex_state, r_delivery)
            except: pass

        idx = (idx + 1) % BUFFER_SIZE

# Variáveis globais para o Signal Handler
total_global = 0.0
nome_proc_total = "P_Total"

def handler_alarme(signum, frame):
    print(f"[{nome_proc_total}] Total acumulado até agora: {total_global:.2f}€", flush=True)
    signal.alarm(3)

def worker_total(nome, buffer_dados, leituras_slot, sem_meu, sem_empty, mutex_slots):
    global total_global, nome_proc_total
    nome_proc_total = nome

    # Configurar alarme
    signal.signal(signal.SIGALRM, handler_alarme)
    signal.alarm(3)

    idx = 0
    while True:
        sem_meu.acquire()

        linha = ler_buffer(buffer_dados, idx)

        with mutex_slots:
            leituras_slot[idx] += 1
            if leituras_slot[idx] == 3:
                leituras_slot[idx] = 0
                sem_empty.release()

        if linha == "FIM":
            try: signal.alarm(0) # Cancelar alarme
            except: pass
            print(f"[{nome}] Total Final: {total_global:.2f}€")
            break

        dados = parse_linha(linha)
        if dados:
            # dados[3] é o preço
            total_global += dados[3]

        idx = (idx + 1) % BUFFER_SIZE

# MAIN

def main():
    if len(sys.argv) != 2:
        print("Uso: python3 analisar_cliente.py <ficheiro.csv>")
        sys.exit(1)

    csv_path = sys.argv[1]
    if not os.path.exists(csv_path):
        print(f"Erro: Ficheiro {csv_path} não encontrado.")
        sys.exit(1)

    # Extrair nome do ficheiro (ex: customer1)
    filename = os.path.basename(csv_path).split('.')[0]

    # Print inicial com nome do cliente
    print(f"[{filename}:main] Análise iniciada.")

    # Memória Partilhada
    buffer_dados = Array('B', BUFFER_SIZE * MAX_STRING_LEN)
    leituras_slot = Array('i', BUFFER_SIZE)
    mutex_slots = Lock()

    # Semáforos do Buffer (Multicast: 1 produtor -> 3 consumidores)
    sem_empty = Semaphore(BUFFER_SIZE)
    sem_read_exp = Semaphore(0)
    sem_read_spec = Semaphore(0)
    sem_read_tot = Semaphore(0)

    # Recursos e Estado (Banker's Algorithm)
    r_stock = Semaphore(1)
    r_delivery = Semaphore(1)
    available = Array('i', [1, 1])
    allocation = Array('i', [0, 0, 0, 0])
    mutex_state = Lock()
    mutex_ledger = Lock()

    # Passar o nome composto nos argumentos
    p_exp = Process(target=worker_expensive, args=(f"{filename}:P_Expensive", buffer_dados, leituras_slot, sem_read_exp, sem_empty, mutex_slots, r_stock, r_delivery, available, allocation, mutex_state, mutex_ledger))
    p_spec = Process(target=worker_special, args=(f"{filename}:P_Special", buffer_dados, leituras_slot, sem_read_spec, sem_empty, mutex_slots, r_stock, r_delivery, available, allocation, mutex_state, mutex_ledger))
    p_tot = Process(target=worker_total, args=(f"{filename}:P_Total", buffer_dados, leituras_slot, sem_read_tot, sem_empty, mutex_slots))

    p_exp.start(); p_spec.start(); p_tot.start()

    # Produtor (Main)
    idx_escrita = 0
    try:
        with open(csv_path, 'r') as f:
            next(f, None) # Ignorar header
            for linha in f:
                linha = linha.strip()
                if not linha: continue

                sem_empty.acquire()
                escrever_buffer(buffer_dados, idx_escrita, linha)

                # Inicializa contagem de leituras
                with mutex_slots: leituras_slot[idx_escrita] = 0

                # Broadcast para os 3 workers
                sem_read_exp.release()
                sem_read_spec.release()
                sem_read_tot.release()

                idx_escrita = (idx_escrita + 1) % BUFFER_SIZE

        # Enviar FIM
        sem_empty.acquire()
        escrever_buffer(buffer_dados, idx_escrita, "FIM")
        with mutex_slots: leituras_slot[idx_escrita] = 0
        sem_read_exp.release(); sem_read_spec.release(); sem_read_tot.release()

    except Exception as e:
        print(f"Erro no Main: {e}")

    p_exp.join(); p_spec.join(); p_tot.join()

    # Print final com nome do cliente
    print(f"[{filename}:main] Análise concluída.")

if __name__ == "__main__":
    main()
