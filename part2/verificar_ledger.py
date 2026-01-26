import pickle
import os

filename = "ledger.bin"

if not os.path.exists(filename):
    print(f"Erro: O ficheiro {filename} não existe.")
else:
    print(f"--- A LER {filename} ---")
    contador = 0
    try:
        with open(filename, "rb") as f:
            while True:
                # O pickle.load lê um objeto de cada vez
                dados = pickle.load(f)
                print(f"Entrada {contador}: {dados}")
                contador += 1
    except EOFError:
        # Isto é normal, significa que o ficheiro acabou
        print("--- FIM DO FICHEIRO ---")
    except Exception as e:
        print(f"Erro a ler: {e}")
        
    print(f"Total de registos encontrados: {contador}")
