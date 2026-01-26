#!/bin/bash

# Termina se o número de argumentos for diferente de 2
if [ "$#" -ne 2 ]; then            #se nº de argumentos for diferente de 2 então
  echo "Uso: $0 <pasta_dos_clientes> <max_processos>"    #faz, nao houve problemas
  exit 1       #erro
fi

pasta="$1"            #é guardado na variavel pasta o primeiro argumento
max_processos="$2"    #é guardado na variavel max_processos o segundo argumento

# aqui vamos verificar se a pasta que foi indicada pelo utilizador existe
if [ ! -d "$pasta" ]; then        #se a $pasta nao existir, ! nega a condiçao no bash
  echo "Pasta não existe: $pasta"        #pasta nao existe
  exit 1            #erro
fi

#vamos verificar se a var max_processos é um numero inteiro positivo
if ! [[ "$max_processos" =~ ^[0-9]+$ ]] || (( max_processos <= 0 )); then        #! como nega entao, faz o contrario de ver se a var e u numero inteiro, vendo se tem sinais ou letras ou entao verifica se o numero inserido e menor ou igual a 0
  echo "$max_processos deve ser um número inteiro positivo"               #valor da var nao esta bem ou seja so pode ser um numero inteiro positivo
  exit 1         #erro
fi

for i in "$pasta"/*.csv; do            #vai percorrer todos os ficheiros .csv na pasta

python3 analisar_cliente.py "$i" &      #lança se o programa python para cada ficheiro csv

while (( $(jobs -r | wc -l) >= max_processos )); do         #enquanto se tiver mais ou igual processos que estao ativos do que a var max_processos
    wait -n            #espera se que qualquer um dos processos ativos termine
done

done

wait             #aqui vai esperar que todos os processos terminem antes de sair
