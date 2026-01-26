#!/bin/bash

# se não tiver 2 argumentos, sai
if [ "$#" -ne 2 ]; then
  echo "Uso: $0 <pasta_dos_clientes> <max_processos>"
  exit 1
fi

pasta="$1"
max_processos="$2"

# se a pasta não existir, sai
if [ ! -d "$pasta" ]; then
  echo "Pasta não existe: $pasta"
  exit 1
fi

#se não for um dígito e for <=0, sai
if ! [[ "$max_processos" =~ ^[0-9]+$ ]] || (( max_processos <= 0 )); then
  echo "$max_processos deve ser um número inteiro positivo"
  exit 1
fi

#apaga o ledger se ja existir
rm -f ledger.bin

for i in "$pasta"/*.csv; do
  python3 analisar_cliente.py "$i" &
  while (( $(jobs -r | wc -l) >= max_processos )); do
     wait -n
  done
done

wait
