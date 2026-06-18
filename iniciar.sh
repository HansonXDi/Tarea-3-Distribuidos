#!/usr/bin/env bash
# iniciar.sh - Script auxiliar para consultar el estado de un proceso.
# Uso: ./iniciar.sh <NUMERO_DE_MAQUINA> ESTADO <NUMERO_DE_ID_DEL_TXT>

set -euo pipefail

BINARY="./expendedora"

# check_binary: verifica que el binario exista, si no lo compila.
# Entrada: ninguna. Salida: ninguna.
check_binary() {
    if [ ! -f "$BINARY" ]; then
        echo "[iniciar] Compilando..."
        go build -o "$BINARY" ./cmd/main.go
    fi
}

if [ $# -lt 3 ]; then
    echo "Uso: ./iniciar.sh <MAQUINA> ESTADO <ID>"
    exit 1
fi

MAQUINA=$1
CMD="${2^^}"
ID=$3

check_binary

case "$CMD" in
    ESTADO)
        "$BINARY" "$MAQUINA" ESTADO "$ID"
        ;;
    *)
        echo "Comando desconocido: $CMD"
        exit 1
        ;;
esac
