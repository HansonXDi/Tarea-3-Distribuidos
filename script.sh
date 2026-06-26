#!/usr/bin/env bash
# script.sh - Control de procesos expendedoras (gRPC, sin líder/réplicas) - Tarea 3 INF-343
# Uso:
#   ./script.sh <MAQUINA> <CANTIDAD>          -> iniciar N procesos
#   ./script.sh <MAQUINA> RESTAURAR <ID>      -> recuperar un proceso caído por quorum
#   ./script.sh <MAQUINA> MATAR <ID>          -> matar el proceso con ese ID
#   ./script.sh <MAQUINA> KILLALL             -> matar todos los procesos de la máquina
#   ./script.sh INFECTAR                      -> togglear modo infectado de esta máquina
#   ./script.sh <MAQUINA> ESTADO <ID>         -> ver estado de un proceso (ver iniciar.sh)

set -euo pipefail

BINARY="./expendedora"
PIDS_DIR=".pids"
MACHINE_INFECTED_FLAG=".infectado"

mkdir -p "$PIDS_DIR" logs

# build_binary: compila el binario Go si no existe.
# Entrada: ninguna. Salida: ninguna (falla si no compila).
build_binary() {
    echo "[script] Compilando..."
    go build -o "$BINARY" ./cmd/main.go
    echo "[script] Compilación exitosa."
}

# pid_file: retorna el path del archivo PID para una máquina y proceso dado.
# Entrada: MAQUINA, ID. Salida: path string.
pid_file() {
    echo "$PIDS_DIR/m${1}p${2}.pid"
}

# start_processes: lanza N procesos como N binarios independientes (un
# proceso real del SO por cada expendedora). Cada uno sortea su propio
# inventario, lo distribuye a TODOS los demás procesos del sistema, ejecuta
# sus instrucciones, y queda activo respondiendo peticiones gRPC.
# Entrada: MAQUINA, N. Salida: ninguna.
start_processes() {
    local MAQUINA=$1
    local N=$2
    [ -f "$BINARY" ] || build_binary

    # Guardar cuántos procesos se lanzan por máquina para que el binario
    # no intente contactar procesos que no existen.
    echo "$N" > .num_procesos

    for ID in $(seq 1 "$N"); do
        local PFILE
        PFILE=$(pid_file "$MAQUINA" "$ID")
        if [ -f "$PFILE" ] && kill -0 "$(cat "$PFILE")" 2>/dev/null; then
            echo "[script] Proceso M${MAQUINA}P${ID} ya está corriendo (PID=$(cat "$PFILE"))."
            continue
        fi

        "$BINARY" "$MAQUINA" PROC "$ID" > "logs/stdout_M${MAQUINA}P${ID}.log" 2>&1 &
        echo $! > "$PFILE"
        echo "[script] Iniciado M${MAQUINA}P${ID} (PID=$!)."
    done
}

# restore_process: recupera (reconstruye por quorum) un proceso específico,
# SIN re-ejecutar sus instrucciones.
# Entrada: MAQUINA, ID. Salida: ninguna.
restore_process() {
    local MAQUINA=$1
    local ID=$2
    [ -f "$BINARY" ] || build_binary
    local PFILE
    PFILE=$(pid_file "$MAQUINA" "$ID")

    "$BINARY" "$MAQUINA" RESTAURAR "$ID" > "logs/stdout_M${MAQUINA}P${ID}.log" 2>&1 &
    echo $! > "$PFILE"
    echo "[script] Proceso M${MAQUINA}P${ID} en recuperación (PID=$!). Revisa logs/stdout_M${MAQUINA}P${ID}.log para ver si el quorum fue exitoso."
}

# kill_process: mata el proceso que está leyendo la instrucción con el ID
# indicado, en la máquina indicada.
# Entrada: MAQUINA, ID. Salida: ninguna.
kill_process() {
    local MAQUINA=$1
    local ID=$2
    local PFILE
    PFILE=$(pid_file "$MAQUINA" "$ID")
    if [ -f "$PFILE" ]; then
        local PID
        PID=$(cat "$PFILE")
        if kill -9 "$PID" 2>/dev/null; then
            echo "[script] Proceso M${MAQUINA}P${ID} (PID=$PID) terminado."
        else
            echo "[script] PID $PID no encontrado (quizás ya terminó)."
        fi
        rm -f "$PFILE"
    else
        echo "[script] No hay PID registrado para M${MAQUINA}P${ID}."
    fi
}

# killall_machine: termina todos los procesos de una máquina.
# Entrada: MAQUINA. Salida: ninguna.
killall_machine() {
    local MAQUINA=$1
    for PFILE in "$PIDS_DIR"/m${MAQUINA}p*.pid; do
        [ -f "$PFILE" ] || continue
        local PID
        PID=$(cat "$PFILE")
        kill -9 "$PID" 2>/dev/null && echo "[script] Terminado PID=$PID" || true
        rm -f "$PFILE"
    done
    echo "[script] Todos los procesos de la máquina $MAQUINA terminados."
}

# toggle_infectar: crea o elimina el flag de infección de ESTA máquina. Los
# procesos ya activos reciben SIGUSR1 para detectar el cambio en caliente:
# desde ese momento, cualquier QueryInventory que reciban (de cualquier
# proceso, propio o de otra máquina) será respondido con datos falsos.
# Entrada: ninguna. Salida: ninguna.
toggle_infectar() {
    if [ -f "$MACHINE_INFECTED_FLAG" ]; then
        rm -f "$MACHINE_INFECTED_FLAG"
        echo "[script] Esta máquina: modo infectado DESACTIVADO."
    else
        touch "$MACHINE_INFECTED_FLAG"
        echo "[script] Esta máquina: modo infectado ACTIVADO."
        echo "[script] Todos los procesos de esta máquina responderán datos falsos ante cualquier consulta."
    fi

    local COUNT=0
    for PFILE in "$PIDS_DIR"/*.pid; do
        [ -f "$PFILE" ] || continue
        local PID
        PID=$(cat "$PFILE")
        kill -USR1 "$PID" 2>/dev/null && COUNT=$((COUNT + 1)) || true
    done
    [ "$COUNT" -gt 0 ] && echo "[script] Señal enviada a $COUNT proceso(s) activo(s) de esta máquina."
    return 0
}

# --- Parsing de argumentos ---

if [ $# -eq 0 ]; then
    echo "Uso: ./script.sh <MAQUINA> <CANTIDAD|RESTAURAR|MATAR|KILLALL|ESTADO> [ID]"
    echo "     ./script.sh INFECTAR"
    exit 1
fi

CMD="${1^^}"

case "$CMD" in
    INFECTAR)
        toggle_infectar
        ;;
    [0-9]*)
        MAQUINA=$1
        if [ $# -lt 2 ]; then
            echo "Error: falta subcomando."
            exit 1
        fi
        SUBCMD="${2^^}"
        case "$SUBCMD" in
            RESTAURAR)
                [ $# -ge 3 ] || { echo "Error: falta ID."; exit 1; }
                restore_process "$MAQUINA" "$3"
                ;;
            MATAR)
                [ $# -ge 3 ] || { echo "Error: falta ID."; exit 1; }
                kill_process "$MAQUINA" "$3"
                ;;
            KILLALL)
                killall_machine "$MAQUINA"
                ;;
            [0-9]*)
                start_processes "$MAQUINA" "$SUBCMD"
                ;;
            *)
                echo "Subcomando desconocido: $SUBCMD"
                exit 1
                ;;
        esac
        ;;
    *)
        echo "Argumento desconocido: $CMD"
        exit 1
        ;;
esac
