#!/usr/bin/env bash
# script.sh - Control de procesos expendedoras (arquitectura REST) - Tarea 3 INF-343
# Uso:
#   ./script.sh <MAQUINA> <CANTIDAD>          -> iniciar N procesos (cada uno corre su ciclo completo)
#   ./script.sh <MAQUINA> RESTAURAR <ID>      -> restaurar (reiniciar ciclo completo) de un proceso
#   ./script.sh <MAQUINA> MATAR <ID>          -> matar proceso por ID
#   ./script.sh <MAQUINA> KILLALL             -> matar todos los procesos de la máquina
#   ./script.sh INFECTAR                      -> togglear modo infectado en todos los procesos locales
#   ./script.sh <MAQUINA> ESTADO <ID>         -> ver estado de un proceso (ver iniciar.sh)

set -euo pipefail

BINARY="./expendedora"
PIDS_DIR=".pids"

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

# start_processes: lanza N procesos como N binarios independientes. Si el
# flag de infección de la máquina (.infectado) existe, crea el flag
# individual por proceso (.infectado_M<M>P<ID>) antes de lanzar cada uno,
# de modo que arrancan en modo infectado desde el principio.
# Entrada: MAQUINA, N. Salida: ninguna.
start_processes() {
    local MAQUINA=$1
    local N=$2
    [ -f "$BINARY" ] || build_binary

    for ID in $(seq 1 "$N"); do
        local PFILE
        PFILE=$(pid_file "$MAQUINA" "$ID")
        if [ -f "$PFILE" ] && kill -0 "$(cat "$PFILE")" 2>/dev/null; then
            echo "[script] Proceso M${MAQUINA}P${ID} ya está corriendo (PID=$(cat "$PFILE"))."
            continue
        fi

        # Si la máquina está infectada, crear el flag individual antes de iniciar
        local PFLAG=".infectado_M${MAQUINA}P${ID}"
        if [ -f "$MACHINE_INFECTED_FLAG" ]; then
            touch "$PFLAG"
            echo "[script] M${MAQUINA}P${ID}: arrancará en modo INFECTADO."
        else
            rm -f "$PFLAG"
        fi

        "$BINARY" "$MAQUINA" PROC "$ID" > "logs/stdout_M${MAQUINA}P${ID}.log" 2>&1 &
        echo $! > "$PFILE"
        echo "[script] Iniciado M${MAQUINA}P${ID} (PID=$!)."
    done
}

# restore_process: restaura (reinicia el ciclo de vida completo) de un
# proceso específico.
# Entrada: MAQUINA, ID. Salida: ninguna.
restore_process() {
    local MAQUINA=$1
    local ID=$2
    [ -f "$BINARY" ] || build_binary
    local PFILE
    PFILE=$(pid_file "$MAQUINA" "$ID")

    "$BINARY" "$MAQUINA" RESTAURAR "$ID" > "logs/stdout_M${MAQUINA}P${ID}.log" 2>&1 &
    echo $! > "$PFILE"
    echo "[script] Proceso M${MAQUINA}P${ID} restaurado (PID=$!)."
}

# kill_process: termina un proceso específico por su PID guardado y limpia
# su flag de infección individual si existe.
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
        rm -f "$PFILE" ".infectado_M${MAQUINA}P${ID}"
    else
        echo "[script] No hay PID registrado para M${MAQUINA}P${ID}."
    fi
}

# killall_machine: termina todos los procesos de una máquina y limpia sus
# flags de infección individuales.
# Entrada: MAQUINA. Salida: ninguna.
killall_machine() {
    local MAQUINA=$1
    for PFILE in "$PIDS_DIR"/m${MAQUINA}p*.pid; do
        [ -f "$PFILE" ] || continue
        local PID
        PID=$(cat "$PFILE")
        local BASENAME
        BASENAME=$(basename "$PFILE" .pid)
        kill -9 "$PID" 2>/dev/null && echo "[script] Terminado PID=$PID" || true
        rm -f "$PFILE" ".infectado_${BASENAME^^}"
    done
    echo "[script] Todos los procesos de la máquina $MAQUINA terminados."
}

MACHINE_INFECTED_FLAG=".infectado"

# toggle_infectar: crea o elimina el flag de infección de esta máquina.
# Debe ejecutarse ANTES de ./script.sh <MAQUINA> <N> para que los procesos
# que se creen a continuación arranquen en modo infectado desde el inicio,
# generando datos corruptos durante su ejecución.
# Si ya hay procesos corriendo, también les envía SIGUSR1 para que se marquen
# infectados en tiempo real (aunque sus instrucciones ya se hayan ejecutado).
# Entrada: ninguna. Salida: ninguna.
toggle_infectar() {
    if [ -f "$MACHINE_INFECTED_FLAG" ]; then
        rm -f "$MACHINE_INFECTED_FLAG"
        echo "[script] Esta máquina: modo infectado DESACTIVADO."
        echo "[script] Los próximos procesos que se creen serán NORMALES."
    else
        touch "$MACHINE_INFECTED_FLAG"
        echo "[script] Esta máquina: modo infectado ACTIVADO."
        echo "[script] Los próximos procesos que se creen serán INFECTADOS."
        echo "[script] Ejecuta './script.sh <MAQUINA> <N>' para crear los procesos infectados."
    fi

    # Notificar a los procesos ya activos (si los hay)
    local COUNT=0
    for PFILE in "$PIDS_DIR"/*.pid; do
        [ -f "$PFILE" ] || continue
        local PID
        PID=$(cat "$PFILE")
        kill -USR1 "$PID" 2>/dev/null && COUNT=$((COUNT + 1)) || true
    done
    [ "$COUNT" -gt 0 ] && echo "[script] Señal enviada a $COUNT proceso(s) ya activo(s)."
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
