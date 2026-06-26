package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"tarea3/internal/process"
)

// Mapa fijo de IPs por máquina virtual.
var machineIPs = map[int]string{
	1: "10.10.28.35",
	2: "10.10.28.36",
	3: "10.10.28.37",
}

// totalProcesosPorMaquina define cuántos procesos lógicos existen en CADA
// máquina. Se lee desde el archivo .num_procesos que script.sh genera al
// iniciar, para evitar que quede hardcodeado y se intenten conectar procesos
// inexistentes.
var totalProcesosPorMaquina = readNumProcesos()

// basePort calcula el puerto gRPC de un proceso a partir de su máquina y su
// ID (ej: M1P1 -> 8001, M2P3 -> 8103).
// Entrada: machineID, processID. Salida: puerto int.
func basePort(machineID, processID int) int {
	return 8000 + (machineID-1)*100 + processID
}

// buildAllPeers construye la lista de TODOS los demás procesos del sistema
// (cualquier máquina, cualquier ID), excluyendo a este mismo proceso. Ya no
// existe el concepto de "réplica paralela": cada proceso conoce a todos.
// Entrada: myMachine, myProcess. Salida: slice de process.PeerAddr.
func buildAllPeers(myMachine, myProcess int) []process.PeerAddr {
	var peers []process.PeerAddr
	for mID, ip := range machineIPs {
		for pID := 1; pID <= totalProcesosPorMaquina; pID++ {
			if mID == myMachine && pID == myProcess {
				continue
			}
			peers = append(peers, process.PeerAddr{
				MachineID: mID,
				ProcessID: pID,
				Addr:      fmt.Sprintf("%s:%d", ip, basePort(mID, pID)),
			})
		}
	}
	return peers
}

// readNumProcesos lee cuántos procesos se configuraron por máquina desde el
// archivo .num_procesos (escrito por script.sh al iniciar). Si no existe,
// devuelve 1 como valor conservador para no conectarse a procesos inexistentes.
// Entrada: ninguna. Salida: int con la cantidad de procesos por máquina.
func readNumProcesos() int {
	data, err := os.ReadFile(".num_procesos")
	if err != nil {
		return 1
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || n < 1 {
		return 1
	}
	return n
}

// findInstructionFile busca en instrucciones/ el archivo terminado en
// _<ID>.txt, o usa el fallback proceso_<ID>.txt.
// Entrada: processID. Salida: path del archivo, o el fallback aunque no exista.
func findInstructionFile(processID int) string {
	suffix := fmt.Sprintf("_%d.txt", processID)
	matches, _ := filepath.Glob("instrucciones/*.txt")
	for _, m := range matches {
		if strings.HasSuffix(m, suffix) {
			return m
		}
	}
	return fmt.Sprintf("instrucciones/proceso_%d.txt", processID)
}

// attachInfectionSignal registra el manejador de SIGUSR1 para que el
// proceso vuelva a leer el flag de infección de la máquina en caliente.
// Entrada: *process.Process. Salida: ninguna.
func attachInfectionSignal(p *process.Process) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGUSR1)
	go func() {
		for range sigCh {
			p.RefreshInfectedFromDisk()
		}
	}()
}

// runProcess construye y ejecuta un Process nuevo (ciclo de vida normal).
// Entrada: machineID, processID. Salida: ninguna (bloquea).
func runProcess(machineID, processID int) {
	rand.Seed(time.Now().UnixNano() + int64(processID) + int64(machineID)*1000)

	instrFile := findInstructionFile(processID)
	peers := buildAllPeers(machineID, processID)
	port := basePort(machineID, processID)

	p := process.New(machineID, processID, instrFile, peers, port)
	attachInfectionSignal(p)

	p.Run()
	select {} // mantener vivo el servidor gRPC para seguir respondiendo a otros procesos
}

// runRecover construye un Process y ejecuta el flujo de recuperación por
// quorum (sin re-ejecutar instrucciones).
// Entrada: machineID, processID. Salida: ninguna (bloquea, o termina con
// log.Fatal si no se alcanza el quorum).
func runRecover(machineID, processID int) {
	rand.Seed(time.Now().UnixNano() + int64(processID) + int64(machineID)*1000)

	instrFile := findInstructionFile(processID)
	peers := buildAllPeers(machineID, processID)
	port := basePort(machineID, processID)

	p := process.New(machineID, processID, instrFile, peers, port)
	attachInfectionSignal(p)

	p.Recover()
	select {}
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Uso: ./expendedora <MAQUINA> PROC <ID>")
		fmt.Println("     ./expendedora <MAQUINA> RESTAURAR <ID>")
		fmt.Println("     ./expendedora <MAQUINA> ESTADO <ID>")
		os.Exit(1)
	}

	machineID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("NUMERO_DE_MAQUINA inválido: %s", os.Args[1])
	}

	switch strings.ToUpper(os.Args[2]) {
	case "PROC":
		if len(os.Args) < 4 {
			log.Fatal("Uso: ./expendedora <MAQUINA> PROC <ID>")
		}
		id, _ := strconv.Atoi(os.Args[3])
		runProcess(machineID, id)

	case "RESTAURAR":
		if len(os.Args) < 4 {
			log.Fatal("Uso: ./expendedora <MAQUINA> RESTAURAR <ID>")
		}
		id, _ := strconv.Atoi(os.Args[3])
		runRecover(machineID, id)

	case "ESTADO":
		if len(os.Args) < 4 {
			log.Fatal("Uso: ./expendedora <MAQUINA> ESTADO <ID>")
		}
		id, _ := strconv.Atoi(os.Args[3])
		printStatusFromLogs(machineID, id)

	default:
		log.Fatalf("Subcomando inválido: %s (use PROC, RESTAURAR o ESTADO)", os.Args[2])
	}
}

// printStatusFromLogs muestra el inventario y vetos actuales de un proceso
// leyendo directamente los archivos que el proceso real escribe en disco,
// sin crear una instancia nueva que choque con el puerto ya ocupado.
// Entrada: machineID, processID. Salida: ninguna (imprime a stdout).
func printStatusFromLogs(machineID, processID int) {
	fmt.Printf("=== Estado M%dP%d ===\n", machineID, processID)

	invPath := fmt.Sprintf("logs/inventario_propio_M%dP%d.json", machineID, processID)
	fmt.Println("Inventario:")
	if data, err := os.ReadFile(invPath); err == nil {
		fmt.Println(string(data))
	} else {
		fmt.Println("  (sin datos; el proceso aún no ha persistido su inventario)")
	}

	vetoPath := fmt.Sprintf("logs/vetos_M%dP%d.log", machineID, processID)
	fmt.Println("Vetos:")
	if data, err := os.ReadFile(vetoPath); err == nil {
		content := strings.TrimSpace(string(data))
		if content == "" {
			fmt.Println("  (ninguno)")
		} else {
			fmt.Println(content)
		}
	} else {
		fmt.Println("  (sin datos)")
	}
}
