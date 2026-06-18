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

// basePort calcula el puerto REST de un proceso a partir de su máquina y su
// ID (ej: M1P1 -> 8001, M2P3 -> 8103).
// Entrada: machineID, processID. Salida: puerto int.
func basePort(machineID, processID int) int {
	return 8000 + (machineID-1)*100 + processID
}

// buildReplicas construye la lista de réplicas paralelas para un proceso:
// el MISMO ProcessID corriendo en las OTRAS máquinas (no incluye la propia).
// Entrada: myMachine, processID, mapa de IPs. Salida: slice de ReplicaConfig.
func buildReplicas(myMachine, processID int) []process.ReplicaConfig {
	var replicas []process.ReplicaConfig
	for mID, ip := range machineIPs {
		if mID == myMachine {
			continue
		}
		replicas = append(replicas, process.ReplicaConfig{
			MachineID: mID,
			BaseURL:   fmt.Sprintf("http://%s:%d", ip, basePort(mID, processID)),
		})
	}
	return replicas
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

// runProcess construye y ejecuta un Process individual, manejando SIGUSR1
// para el toggle de modo "infectado".
// Entrada: machineID, processID. Salida: ninguna (bloquea).
func runProcess(machineID, processID int) {
	rand.Seed(time.Now().UnixNano() + int64(processID) + int64(machineID)*1000)

	instrFile := findInstructionFile(processID)
	replicas := buildReplicas(machineID, processID)
	port := basePort(machineID, processID)

	p := process.New(machineID, processID, instrFile, replicas, port)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGUSR1)
	go func() {
		for range sigCh {
			p.RefreshInfectedFromDisk()
		}
	}()

	p.Run()
	select {} // mantener vivo el servidor REST tras terminar el ciclo, para responder a otras réplicas
}

// runRecover construye un Process y ejecuta el ciclo RESTAURAR (equivalente
// a Run, pero invocado explícitamente vía el subcomando del enunciado).
// Entrada: machineID, processID. Salida: ninguna (bloquea).
func runRecover(machineID, processID int) {
	rand.Seed(time.Now().UnixNano() + int64(processID) + int64(machineID)*1000)

	instrFile := findInstructionFile(processID)
	replicas := buildReplicas(machineID, processID)
	port := basePort(machineID, processID)

	p := process.New(machineID, processID, instrFile, replicas, port)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGUSR1)
	go func() {
		for range sigCh {
			p.RefreshInfectedFromDisk()
		}
	}()

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

	resultPath := fmt.Sprintf("logs/resultado_M%dP%d.txt", machineID, processID)
	if data, err := os.ReadFile(resultPath); err == nil {
		fmt.Println("Resultado de comparación final:")
		fmt.Println(string(data))
	}
}
