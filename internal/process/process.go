package process

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"tarea3/internal/httpapi"
	"tarea3/internal/protocol"
	"tarea3/internal/store"
	"time"
)

// ReplicaConfig identifica a una réplica paralela: mismo ProcessID (mismo
// rol/expendedora), pero corriendo en otra máquina virtual.
type ReplicaConfig struct {
	MachineID int
	BaseURL   string // ej: http://10.10.28.36:8101
}

// Process representa una expendedora: tiene su propio Store (inventario y
// vetos persistidos en disco) y se comunica con sus réplicas paralelas
// (mismo ProcessID en las otras máquinas) vía REST.
type Process struct {
	MachineID    int
	ProcessID    int
	instructFile string

	st       *store.Store
	server   *httpapi.Server
	replicas []ReplicaConfig

	mu       sync.Mutex
	infected bool // modo "infectado": reporta datos falsos en el state-report final

	logPath     string
	vetoLogPath string
}

// New crea un Process con su Store y servidor HTTP propios, pero todavía sin
// inventario cargado (eso ocurre en Run()).
// Entrada: machineID, processID, archivo de instrucciones, réplicas paralelas, puerto.
// Salida: *Process listo para Run().
func New(machineID, processID int, instructFile string, replicas []ReplicaConfig, port int) *Process {
	invPath := fmt.Sprintf("logs/inventario_propio_M%dP%d.json", machineID, processID)
	p := &Process{
		MachineID:    machineID,
		ProcessID:    processID,
		instructFile: instructFile,
		st:           store.New(invPath),
		server:       httpapi.NewServer(port),
		replicas:     replicas,
		logPath:      fmt.Sprintf("logs/inventario_M%dP%d.log", machineID, processID),
		vetoLogPath:  fmt.Sprintf("logs/vetos_M%dP%d.log", machineID, processID),
	}
	return p
}

// Run ejecuta el ciclo de vida completo y bloquea hasta terminar:
//  1. Levanta el servidor REST propio.
//  2. Revisa si existe el flag de infección en disco (creado por
//     `script.sh INFECTAR` antes de levantar el proceso) y, si existe,
//     activa el modo infectado desde el arranque.
//  3. Determina si este proceso es el LÍDER del grupo de réplicas (el de
//     menor MachineID). Si lo es, sortea un inventario plantilla al azar y
//     lo copia como propio; si no lo es, espera a recibir ESE MISMO
//     inventario desde el líder vía POST /inventory, garantizando que las
//     3 réplicas arrancan con datos idénticos.
//  4. Espera a que las réplicas paralelas estén disponibles (solo aplica al
//     líder, que es quien debe esperar antes de poder enviarles datos).
//  5. El líder distribuye su inventario inicial a las réplicas.
//  6. Ejecuta sus instrucciones locales, generando logs.
//  7. Vuelve a revisar el flag de infección (por si se activó vía señal
//     SIGUSR1 durante la ejecución) antes de reportar su estado final.
//  8. Reporta su estado final a las réplicas y espera los reportes de ellas.
//  9. Determina el resultado por quórum 2/3 y lo deja escrito en disco.
//
// Entrada: ninguna. Salida: ninguna.
func (p *Process) Run() {
	p.server.Start()

	if p.infectionFlagExists() {
		p.SetInfected(true)
	}

	if p.isLeader() {
		log.Printf("[P%d] Este proceso es el LÍDER del grupo (M%d): sorteará el inventario inicial.", p.ProcessID, p.MachineID)
		if err := p.pickAndLoadInventory(); err != nil {
			log.Fatalf("[P%d] No se pudo cargar inventario: %v", p.ProcessID, err)
		}
		p.waitForReplicas()
		p.broadcastInitialInventory()
	} else {
		log.Printf("[P%d] Esperando inventario inicial del líder del grupo...", p.ProcessID)
		if !p.waitForInitialInventoryFromLeader(10 * time.Second) {
			log.Printf("[P%d] ADVERTENCIA: no se recibió inventario del líder a tiempo; "+
				"se sortea uno local como respaldo (esto puede causar inconsistencias).", p.ProcessID)
			if err := p.pickAndLoadInventory(); err != nil {
				log.Fatalf("[P%d] No se pudo cargar inventario: %v", p.ProcessID, err)
			}
		}
	}

	log.Printf("[P%d] Ejecutando instrucciones...", p.ProcessID)
	p.runInstructions()

	// Revisión final del flag justo antes de reportar: cubre el caso de un
	// ciclo de instrucciones muy rápido donde una señal SIGUSR1 enviada
	// "a tiempo" podría procesarse después de este punto por scheduling.
	if p.infectionFlagExists() {
		p.SetInfected(true)
	}

	log.Printf("[P%d] Instrucciones finalizadas. Iniciando fase de comparación final.", p.ProcessID)
	p.reportAndCompareFinalState()
}

// isLeader determina si este proceso es el encargado de sortear el
// inventario inicial para todo el grupo de réplicas: por convención, es la
// réplica corriendo en la máquina con el MachineID más bajo del grupo
// (considerando también su propio MachineID).
// Entrada: ninguna. Salida: bool.
func (p *Process) isLeader() bool {
	for _, r := range p.replicas {
		if r.MachineID < p.MachineID {
			return false
		}
	}
	return true
}

// waitForInitialInventoryFromLeader bloquea hasta recibir un InventoryPayload
// por el canal del servidor (proveniente del líder del grupo) o hasta agotar
// el timeout indicado.
// Entrada: timeout máximo de espera. Salida: true si se recibió y aplicó el
// inventario del líder; false si se agotó el tiempo sin recibir nada.
func (p *Process) waitForInitialInventoryFromLeader(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case payload := <-p.server.InventoryCh:
			log.Printf("[P%d] Inventario inicial recibido del líder (M%d). Aplicando como propio.", p.ProcessID, payload.MachineID)
			p.st.SetInventory(payload.Inventory)
			return true
		case <-time.After(100 * time.Millisecond):
		}
	}
	return false
}

// infectionFlagPath retorna la ruta del archivo flag que indica modo
// infectado para este proceso específico.
// Entrada: ninguna. Salida: path string.
func (p *Process) infectionFlagPath() string {
	return fmt.Sprintf(".infectado_M%dP%d", p.MachineID, p.ProcessID)
}

// infectionFlagExists revisa si existe el archivo flag de infección para
// este proceso en el directorio de trabajo actual.
// Entrada: ninguna. Salida: bool.
func (p *Process) infectionFlagExists() bool {
	_, err := os.Stat(p.infectionFlagPath())
	return err == nil
}

// RefreshInfectedFromDisk vuelve a leer el archivo flag de infección y
// actualiza el estado interno en consecuencia (presente = infectado,
// ausente = no infectado). Se usa como reacción a SIGUSR1, manteniendo
// disco y memoria sincronizados.
// Entrada: ninguna. Salida: ninguna.
func (p *Process) RefreshInfectedFromDisk() {
	p.SetInfected(p.infectionFlagExists())
}

// pickAndLoadInventory elige aleatoriamente un archivo de /inventario y lo
// copia como el inventario propio de este proceso (nunca usa el original
// directamente).
// Entrada: ninguna. Salida: error si no hay plantillas disponibles.
func (p *Process) pickAndLoadInventory() error {
	matches, err := filepath.Glob("inventario/*.json")
	if err != nil || len(matches) == 0 {
		return fmt.Errorf("no hay archivos de inventario en /inventario")
	}
	chosen := matches[rand.Intn(len(matches))]
	log.Printf("[P%d] Plantilla de inventario elegida: %s", p.ProcessID, chosen)
	return p.st.LoadFromTemplate(chosen)
}

// waitForReplicas espera hasta 2 segundos a que cada réplica paralela
// responda su endpoint /health, según lo especificado en el enunciado.
// Entrada: ninguna. Salida: ninguna (continúa de todos modos tras el timeout).
func (p *Process) waitForReplicas() {
	var wg sync.WaitGroup
	for _, r := range p.replicas {
		wg.Add(1)
		go func(r ReplicaConfig) {
			defer wg.Done()
			c := httpapi.NewClient(r.BaseURL)
			ok := c.WaitHealthy(20, 100*time.Millisecond) // 20*100ms = 2s máx
			if !ok {
				log.Printf("[P%d] Advertencia: réplica M%dP%d (%s) no respondió a tiempo", p.ProcessID, r.MachineID, p.ProcessID, r.BaseURL)
			}
		}(r)
	}
	wg.Wait()
	log.Printf("[P%d] Espera de réplicas finalizada.", p.ProcessID)
}

// broadcastInitialInventory envía el inventario recién sorteado a todas las
// réplicas paralelas vía POST /inventory, con reintentos por si alguna
// réplica no estaba lista aún al primer intento.
// Entrada: ninguna. Salida: ninguna (loguea errores de envío, no es fatal).
func (p *Process) broadcastInitialInventory() {
	payload := protocol.InventoryPayload{
		MachineID: p.MachineID,
		ProcessID: p.ProcessID,
		Inventory: p.st.GetInventory(),
	}
	for _, r := range p.replicas {
		go func(r ReplicaConfig) {
			c := httpapi.NewClient(r.BaseURL)
			for attempt := 1; attempt <= 10; attempt++ {
				if err := c.SendInventory(payload); err == nil {
					log.Printf("[P%d] Inventario inicial enviado a M%dP%d.", p.ProcessID, r.MachineID, p.ProcessID)
					return
				}
				log.Printf("[P%d] Reintento %d/10 enviando inventario a M%d...", p.ProcessID, attempt, r.MachineID)
				time.Sleep(500 * time.Millisecond)
			}
			log.Printf("[P%d] ERROR: no se pudo enviar inventario inicial a M%d tras 10 intentos.", p.ProcessID, r.MachineID)
		}(r)
	}
}

// runInstructions lee y ejecuta cada línea del archivo de instrucciones
// asignado a este proceso, generando una entrada de log por cada una.
// Trunca los logs existentes al inicio para que cada ejecución nueva
// sobreescriba la anterior en lugar de acumular.
// Entrada: ninguna. Salida: ninguna.
func (p *Process) runInstructions() {
	// Truncar logs al inicio de cada nueva ejecución
	os.MkdirAll("logs", 0755)
	os.Truncate(p.logPath, 0)
	os.Truncate(p.vetoLogPath, 0)

	f, err := os.Open(p.instructFile)
	if err != nil {
		log.Printf("[P%d] No se pudo abrir %s: %v", p.ProcessID, p.instructFile, err)
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		result := p.execInstruction(line)
		p.appendLog(line, result)
	}
	p.writeVetoLog()
}

// execInstruction parsea y ejecuta una instrucción de texto (VETAR, COMPRAR
// o PERDONAR) sobre el Store local de este proceso. La ejecución es siempre
// normal, independientemente del modo infectado: la corrupción ocurre solo
// al momento de reportar el estado final a las réplicas.
// Entrada: línea de instrucción. Salida: resultado a loguear.
func (p *Process) execInstruction(line string) string {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return ""
	}
	cmd := strings.ToUpper(parts[0])

	switch cmd {
	case "VETAR":
		if len(parts) < 2 {
			return ""
		}
		persona := strings.Join(parts[1:], " ")
		p.st.Veto(persona)
		return ""

	case "COMPRAR":
		if len(parts) < 4 {
			return "NO VALIDO"
		}
		cantidad := 0
		fmt.Sscanf(parts[len(parts)-1], "%d", &cantidad)
		producto := parts[len(parts)-2]
		persona := strings.Join(parts[1:len(parts)-2], " ")
		result := p.st.Buy(persona, producto, cantidad)
		p.st.DecrementVetos()
		return result

	case "PERDONAR":
		if len(parts) < 2 {
			return ""
		}
		persona := strings.Join(parts[1:], " ")
		p.st.Pardon(persona)
		return ""
	}
	return ""
}

// reportAndCompareFinalState envía el estado final (inventario + vetos) de
// este proceso a sus réplicas paralelas, espera sus reportes, y determina el
// resultado final por quórum 2/3. Si no se alcanza el quórum, se considera
// que el sistema fue "infectado" y se reporta el error correspondiente.
// Entrada: ninguna. Salida: ninguna (persiste el resultado a logs/resultado_*).
func (p *Process) reportAndCompareFinalState() {
	realPayload := protocol.StatePayload{
		MachineID: p.MachineID,
		ProcessID: p.ProcessID,
		Inventory: p.st.GetInventory(),
		Vetos:     p.st.GetVetos(),
		Infected:  p.IsInfected(),
	}

	// Si este proceso está infectado, TANTO lo que envía a las réplicas COMO
	// lo que aporta a su propio cálculo de quórum es el estado falso: un
	// proceso bizantino miente de forma consistente, no solo hacia afuera.
	outgoing := realPayload
	if p.IsInfected() {
		outgoing = p.corruptedPayload(realPayload)
		log.Printf("[P%d] MODO INFECTADO: enviando estado falso a las réplicas", p.ProcessID)
	}

	for _, r := range p.replicas {
		c := httpapi.NewClient(r.BaseURL)
		if err := c.SendStateReport(outgoing); err != nil {
			log.Printf("[P%d] Error enviando state-report a M%d: %v", p.ProcessID, r.MachineID, err)
		}
	}

	// Recolectar reportes de las réplicas durante una ventana de tiempo.
	received := []protocol.StatePayload{outgoing}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && len(received) < len(p.replicas)+1 {
		select {
		case sp := <-p.server.StateCh:
			received = append(received, sp)
		case <-time.After(100 * time.Millisecond):
		}
	}

	p.resolveQuorum(received)
}

// corruptedPayload genera una versión falsa/corrupta del estado, simulando
// un proceso bizantino en modo "infectado".
// Entrada: el payload real. Salida: una copia con datos inventados.
func (p *Process) corruptedPayload(real protocol.StatePayload) protocol.StatePayload {
	fake := protocol.StatePayload{
		MachineID: real.MachineID,
		ProcessID: real.ProcessID,
		Infected:  true,
	}
	for _, item := range real.Inventory {
		fake.Inventory = append(fake.Inventory, protocol.Item{
			Nombre:   item.Nombre,
			Cantidad: item.Cantidad + 9999 + rand.Intn(500), // cantidad inventada
		})
	}
	fake.Vetos = append(fake.Vetos, protocol.VetoEntry{Persona: "splicer_fantasma", Counter: 99})
	return fake
}

// resolveQuorum determina si ≥2/3 del TOTAL de réplicas del grupo (siempre
// len(p.replicas)+1, es decir 3 en esta arquitectura) coinciden en su
// inventario y vetos. El denominador es siempre el total fijo, no la
// cantidad de reportes recibidos, para que la ausencia de un reporte cuente
// como discrepancia y no se infle artificialmente el porcentaje de acuerdo.
// Entrada: slice de StatePayload recolectados (incluye el propio). Salida: ninguna.
func (p *Process) resolveQuorum(received []protocol.StatePayload) {
	total := len(p.replicas) + 1 // siempre 3 en esta arquitectura

	counts := make(map[string]int)
	reps := make(map[string]protocol.StatePayload)
	for _, sp := range received {
		key := stateKey(sp)
		counts[key]++
		reps[key] = sp
	}

	var bestKey string
	best := 0
	for k, c := range counts {
		if c > best {
			best = c
			bestKey = k
		}
	}

	threshold := float64(total) * 2.0 / 3.0
	resultPath := fmt.Sprintf("logs/resultado_M%dP%d.txt", p.MachineID, p.ProcessID)

	if float64(best) >= threshold {
		winner := reps[bestKey]
		msg := fmt.Sprintf(
			"QUORUM ALCANZADO (%d/%d). Resultado validado:\nInventario: %+v\nVetos: %+v\n",
			best, total, winner.Inventory, winner.Vetos,
		)
		log.Printf("[P%d] %s", p.ProcessID, strings.ReplaceAll(msg, "\n", " "))
		_ = os.WriteFile(resultPath, []byte(msg), 0644)
	} else {
		msg := fmt.Sprintf(
			"ERROR DE INTEGRIDAD (%d/%d, se requiere >= 2/3): "+
				"Todas las máquinas han sido infectadas, por favor revíseme.\n"+
				"Reportes recibidos: %d de %d\n",
			best, total, len(received), total,
		)
		log.Printf("[P%d] %s", p.ProcessID, strings.ReplaceAll(msg, "\n", " "))
		_ = os.WriteFile(resultPath, []byte(msg), 0644)
	}
}

// stateKey serializa el inventario y vetos de un StatePayload a una clave
// comparable, ignorando el campo Infected (que es metadato, no parte del
// estado a comparar) y MachineID/ProcessID (distintas réplicas del MISMO
// proceso lógico deben compararse solo por su contenido).
// Entrada: StatePayload. Salida: string clave determinística.
func stateKey(sp protocol.StatePayload) string {
	var b strings.Builder
	items := append([]protocol.Item{}, sp.Inventory...)
	sortItems(items)
	for _, it := range items {
		fmt.Fprintf(&b, "%s=%d;", it.Nombre, it.Cantidad)
	}
	b.WriteString("|")
	vetos := append([]protocol.VetoEntry{}, sp.Vetos...)
	sortVetos(vetos)
	for _, v := range vetos {
		fmt.Fprintf(&b, "%s=%d;", v.Persona, v.Counter)
	}
	return b.String()
}

// sortItems ordena un slice de Items por nombre, in-place, para comparación determinística.
// Entrada: slice de Items. Salida: ninguna (modifica in-place).
func sortItems(items []protocol.Item) {
	for i := 1; i < len(items); i++ {
		for j := i; j > 0 && items[j-1].Nombre > items[j].Nombre; j-- {
			items[j-1], items[j] = items[j], items[j-1]
		}
	}
}

// sortVetos ordena un slice de VetoEntry por persona, in-place, para comparación determinística.
// Entrada: slice de VetoEntry. Salida: ninguna (modifica in-place).
func sortVetos(vetos []protocol.VetoEntry) {
	for i := 1; i < len(vetos); i++ {
		for j := i; j > 0 && vetos[j-1].Persona > vetos[j].Persona; j-- {
			vetos[j-1], vetos[j] = vetos[j], vetos[j-1]
		}
	}
}

// appendLog escribe una línea en el log de instrucciones del proceso.
// Entrada: instrucción original, resultado obtenido. Salida: ninguna.
func (p *Process) appendLog(instruction, result string) {
	os.MkdirAll("logs", 0755)
	f, err := os.OpenFile(p.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	line := instruction
	if result != "" {
		line += " | " + result
	}
	fmt.Fprintln(f, line)
}

// writeVetoLog escribe el estado final de vetos en su archivo de log.
// Entrada: ninguna. Salida: ninguna.
func (p *Process) writeVetoLog() {
	os.MkdirAll("logs", 0755)
	vetos := p.st.GetVetos()
	f, err := os.Create(p.vetoLogPath)
	if err != nil {
		return
	}
	defer f.Close()
	for _, v := range vetos {
		fmt.Fprintf(f, "VETADO %s %d\n", v.Persona, v.Counter)
	}
}

// Recover en este modelo de arquitectura no reconstruye estado a partir de
// mensajes en vivo: un proceso restaurado simplemente vuelve a ejecutar su
// ciclo de vida completo desde cero (Run), lo cual incluye sortear un nuevo
// inventario, distribuirlo, ejecutar sus instrucciones y comparar al final.
// Esta función queda como alias explícito para mantener la semántica del
// comando RESTAURAR del enunciado.
// Entrada: ninguna. Salida: ninguna (bloquea igual que Run).
func (p *Process) Recover() {
	log.Printf("[P%d] RESTAURAR: reiniciando ciclo de vida completo.", p.ProcessID)
	p.Run()
}

// SetInfected activa o desactiva el modo "infectado" de este proceso: al
// activarse, el siguiente reporte de estado final que se envíe a las
// réplicas paralelas contendrá datos falsos/inventados.
// Entrada: bool. Salida: ninguna.
func (p *Process) SetInfected(v bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.infected = v
	log.Printf("[P%d] Modo infectado: %v", p.ProcessID, v)
}

// IsInfected informa si el proceso está actualmente en modo infectado.
// Entrada: ninguna. Salida: bool.
func (p *Process) IsInfected() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.infected
}

// Status imprime el inventario y vetos actuales por stdout.
// Entrada: ninguna. Salida: ninguna (escribe a stdout).
func (p *Process) Status() {
	inv := p.st.GetInventory()
	vetos := p.st.GetVetos()
	fmt.Printf("=== Estado M%dP%d ===\n", p.MachineID, p.ProcessID)
	fmt.Println("Inventario:")
	for _, item := range inv {
		fmt.Printf("  %s: %d\n", item.Nombre, item.Cantidad)
	}
	fmt.Println("Vetos:")
	for _, v := range vetos {
		fmt.Printf("  %s (counter=%d)\n", v.Persona, v.Counter)
	}
}
