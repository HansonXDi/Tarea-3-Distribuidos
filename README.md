# Tarea 3 – INF-343 Sistemas Distribuidos: ¿Y El Pensador?

## Integrantes

| Nombre | Apellido | Rol |
|--------|----------|-----|
| — | — | — |
| — | — | — |
| — | — | — |

> Completar con los datos de cada integrante.

---

## ⚠️ Paso obligatorio antes de compilar: generar código gRPC

Este proyecto usa **gRPC** para la comunicación entre procesos. El archivo `proto/expendedora.proto` define los mensajes y el servicio, pero el código Go correspondiente (`internal/grpcapi/expendedora.pb.go` y `expendedora_grpc.pb.go`) **debe generarse en tu máquina** ejecutando `protoc`, ya que el entorno donde se escribió este proyecto no tuvo acceso a internet para descargar las herramientas de generación ni para compilar/probar el resultado.

### 1. Instalar herramientas (una sola vez)

```bash
sudo apt-get update && sudo apt-get install -y protobuf-compiler golang-go

go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

export PATH="$PATH:$(go env GOPATH)/bin"
# Agrega esa línea export a tu ~/.bashrc para no repetirla cada sesión
```

### 2. Generar el código

```bash
cd tarea3
chmod +x generate_proto.sh
./generate_proto.sh
```

Esto crea `internal/grpcapi/expendedora.pb.go` y `internal/grpcapi/expendedora_grpc.pb.go`.

### 3. Resolver dependencias y compilar

```bash
go mod tidy
go build -o expendedora ./cmd/main.go
```

**Repite los pasos 1–3 en CADA una de las 3 VMs** antes de usar `script.sh`.

Si modificas `proto/expendedora.proto`, vuelve a ejecutar `./generate_proto.sh` y `go mod tidy`.

---

## Rol de cada máquina virtual

| Máquina | IP           | Rol |
|---------|--------------|-----|
| 1       | 10.10.28.35  | Contenedor físico que aloja N procesos expendedora. No hay jerarquía: solo es el lugar donde corren. |
| 2       | 10.10.28.36  | Igual que máquina 1. |
| 3       | 10.10.28.37  | Igual que máquinas 1 y 2. |

**Importante:** las máquinas virtuales ya NO actúan como nodos de replicación ni agrupan procesos en "réplicas paralelas". Cada proceso (`M<máquina>P<id>`) es una entidad independiente con su propio inventario. Las VMs son solo el contenedor físico/red donde corren.

---

## Arquitectura de comunicación: gRPC

Cada proceso expone un servicio gRPC (`ExpendedoraService`, definido en `proto/expendedora.proto`) en el puerto `8000 + (machineID-1)*100 + processID`.

| RPC | Descripción |
|-----|-------------|
| `Health` | Chequeo de disponibilidad. |
| `PushInventory` | Un proceso informa su inventario y vetos actuales a OTRO proceso, para que este último actualice su copia de solo lectura de aquel. |
| `QueryInventory` | Un proceso (en recuperación) pregunta a otro qué copia tiene almacenada de un proceso específico. |

**Justificación:** gRPC sobre HTTP/2 ofrece tipado fuerte vía Protocol Buffers, multiplexado eficiente de conexiones, y es el estándar de la industria para comunicación entre microservicios/procesos distribuidos, cumpliendo el requisito explícito de la tarea.

---

## Modelo de datos: sin líder, sin réplicas

Cada proceso `M<m>P<p>` mantiene dos estructuras:

1. **Su propio inventario y vetos** (`internal/store/store.go`), persistidos en `logs/inventario_propio_M<m>P<p>.json`. Solo este proceso puede modificarlos (ejecutando sus propias instrucciones).
2. **Una tabla de copias de solo lectura** (`internal/store/peertable.go`, `PeerTable`) con el último inventario y vetos conocidos de **todos los demás procesos del sistema** (de su misma máquina y de las otras dos). Esta tabla se llena pasivamente, recibiendo `PushInventory` de cada proceso.

No existe el concepto de "líder" ni de "grupo de réplicas paralelas": cada uno de los N procesos por máquina es independiente, y cada uno conoce (de forma eventual, vía push) el estado de TODOS los demás procesos del sistema completo.

---

## Ciclo de vida de un proceso nuevo

1. Levanta su servidor gRPC.
2. Revisa si su máquina está marcada como infectada (`.infectado`).
3. Sortea aleatoriamente un archivo de `/inventario` y lo **copia** (nunca lo referencia) como su inventario propio.
4. Espera hasta 2 segundos a que cada uno de los demás procesos del sistema responda `Health`.
5. Hace `PushInventory` de su inventario inicial a **todos** los demás procesos del sistema (con reintentos).
6. Ejecuta sus instrucciones (`VETAR`/`COMPRAR`/`PERDONAR`) una por una. **Cada vez que una instrucción modifica su inventario o vetos**, vuelve a hacer `PushInventory` a todos los demás procesos, para que sus copias se mantengan al día.
7. Al terminar sus instrucciones, queda activo indefinidamente, respondiendo `Health`/`PushInventory`/`QueryInventory` de otros procesos.

---

## Recuperación de un proceso caído (RESTAURAR)

Cuando un proceso muere y se restaura, **no vuelve a ejecutar su archivo de instrucciones**. En su lugar:

1. Levanta su servidor gRPC de nuevo.
2. Consulta, vía `QueryInventory`, a **todos** los demás procesos del sistema, preguntando qué copia tienen almacenada de él mismo (`M<m>P<p>`).
3. Agrupa las respuestas recibidas y determina cuál combinación de inventario+vetos se repite más veces.
4. Si esa combinación mayoritaria alcanza un **quorum de al menos 2/3** del total de procesos consultados, la adopta como su estado real, hace `PushInventory` a todos para informar su recuperación, y queda activo.
5. Si **no se alcanza el quorum**, el proceso no puede recuperarse y se imprime por consola:
   > *Todas las máquinas han sido infectadas, por favor revíseme.*

---

## Modo "Infectado" (proceso/máquina bizantino)

`./script.sh INFECTAR` marca **toda la máquina actual** como infectada (crea el flag `.infectado`). Mientras una máquina está infectada, **todos los procesos alojados en ella**, sin excepción, responden con **datos falsos** ante cualquier `QueryInventory` que reciban — sin importar quién pregunte (un proceso de otra máquina, o incluso un proceso de la misma máquina infectada).

La infección **no afecta lo que el proceso ejecuta ni lo que registra de otros** (sus `PushInventory` salientes siguen siendo reales, y lo que recibe vía `PushInventory` de otros lo guarda fielmente). Solo se corrompen las **respuestas a consultas** (`QueryInventory`), que es precisamente el mecanismo que se usa durante una recuperación — de ahí que un atacante infectando suficientes máquinas pueda impedir que un proceso caído se recupere correctamente.

```bash
./script.sh INFECTAR     # activa/desactiva (toggle) la infección de esta máquina
```

Los procesos ya activos reciben `SIGUSR1` y vuelven a leer el flag en caliente.

---

## Estructura del repositorio

```
.
├── proto/
│   └── expendedora.proto         # Definición del servicio y mensajes gRPC
├── generate_proto.sh             # Genera internal/grpcapi/*.pb.go a partir del .proto
├── cmd/
│   └── main.go                   # Punto de entrada: parsea argumentos y arranca el proceso
├── internal/
│   ├── process/
│   │   └── process.go            # Ciclo de vida: sorteo, broadcast, ejecución, recuperación por quorum
│   ├── store/
│   │   ├── store.go               # Inventario y vetos PROPIOS del proceso (persistidos en JSON)
│   │   └── peertable.go           # Copias de solo lectura de TODOS los demás procesos del sistema
│   └── grpcapi/
│       ├── server.go               # Implementación del servicio gRPC (Health/PushInventory/QueryInventory)
│       ├── client.go                # Cliente gRPC para llamar a otros procesos
│       ├── expendedora.pb.go        # GENERADO por protoc (mensajes) — no existe hasta ejecutar generate_proto.sh
│       └── expendedora_grpc.pb.go   # GENERADO por protoc (servicio) — no existe hasta ejecutar generate_proto.sh
├── instrucciones/                  # Archivos proceso_<ID>.txt (7 de ejemplo)
├── inventario/                     # Plantillas de inventario (20 de ejemplo)
├── logs/                           # Logs, inventarios propios, stdout por proceso
├── script.sh                       # Script principal de control
├── iniciar.sh                      # Script auxiliar para ESTADO
├── go.mod
└── README.md
```

---

## Instrucciones de uso

### 1. Prerequisitos (en cada VM)

Ver la sección **"Paso obligatorio antes de compilar"** al inicio de este documento.

```bash
git clone <URL_PRIVADA> tarea3
cd tarea3
chmod +x script.sh iniciar.sh generate_proto.sh
./generate_proto.sh
go mod tidy
```

### 2. Compilar

```bash
go build -o expendedora ./cmd/main.go
```

`script.sh` compila automáticamente si el binario no existe.

### 3. Configurar la cantidad de procesos por máquina

Antes de compilar, revisa la constante `totalProcesosPorMaquina` en `cmd/main.go`: debe coincidir con el número de procesos que vas a levantar en `./script.sh <MAQUINA> <N>` (por defecto está en 7). Si cambias la cantidad, edita esa constante y recompila en las 3 VMs.

### 4. Inicializar procesos

Ejecutar en **cada máquina**:

```bash
# Máquina 1 (IP 10.10.28.35)
./script.sh 1 7

# Máquina 2 (IP 10.10.28.36)
./script.sh 2 7

# Máquina 3 (IP 10.10.28.37)
./script.sh 3 7
```

### 5. Recuperar un proceso caído (sin re-ejecutar instrucciones)

```bash
./script.sh 3 RESTAURAR 4
```

### 6. Matar un proceso / todos los de una máquina

```bash
./script.sh 3 MATAR 4      # mata el proceso que lee proceso_4.txt en la máquina 3
./script.sh 2 KILLALL
```

### 7. Activar/desactivar modo infectado de esta máquina

```bash
./script.sh INFECTAR
```

### 8. Ver estado de un proceso

```bash
./iniciar.sh 1 ESTADO 1
```

---

## Formato de archivos de instrucciones

```
instrucciones/proceso_1.txt ... proceso_7.txt
```

```
VETAR <nombre>
COMPRAR <persona> <producto> <cantidad>
PERDONAR <nombre>
```

---

## Formato de logs generados

- **`logs/inventario_M<m>P<p>.log`** — una línea por instrucción ejecutada (se trunca al iniciar una nueva ejecución).
- **`logs/vetos_M<m>P<p>.log`** — estado final de vetos.
- **`logs/inventario_propio_M<m>P<p>.json`** — inventario propio persistido (copia individual, nunca la plantilla original).
- **`logs/stdout_M<m>P<p>.log`** — salida estándar completa del proceso, incluida la traza de recuperación por quorum.

---

## Mecanismos de consistencia

| Problema | Solución implementada |
|----------|-----------------------|
| Cada proceso debe tener su copia individual de inventario | `Store.LoadFromTemplate` copia (no referencia) el JSON elegido al azar |
| Conocimiento del estado de los demás procesos | `PeerTable`: cada proceso recibe `PushInventory` de todos los demás cada vez que cambian, y mantiene una copia de solo lectura |
| Recuperación de un proceso caído | `QueryInventory` a todos los procesos del sistema + quorum 2/3 sobre las respuestas; si no se alcanza, error de integridad referenciando BioShock |
| Detección de comportamiento bizantino | Modo "infectado" por máquina: todos sus procesos responden `QueryInventory` con datos falsos a cualquiera que pregunte |
| Condiciones de carrera en el Store local | `sync.RWMutex` en `Store` y en `PeerTable` |
| Eliminación selectiva de un proceso | Cada expendedora corre como proceso real e independiente del SO; `MATAR <ID>` solo afecta a ese proceso |

---

## Uso de IA

Se utilizó asistencia de IA (Claude) para la generación de la arquitectura gRPC, el diseño del archivo `.proto`, la lógica de `PeerTable`/recuperación por quorum, y el script bash. El entorno de desarrollo de la IA no tuvo acceso a internet para descargar las dependencias de gRPC ni para compilar/probar el código resultante; por eso el código fue validado sintácticamente contra una interfaz equivalente simulada, pero **debe compilarse y probarse en un entorno real (tu VM) antes de la entrega**. Todo el código fue revisado y documentado manualmente por el grupo.

---

## Consideraciones especiales

- **No se pudo compilar ni ejecutar este código en el entorno de generación.** Es imprescindible que ejecutes `./generate_proto.sh`, `go mod tidy`, y compiles exitosamente en al menos una VM antes de distribuirlo a las otras dos, verificando que no haya errores de compilación.
- La constante `totalProcesosPorMaquina` en `cmd/main.go` debe coincidir exactamente entre las 3 VMs y con el número de procesos que efectivamente levantes.
- Los puertos `8001`–`8407` (con 7 procesos por máquina) deben estar abiertos entre las 3 VMs.
- El archivo flag de infección (`.infectado`) vive en el directorio de trabajo; ejecuta siempre `script.sh` desde la raíz del repositorio.
