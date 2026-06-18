# Tarea 3 – INF-343 Sistemas Distribuidos: ¿Y El Pensador?

## Integrantes

| Nombres | Apellidos | Rol |
|--------|----------|-----|
| Emilio Alonso | Valdebenito Pinto | 202273040-4 |
| Erick Jakin | Ávila Ayala | 202273103-6 |
| Hans | Villouta Laing | 202273052-8 |

---

## Rol de cada máquina virtual

| Máquina | IP           | Rol |
|---------|--------------|-----|
| 1       | 10.10.28.35  | Maquina lider, es la que pone de acuerdo a todas sobre el inventario elegido durante la creación del proceso. Corre una instancia de cada proceso lógico (expendedora) del sistema. Expone una API REST por cada proceso. |
| 2       | 10.10.28.36  | Igual que máquina 1: corre la réplica paralela de cada proceso lógico, comunicándose por REST con las otras dos máquinas. |
| 3       | 10.10.28.37  | Igual que máquinas 1 y 2: completa el grupo de 3 réplicas por proceso lógico, necesario para el quórum 2/3. |

---

## Arquitectura de comunicación: API REST

Toda la comunicación entre procesos usa **HTTP REST con JSON**. Cada proceso expone su propia API en el puerto `8000 + (machineID-1)*100 + processID`.

| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/health` | GET | Chequeo de disponibilidad, usado al iniciar para esperar a las réplicas paralelas (máx. 2s). |
| `/inventory` | POST | Una réplica paralela envía su inventario inicial recién sorteado. |
| `/state-report` | POST | Una réplica paralela envía su inventario y vetos finales para la fase de comparación por quórum. |

**Justificación:** REST sobre HTTP es ampliamente soportado por la librería estándar de Go (`net/http`), no requiere dependencias externas, y los endpoints son fáciles de probar manualmente con `curl` durante el desarrollo. En corto, era lo más fácil de usar.

---

## Ciclo de vida de un proceso (expendedora)

Al iniciar la creación de procesos se necesita encontrar al **líder de inventario**: la definimos como la réplica corriendo en la máquina con el `MachineID` más bajo del grupo, como en este caso son 3 máquinas, siempre es la máquina 1). Esto para evitar que cada réplica escoja su inventario de forma random, haciendo que exista la probabilidad de que los procesos paralelos sean diferentes. Los pasos que sigue cada máquina durante la ejecución de los procesos es la siguiente:

1. **Levanta su servidor REST** propio (escucha en su puerto asignado).
2. **Si es el líder**: sortea un inventario plantilla al azar de `/inventario` y lo **copia** (nunca modifica el original) a su archivo propio. Si **no es el líder**: espera (hasta 5 segundos) a recibir ese mismo inventario desde el líder vía `POST /inventory`, y lo aplica como su copia propia. Así las 3 réplicas arrancan siempre con datos idénticos.
3. **El líder espera a sus réplicas paralelas** vía `GET /health`, hasta 2 segundos, antes de enviarles el inventario.
4. **El líder distribuye su inventario inicial** a las réplicas vía `POST /inventory`.
5. **Cada réplica ejecuta sus instrucciones** (`VETAR`/`COMPRAR`/`PERDONAR`) desde su archivo `instrucciones/proceso_<ID>.txt`, aplicándolas sobre su propia copia de inventario y vetos (ahora consistente entre las 3), generando un log por cada instrucción.
6. **Reporta su estado final** (inventario + vetos resultantes) a sus réplicas vía `POST /state-report`, y recolecta los reportes que ellas le envían.
7. **Determina el resultado por quórum**: si ≥ 2/3 de los reportes (incluido el propio) coinciden exactamente en inventario y vetos, ese es el resultado válido. Si no se alcanza el quórum, se considera que el sistema fue comprometido y se imprime:
   > *Todas las máquinas han sido infectadas, por favor revíseme.*

---

## Modo "Infectado" (proceso bizantino)

`INFECTAR` marca a la máquina como infectada. Los procesos que se creen a continuación arrancarán en modo infectado desde el inicio, la ejecución será normal, pero a la hora de compartir su inventario final con las demás máquinas esta reportar datos falsos.

```bash
# 1. Marcar la máquina como infectada (crea el flag .infectado)
./script.sh INFECTAR

# 2. Ahora crear los procesos — arrancarán infectados
./script.sh 1 2

# Para desactivar la infección (toggle):
./script.sh INFECTAR
```

---

## Estructura del repositorio

```
.
├── cmd/
│   └── main.go                  # Punto de entrada: parsea argumentos y arranca el proceso
├── internal/
│   ├── process/
│   │   └── process.go           # Ciclo de vida completo: sorteo, distribución, ejecución, quórum
│   ├── store/
│   │   └── store.go             # Inventario y vetos persistidos en disco (JSON)
│   ├── httpapi/
│   │   ├── server.go            # Servidor REST (handlers /health, /inventory, /state-report)
│   │   └── client.go            # Cliente REST para llamar a las réplicas paralelas
│   └── protocol/
│       └── types.go             # Tipos JSON compartidos entre cliente y servidor
├── instrucciones/                # Archivos proceso_<ID>.txt
├── inventario/                   # Plantillas de inventario (nunca se modifican directamente)
├── logs/                         # Logs, inventarios propios, resultados finales
├── script.sh                     # Script principal de control
├── iniciar.sh                    # Script auxiliar para ESTADO
├── go.mod
└── README.md
```

---

## Instrucciones de uso

### 1. Prerequisitos (en cada VM)

```bash
cd Tarea3
chmod +x script.sh iniciar.sh
```

### 2. Compilar

```bash
go build -o expendedora ./cmd/main.go
```

### 3. Inicializar procesos

Ejecutar en cada máquina:

```bash
# Máquina 1 (IP 10.10.28.35)
./script.sh 1 2

# Máquina 2 (IP 10.10.28.36)
./script.sh 2 2

# Máquina 3 (IP 10.10.28.37)
./script.sh 3 2
```

Cada proceso ejecuta su ciclo de vida completo automáticamente (sorteo → distribución → instrucciones → comparación final). Ejecutar las instrucciones lo más rápido posible entre las máquinas, por el tiempo de espera de 2 segundos, y siempre en orden MV1 -> MV2 -> MV3.

### 4. Restaurar un proceso (reiniciar su ciclo completo)

```bash
./script.sh <numero_maquina> RESTAURAR <numero_proceso>
```

### 5. Matar un proceso específico / todos los de una máquina

```bash
./script.sh <numero_maquina> MATAR <numero_proceso>
./script.sh <numero_maquina> KILLALL
```

### 6. Activar/desactivar modo infectado

```bash
./script.sh INFECTAR
```

### 7. Ver estado de un proceso

```bash
./iniciar.sh <numero_maquina> ESTADO <numero_proceso>
```

Muestra el inventario propio, los vetos actuales, y el resultado de la última comparación por quórum.

---

## Formato de archivos de instrucciones

```
instrucciones/proceso_1.txt
instrucciones/proceso_2.txt
```

Instrucciones soportadas:

```
VETAR <nombre>
COMPRAR <persona> <producto> <cantidad>
PERDONAR <nombre>
```

---

## Formato de logs generados

**`logs/inventario_M<numero_maquina>P<numero_proceso>.log`** – una línea por instrucción ejecutada, por ejemplo:

```
VETAR jack
COMPRAR jack manzana 10 | DENEGADO
COMPRAR atlas ADAM 5 | VALIDO
```

**`logs/vetos_M<M>P<P>.log`** – estado final de vetos luego de la ejecución, por ejemplo:

```
VETADO jack 3
```

**`logs/inventario_propio_M<M>P<P>.json`** – el inventario propio del proceso, que es una copia elegida aleatoriamente del total de inventarios de la carpeta inventario.

**`logs/resultado_M<M>P<P>.txt`** – resultado de la comparación final por quórum.

---

## Uso de IA

Se utilizó asistencia de IA (Claude) para la generación inicial de la estructura de los archivos Go y el script bash, incluyendo el rediseño de la arquitectura hacia API REST y el mecanismo de quórum/infección. Todo el código fue revisado, adaptado y documentado manualmente por el grupo.

---

## Consideraciones especiales

- Cada proceso, al reportar su estado final, espera hasta 3 segundos a recibir los reportes de sus réplicas paralelas antes de resolver el quórum con los datos disponibles.
- El archivo flag de infección (`.infectado_M<M>P<P>`) vive en el directorio de trabajo del proceso; asegúrese de ejecutar `script.sh` siempre desde la raíz del repositorio.
- Los puertos `8001`–`8399` deben estar abiertos entre las 3 VMs para que la API REST funcione.
