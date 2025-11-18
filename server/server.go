// server.go
package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
	"github.com/vmihailenco/msgpack/v5"
)

type Envelope struct {
	Service string                 `msgpack:"service"`
	Data    map[string]interface{} `msgpack:"data"`
	Clock   int                    `msgpack:"clock"`
}

type RefReply struct {
	Service string                 `msgpack:"service"`
	Data    map[string]interface{} `msgpack:"data"`
	Clock   int                    `msgpack:"clock"`
}

// -----------------------------------------------------------------------------
// FIX PARA TIPOS -> int e string
// -----------------------------------------------------------------------------
func getInt(v interface{}) int {
	switch x := v.(type) {
	case int:
		return x
	case int8, int16, int32, int64:
		return int(fmt.Sprintf("%d", x)[0])
	case float32:
		return int(x)
	case float64:
		return int(x)
	case uint:
		return int(x)
	case uint8:
		return int(x)
	case uint16:
		return int(x)
	case uint32:
		return int(x)
	case uint64:
		return int(x)
	default:
		return 0
	}
}

func getString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}

// -----------------------------------------------------------------------------
// VARIÁVEIS GLOBAIS
// -----------------------------------------------------------------------------
var (
	serverName  string
	serverRank  int
	serverAddr  string
	clock       int
	clockMutex  sync.Mutex
	coordMutex  sync.Mutex
	coordinator string

	pubSocket *zmq.Socket
	repClient *zmq.Socket
	repSrv    *zmq.Socket
	reqMap    = map[string]*zmq.Socket{}

	serversMutex sync.Mutex
	serversList  = map[string]map[string]interface{}{}

	// Persistência
	messages []Envelope
)

// -----------------------------------------------------------------------------
// CLOCK
// -----------------------------------------------------------------------------
func incClock() int {
	clockMutex.Lock()
	clock++
	v := clock
	clockMutex.Unlock()
	return v
}

func updateClock(v interface{}) {
	n := getInt(v)
	clockMutex.Lock()
	if n > clock {
		clock = n
	}
	clock++
	clockMutex.Unlock()
}

func nowISO() string { return time.Now().UTC().Format(time.RFC3339) }

// -----------------------------------------------------------------------------
// PERSISTÊNCIA
// -----------------------------------------------------------------------------
func saveJSON(path string, v interface{}) {
	b, _ := json.MarshalIndent(v, "", "  ")
	_ = ioutil.WriteFile(path, b, 0644)
}

func persistMessages() {
	saveJSON("/app/data/messages.json", messages)
}

func loadMessages() {
	b, err := ioutil.ReadFile("/app/data/messages.json")
	if err != nil || len(b) == 0 {
		messages = []Envelope{}
		return
	}
	_ = json.Unmarshal(b, &messages)
}

// -----------------------------------------------------------------------------
// REF SERVER
// -----------------------------------------------------------------------------
func refRequest(req map[string]interface{}) (map[string]interface{}, error) {

	ctx, _ := zmq.NewContext()
	defer ctx.Term()
	sock, _ := ctx.NewSocket(zmq.REQ)
	defer sock.Close()

	refAddr := os.Getenv("REF_ADDR")
	sock.Connect(refAddr)

	req["clock"] = incClock()

	packed, _ := msgpack.Marshal(map[string]interface{}{
		"service": req["service"],
		"data":    req,
		"clock":   req["clock"],
	})

	sock.SendBytes(packed, 0)

	raw, err := sock.RecvBytes(0)
	if err != nil {
		return nil, err
	}

	var rep RefReply
	msgpack.Unmarshal(raw, &rep)

	updateClock(rep.Clock)

	return rep.Data, nil
}

// -----------------------------------------------------------------------------
// SERVER-TO-SERVER
// -----------------------------------------------------------------------------
func ensureReqTo(addr string) (*zmq.Socket, error) {
	if s, ok := reqMap[addr]; ok {
		return s, nil
	}

	ctx, _ := zmq.NewContext()
	req, _ := ctx.NewSocket(zmq.REQ)
	req.SetLinger(0)
	req.Connect(addr)

	reqMap[addr] = req
	return req, nil
}

func sendSrvReq(addr string, service string, data map[string]interface{}, timeoutMs int) (map[string]interface{}, error) {

	sock, _ := ensureReqTo(addr)

	env := map[string]interface{}{
		"service": service,
		"data":    data,
		"clock":   incClock(),
	}

	out, _ := msgpack.Marshal(env)
	sock.SendBytes(out, 0)

	poller := zmq.NewPoller()
	poller.Add(sock, zmq.POLLIN)

	res, _ := poller.Poll(time.Duration(timeoutMs) * time.Millisecond)
	if len(res) == 0 {
		return nil, fmt.Errorf("timeout")
	}

	raw, _ := sock.RecvBytes(0)

	var rep map[string]interface{}
	msgpack.Unmarshal(raw, &rep)

	updateClock(rep["clock"])

	return rep, nil
}

// -----------------------------------------------------------------------------
// ELEIÇÃO (BULLY)
// -----------------------------------------------------------------------------
func startElection() {
	log.Printf("[INFO] Iniciando eleição Bully...")

	serversMutex.Lock()
	targets := []struct {
		name string
		addr string
		rank int
	}{}

	for name, info := range serversList {
		r := getInt(info["rank"])
		addr := getString(info["addr"])
		if r > serverRank {
			targets = append(targets, struct {
				name string
				addr string
				rank int
			}{name, addr, r})
		}
	}
	serversMutex.Unlock()

	if len(targets) == 0 {
		declareCoordinator(serverName)
		return
	}

	gotOK := false
	for _, t := range targets {
		_, err := sendSrvReq(t.addr, "election", map[string]interface{}{"from": serverName}, 1500)
		if err == nil {
			gotOK = true
			break
		}
	}

	if !gotOK {
		declareCoordinator(serverName)
	}
}

func declareCoordinator(name string) {

	coordMutex.Lock()
	coordinator = name
	coordMutex.Unlock()

	ann := map[string]interface{}{
		"service": "election",
		"data": map[string]interface{}{
			"coordinator": name,
			"timestamp":   nowISO(),
			"clock":       incClock(),
		},
	}

	packed, _ := msgpack.Marshal(ann)
	pubSocket.SendMessage("servers", packed)

	log.Printf("🟡 Novo coordenador: %s", name)
}

// -----------------------------------------------------------------------------
// LISTENER SUB
// -----------------------------------------------------------------------------
func startSubListener(sub *zmq.Socket) {
	for {
		parts, _ := sub.RecvMessageBytes(0)
		if len(parts) < 2 {
			continue
		}

		topic := string(parts[0])
		body := parts[1]

		if topic == "servers" {
			var ann map[string]interface{}
			msgpack.Unmarshal(body, &ann)

			if data, ok := ann["data"].(map[string]interface{}); ok {
				if coord, ok := data["coordinator"].(string); ok {
					coordMutex.Lock()
					coordinator = coord
					coordMutex.Unlock()
					log.Printf("[INFO] 📢 Coordenador atualizado: %s", coord)
				}
			}
		}
	}
}

// -----------------------------------------------------------------------------
// SERVER REP LOOP (COMUNICAÇÃO ENTRE SERVERS)
// -----------------------------------------------------------------------------
func handleSrvReq(env Envelope) map[string]interface{} {
	switch env.Service {

	case "election":
		return map[string]interface{}{"election": "OK", "clock": incClock()}

	case "clock":
		return map[string]interface{}{"time": nowISO(), "clock": incClock()}

	default:
		return map[string]interface{}{"error": "unknown", "clock": incClock()}
	}
}

func startRepSrvLoop() {
	for {
		raw, _ := repSrv.RecvBytes(0)

		var env Envelope
		msgpack.Unmarshal(raw, &env)

		updateClock(env.Clock)

		resp := handleSrvReq(env)
		out, _ := msgpack.Marshal(resp)
		repSrv.SendBytes(out, 0)
	}
}

// -----------------------------------------------------------------------------
// MAIN
// -----------------------------------------------------------------------------
func main() {

	rand.Seed(time.Now().UnixNano())

	serverName = os.Getenv("SERVER_NAME")
	if serverName == "" {
		serverName = fmt.Sprintf("server-%04d", rand.Intn(10000))
	}

	serverAddr = os.Getenv("SERVER_ADDR")

	os.MkdirAll("/app/data", 0755)
	loadMessages()

	ctx, _ := zmq.NewContext()

	// REP -> Broker
	repClient, _ = ctx.NewSocket(zmq.REP)
	repClient.Connect(os.Getenv("BROKER_DEALER_ADDR"))

	// PUB -> Proxy
	pubSocket, _ = ctx.NewSocket(zmq.PUB)
	pubSocket.Connect(os.Getenv("PROXY_PUB_ADDR"))

	// SUB -> Proxy
	sub, _ := ctx.NewSocket(zmq.SUB)
	sub.SetSubscribe("servers")
	sub.Connect(os.Getenv("PROXY_SUB_ADDR"))
	go startSubListener(sub)

	// REP -> outros servers
	repSrv, _ = ctx.NewSocket(zmq.REP)
	repSrv.Bind(serverAddr)
	go startRepSrvLoop()

	// REGISTRO REF
	refResp, err := refRequest(map[string]interface{}{
		"service": "rank",
		"user":    serverName,
		"addr":    serverAddr,
	})

	if err != nil {
		log.Fatalf("Erro no REF: %v", err)
	}

	serverRank = getInt(refResp["rank"])
	log.Printf("[INFO] Servidor iniciado: %s rank=%d", serverName, serverRank)

	// LIST SERVERS
	listResp, _ := refRequest(map[string]interface{}{"service": "list"})
	if l, ok := listResp["list"].([]interface{}); ok {

		serversMutex.Lock()
		for _, it := range l {
			m := it.(map[string]interface{})
			name := getString(m["name"])
			serversList[name] = map[string]interface{}{
				"rank": getInt(m["rank"]),
				"addr": getString(m["addr"]),
			}
		}
		serversMutex.Unlock()
	}

	// DEFINE COORDENADOR
	bestName := serverName
	bestRank := serverRank

	serversMutex.Lock()
	for name, info := range serversList {
		r := getInt(info["rank"])
		if r > bestRank {
			bestRank = r
			bestName = name
		}
	}
	serversMutex.Unlock()

	coordinator = bestName
	log.Printf("[INFO] Coordenador inicial: %s", bestName)

	// HEARTBEAT
	go func() {
		for {
			refRequest(map[string]interface{}{
				"service": "heartbeat",
				"user":    serverName,
				"addr":    serverAddr,
			})
			time.Sleep(5 * time.Second)
		}
	}()

	// LIVENESS DO COORDENADOR
	go func() {
		for {
			time.Sleep(3 * time.Second)

			if coordinator == serverName {
				continue
			}

			serversMutex.Lock()
			info, ok := serversList[coordinator]
			serversMutex.Unlock()

			if !ok {
				startElection()
				continue
			}

			addr := getString(info["addr"])

			_, err := sendSrvReq(addr, "ping", map[string]interface{}{"from": serverName}, 1200)
			if err != nil {
				log.Printf("[WARN] Coordenador %s caiu. Elegendo novo...", coordinator)
				startElection()
			}
		}
	}()

	// MAIN LOOP -> CLIENTES E BOTS
	for {
		msg, err := repClient.RecvMessageBytes(0)
		if err != nil {
			continue
		}

		var req Envelope
		msgpack.Unmarshal(msg[0], &req)

		updateClock(req.Clock)

		resp := map[string]interface{}{
			"timestamp": nowISO(),
			"clock":     incClock(),
		}

		switch req.Service {

		case "login":
			user := getString(req.Data["user"])
			if user == "" {
				resp["status"] = "erro"
			} else {
				resp["status"] = "sucesso"
			}

		case "channels":
			resp["channels"] = []string{"geral"}

		case "publish":
			ch := getString(req.Data["channel"])

			env := Envelope{
				Service: "publish",
				Data: map[string]interface{}{
					"user":      req.Data["user"],
					"channel":   ch,
					"message":   req.Data["message"],
					"timestamp": resp["timestamp"],
				},
				Clock: incClock(),
			}

			raw, _ := msgpack.Marshal(env)
			pubSocket.SendMessage(ch, raw)

			messages = append(messages, env)
			persistMessages()

			resp["status"] = "OK"

		case "message":
			dst := getString(req.Data["dst"])

			env := Envelope{
				Service: "message",
				Data: map[string]interface{}{
					"src":       req.Data["src"],
					"dst":       dst,
					"message":   req.Data["message"],
					"timestamp": resp["timestamp"],
				},
				Clock: incClock(),
			}

			raw, _ := msgpack.Marshal(env)
			pubSocket.SendMessage(dst, raw)

			messages = append(messages, env)
			persistMessages()

			resp["status"] = "OK"

		default:
			resp["status"] = "erro"
			resp["message"] = "serviço desconhecido"
		}

		out, _ := msgpack.Marshal(map[string]interface{}{
			"service": req.Service,
			"data":    resp,
			"clock":   incClock(),
		})

		repClient.SendBytes(out, 0)
	}
}
