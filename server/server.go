// server.go
package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
	"github.com/vmihailenco/msgpack/v5"
)

// -------------------------------
// GLOBALS UNIFICADOS
// -------------------------------
var (
	coordMutex  sync.Mutex
	coordinator string // nome do coordenador atual

	meuNome string // nome deste servidor
	meuRank int    // rank recebido do REF

	serverAddr string

	serversList  = map[string]map[string]interface{}{}
	serversMutex sync.Mutex

	usersMutex sync.Mutex
	users      = map[string]string{} // user -> timestamp

	channelsMutex sync.Mutex
	channels      = map[string]bool{}

	clock      int
	clockMutex sync.Mutex

	messages            = []Envelope{}
	msgCountSinceSync   = 0
	msgCountSinceSyncMu sync.Mutex

	pubSocket *zmq.Socket
	repClient *zmq.Socket
	repSrv    *zmq.Socket
	reqMap    = map[string]*zmq.Socket{}

	dataDir string

	// Election protection
	emElection   bool
	emElectionMu sync.Mutex
)

// -------------------------------
// TIPOS
// -------------------------------
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

// ----------------------- util conversoes -----------------------
func getInt(v interface{}) int {
	switch x := v.(type) {
	case int:
		return x
	case int8:
		return int(x)
	case int16:
		return int(x)
	case int32:
		return int(x)
	case int64:
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
	case float32:
		return int(x)
	case float64:
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

// ----------------------- logging -----------------------
func logInfo(format string, a ...interface{}) { log.Printf("[INFO] "+format, a...) }
func logWarn(format string, a ...interface{}) { log.Printf("[WARN] "+format, a...) }
func logErr(format string, a ...interface{})  { log.Printf("[ERROR] "+format, a...) }

// ----------------------- clock -----------------------
func incClock() int {
	clockMutex.Lock()
	clock++
	v := clock
	clockMutex.Unlock()
	return v
}

func updateClock(recv interface{}) {
	n := getInt(recv)
	clockMutex.Lock()
	if n > clock {
		clock = n
	}
	clock++
	clockMutex.Unlock()
}

func nowISO() string { return time.Now().UTC().Format(time.RFC3339) }

// ----------------------- persistência -----------------------
func ensureDataDir() {
	if dataDir == "" {
		dataDir = "/app/data"
	}
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		_ = os.MkdirAll(dataDir, 0755)
	}
}

func saveJSONFile(p string, v interface{}) {
	ensureDataDir()
	b, _ := json.MarshalIndent(v, "", "  ")
	_ = ioutil.WriteFile(p, b, 0644)
}

func persistUsers() {
	usersMutex.Lock()
	out := []map[string]string{}
	for u, ts := range users {
		out = append(out, map[string]string{"user": u, "timestamp": ts})
	}
	usersMutex.Unlock()
	saveJSONFile(path.Join(dataDir, "users.json"), out)
}

func persistChannels() {
	channelsMutex.Lock()
	out := []string{}
	for c := range channels {
		out = append(out, c)
	}
	channelsMutex.Unlock()
	saveJSONFile(path.Join(dataDir, "channels.json"), out)
}

func persistMessages() {
	saveJSONFile(path.Join(dataDir, "messages.json"), messages)
}

func loadState() {
	ensureDataDir()

	b, err := ioutil.ReadFile(path.Join(dataDir, "users.json"))
	if err == nil && len(b) > 0 {
		var ul []map[string]string
		if _ = json.Unmarshal(b, &ul); ul != nil {
			usersMutex.Lock()
			for _, u := range ul {
				if name, ok := u["user"]; ok {
					users[name] = u["timestamp"]
				}
			}
			usersMutex.Unlock()
		}
	}

	b, err = ioutil.ReadFile(path.Join(dataDir, "channels.json"))
	if err == nil && len(b) > 0 {
		var cl []string
		if _ = json.Unmarshal(b, &cl); cl != nil {
			channelsMutex.Lock()
			for _, c := range cl {
				channels[c] = true
			}
			channelsMutex.Unlock()
		}
	}

	b, err = ioutil.ReadFile(path.Join(dataDir, "messages.json"))
	if err == nil && len(b) > 0 {
		var ml []Envelope
		if _ = json.Unmarshal(b, &ml); ml != nil {
			messages = ml
		}
	}
}

// ----------------------- REF (REQ/REP) -----------------------
func refRequest(req map[string]interface{}) (map[string]interface{}, error) {
	ctx, _ := zmq.NewContext()
	defer ctx.Term()

	sock, _ := ctx.NewSocket(zmq.REQ)
	defer sock.Close()

	refAddr := os.Getenv("REF_ADDR")
	if refAddr == "" {
		return nil, fmt.Errorf("REF_ADDR missing")
	}

	if err := sock.Connect(refAddr); err != nil {
		return nil, err
	}

	// include logical clock in the request
	req["clock"] = incClock()

	outEnv := map[string]interface{}{
		"service": req["service"],
		"data":    req,
		"clock":   req["clock"],
	}

	out, _ := msgpack.Marshal(outEnv)
	if _, err := sock.SendBytes(out, 0); err != nil {
		return nil, err
	}

	replyBytes, err := sock.RecvBytes(0)
	if err != nil {
		return nil, err
	}

	var rep RefReply
	if err := msgpack.Unmarshal(replyBytes, &rep); err != nil {
		return nil, err
	}

	// update local clock with REF clock
	updateClock(rep.Clock)
	return rep.Data, nil
}

// ----------------------- server-to-server REQ helper -----------------------
func ensureReqTo(addr string) (*zmq.Socket, error) {
	if s, ok := reqMap[addr]; ok {
		return s, nil
	}

	ctx, _ := zmq.NewContext()
	req, _ := ctx.NewSocket(zmq.REQ)
	req.SetLinger(0)

	if err := req.Connect(addr); err != nil {
		return nil, err
	}

	reqMap[addr] = req
	return req, nil
}

func sendSrvReq(addr string, service string, data map[string]interface{}, timeoutMs int) (map[string]interface{}, error) {
	sock, err := ensureReqTo(addr)
	if err != nil {
		return nil, err
	}

	env := map[string]interface{}{
		"service": service,
		"data":    data,
		"clock":   incClock(),
	}

	out, _ := msgpack.Marshal(env)
	if _, err := sock.SendBytes(out, 0); err != nil {
		return nil, err
	}

	poller := zmq.NewPoller()
	poller.Add(sock, zmq.POLLIN)
	socks, _ := poller.Poll(time.Duration(timeoutMs) * time.Millisecond)
	if len(socks) == 0 {
		return nil, fmt.Errorf("timeout")
	}

	msg, _ := sock.RecvBytes(0)

	var rep map[string]interface{}
	_ = msgpack.Unmarshal(msg, &rep)

	if c, ok := rep["clock"]; ok {
		updateClock(c)
	}

	return rep, nil
}

// ----------------------- replicação (PUB replicate) -----------------------
func publishReplicate(action string, payload map[string]interface{}) {
	rm := map[string]interface{}{
		"origin":    meuNome,
		"action":    action,
		"payload":   payload,
		"timestamp": nowISO(),
		"clock":     incClock(),
	}

	outEnv := map[string]interface{}{
		"service": "replicate",
		"data":    rm,
		"clock":   incClock(),
	}

	out, _ := msgpack.Marshal(outEnv)

	if pubSocket != nil {
		if _, err := pubSocket.SendMessage("replicate", out); err != nil {
			logWarn("replicate publish error: %v", err)
		}
	}
}

// ----------------------- COORDENADOR (INICIAL / ELECTION) -----------------------

// Define quem é o coordenador inicial baseado no meuRank
func definirCoordenadorInicial(pub *zmq.Socket) {
	if meuRank == 1 {
		coordMutex.Lock()
		coordinator = meuNome
		coordMutex.Unlock()

		log.Printf("[INFO] Coordenador inicial definido: %s", meuNome)

		// Publica para todos saberem
		publicarCoordenador(pub, meuNome)
	} else {
		coordMutex.Lock()
		coordinator = ""
		coordMutex.Unlock()
		log.Printf("[INFO] Aguardando coordenador inicial...")
	}
}

// Publica coordenador via tópico "servers"
func publicarCoordenador(pub *zmq.Socket, nome string) {
	env := map[string]interface{}{
		"service": "election",
		"data": map[string]interface{}{
			"coordinator": nome,
		},
		"timestamp": nowISO(),
		"clock":     incClock(),
	}

	raw, _ := msgpack.Marshal(env)
	// PUB: send topic + payload
	if pub != nil {
		pub.Send("servers", zmq.SNDMORE)
		pub.SendBytes(raw, 0)
	}

	log.Printf("[INFO] Coordenador publicado para todos: %s", nome)
}

// DECLARE coordinator locally and announce (thread-safe)
func declareCoordinator(name string) {
	coordMutex.Lock()
	coordinator = name
	coordMutex.Unlock()

	// announce on PUB
	ann := map[string]interface{}{
		"service": "election",
		"data": map[string]interface{}{
			"coordinator": name,
			"timestamp":   nowISO(),
			"clock":       incClock(),
		},
	}

	out, _ := msgpack.Marshal(ann)
	if pubSocket != nil {
		if _, err := pubSocket.SendMessage("servers", out); err != nil {
			logWarn("Erro ao anunciar coordenador: %v", err)
		}
	}

	logInfo("🟡 Novo coordenador: %s", name)
	publishReplicate("election", map[string]interface{}{"coordinator": name})
}

// Recebe anúncio via SUB (tópico "servers")
func receberNotificacaoCoordenador(sub *zmq.Socket) {
	for {
		_, err := sub.Recv(0)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		raw, err := sub.RecvBytes(0)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		var env map[string]interface{}
		_ = msgpack.Unmarshal(raw, &env)
		if env["service"] == "election" {
			if data, ok := env["data"].(map[string]interface{}); ok {
				if coord, ok2 := data["coordinator"].(string); ok2 {
					coordMutex.Lock()
					if coordinator != coord {
						coordinator = coord
						logInfo("📢 Novo coordenador recebido: %s", coord)
					}
					coordMutex.Unlock()
				}
			}
		}
	}
}

// ----------------------- election (Bully) -----------------------
func startElection() {
    // Garantir que apenas UMA eleição rode por vez
    emElectionMu.Lock()
    if emElection {
        emElectionMu.Unlock()
        logInfo("Eleição já em andamento — ignorando nova requisição")
        return
    }
    emElection = true
    emElectionMu.Unlock()

    logInfo("Iniciando eleição Bully — procurando servidores com rank maior...")

    // 1) Encontrar servidores com rank MAIOR que eu
    serversMutex.Lock()
    var higher []struct {
        name string
        addr string
        rank int
    }
    for name, info := range serversList {
        r := getInt(info["rank"])
        if r > meuRank {
            higher = append(higher, struct {
                name string
                addr string
                rank int
            }{
                name, getString(info["addr"]), r,
            })
        }
    }
    serversMutex.Unlock()

    // 2) Se sou o maior → ganho automaticamente
    if len(higher) == 0 {
        declareCoordinator(meuNome)
        emElectionMu.Lock()
        emElection = false
        emElectionMu.Unlock()
        return
    }

    // 3) Enviar "election" para todos maiores
    gotOK := false
    for _, h := range higher {
        logInfo("Enviando election para %s (rank=%d)", h.name, h.rank)

        _, err := sendSrvReq(h.addr, "election", map[string]interface{}{
            "from": meuNome,
        }, 1500)

        if err == nil {
            gotOK = true
            logInfo("%s respondeu OK — cedendo eleição", h.name)
            break
        }
    }

    // 4) Se ninguém respondeu → eu ganho
    if !gotOK {
        declareCoordinator(meuNome)
        emElectionMu.Lock()
        emElection = false
        emElectionMu.Unlock()
        return
    }

    // 5) Alguém maior respondeu → esperar anúncio oficial via SUB
    logInfo("Aguardando anúncio do novo coordenador...")

    timeout := time.After(4 * time.Second)
    ticker := time.NewTicker(150 * time.Millisecond)

waitLoop:
    for {
        select {
        case <-timeout:
            // Timeout → reiniciar eleição
            logWarn("Timeout aguardando anúncio — reiniciando eleição")

            emElectionMu.Lock()
            emElection = false
            emElectionMu.Unlock()

            go func() {
                time.Sleep(300 * time.Millisecond)
                startElection()
            }()
            break waitLoop

        case <-ticker.C:
            coordMutex.Lock()
            c := coordinator
            coordMutex.Unlock()

            if c != "" && c != meuNome {
                // Recebi anúncio → eleição termina
                emElectionMu.Lock()
                emElection = false
                emElectionMu.Unlock()
                break waitLoop
            }
        }
    }
}

// ----------------------- server-to-server handler (REP) -----------------------
func handleSrvReq(env Envelope) map[string]interface{} {
	switch env.Service {
	case "election":
		// when asked for election, reply OK and do NOT start a new election
		logInfo("Recebi pedido de eleição de %v", env.Data["from"])
		return map[string]interface{}{"election": "OK", "clock": incClock()}

	case "clock":
		return map[string]interface{}{"time": nowISO(), "clock": incClock()}

	case "ping":
		return map[string]interface{}{"pong": "OK", "clock": incClock()}

	default:
		return map[string]interface{}{"error": "unknown service", "clock": incClock()}
	}
}

func startRepSrvLoop() {
	for {
		msg, err := repSrv.RecvBytes(0)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		var env Envelope
		_ = msgpack.Unmarshal(msg, &env)

		updateClock(env.Clock)
		resp := handleSrvReq(env)

		out, _ := msgpack.Marshal(resp)
		_, _ = repSrv.SendBytes(out, 0)
	}
}

// ----------------------- SUB listener (proxy) -----------------------
func startSubListener(sub *zmq.Socket) {
    for {
        parts, err := sub.RecvMessageBytes(0)
        if err != nil || len(parts) < 2 {
            time.Sleep(50 * time.Millisecond)
            continue
        }

        topic := string(parts[0])
        body := parts[1]

        switch topic {
        case "servers":
            var ann map[string]interface{}
            _ = msgpack.Unmarshal(body, &ann)

            if data, ok := ann["data"].(map[string]interface{}); ok {
                if coord, ok := data["coordinator"].(string); ok {

                    // Atualiza coordenador
                    coordMutex.Lock()
                    coordinator = coord
                    coordMutex.Unlock()

                    // ENCERRA ELEIÇÃO
                    emElectionMu.Lock()
                    emElection = false
                    emElectionMu.Unlock()

                    logInfo("📢 Novo coordenador recebido: %s", coord)
                }
            }

  		case "replicate":
			var r map[string]interface{}
			_ = msgpack.Unmarshal(body, &r)

			if data, ok := r["data"].(map[string]interface{}); ok {
				if action, ok2 := data["action"].(string); ok2 {

					switch action {
					case "add_user":
						if payload, ok3 := data["payload"].(map[string]interface{}); ok3 {
							u := getString(payload["user"])
							ts := getString(payload["timestamp"])

							usersMutex.Lock()
							if _, exists := users[u]; !exists {
								users[u] = ts
								usersMutex.Unlock()
								persistUsers()
							} else {
								usersMutex.Unlock()
							}
						}

					case "add_channel":
						if payload, ok3 := data["payload"].(map[string]interface{}); ok3 {
							ch := getString(payload["channel"])

							channelsMutex.Lock()
							if !channels[ch] {
								channels[ch] = true
								channelsMutex.Unlock()
								persistChannels()
							} else {
								channelsMutex.Unlock()
							}
						}
					}
				}
			}
		default:
			// also handle channel names (topic=channel)
			// Many proxies send topic as string; we already handle above
		}
	}
}

// ----------------------- MAIN -----------------------
func main() {
	rand.Seed(time.Now().UnixNano())

	// nome do servidor
	meuNome = os.Getenv("SERVER_NAME")
	if meuNome == "" {
		meuNome = fmt.Sprintf("server-%04d", rand.Intn(10000))
	}

	serverAddr = os.Getenv("SERVER_ADDR")
	if serverAddr == "" {
		logErr("SERVER_ADDR ausente!")
		os.Exit(1)
	}

	dataDir = os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "/app/data"
	}

	ensureDataDir()
	loadState()

	ctx, _ := zmq.NewContext()

	// REP socket to receive client/broker requests
	repClient, _ = ctx.NewSocket(zmq.REP)
	if brokerAddr := os.Getenv("BROKER_DEALER_ADDR"); brokerAddr != "" {
		_ = repClient.Connect(brokerAddr)
	} else {
		logWarn("BROKER_DEALER_ADDR vazio")
	}

	// PUB socket to proxy
	pubSocket, _ = ctx.NewSocket(zmq.PUB)
	if pubAddr := os.Getenv("PROXY_PUB_ADDR"); pubAddr != "" {
		_ = pubSocket.Connect(pubAddr)
	} else {
		logWarn("PROXY_PUB_ADDR vazio")
	}

	// SUB socket to receive servers announcements / replication
	sub, _ := ctx.NewSocket(zmq.SUB)
	_ = sub.SetSubscribe("servers")
	_ = sub.SetSubscribe("replicate")
	if subAddr := os.Getenv("PROXY_SUB_ADDR"); subAddr != "" {
		_ = sub.Connect(subAddr)
	} else {
		logWarn("PROXY_SUB_ADDR vazio")
	}
	go startSubListener(sub)

	// REP socket to serve other servers
	repSrv, _ = ctx.NewSocket(zmq.REP)
	if err := repSrv.Bind(serverAddr); err != nil {
		logErr("Erro bind repSrv: %v", err)
		os.Exit(1)
	}
	go startRepSrvLoop()

	// Ask REF for rank
	refResp, err := refRequest(map[string]interface{}{
		"service": "rank",
		"user":    meuNome,
		"addr":    serverAddr,
	})
	if err != nil {
		logErr("Erro no REF: %v", err)
		os.Exit(1)
	}

	meuRank = getInt(refResp["rank"])
	logInfo("Servidor iniciado → Nome=%s Rank=%d Addr=%s", meuNome, meuRank, serverAddr)

	// load list of servers
	listResp, _ := refRequest(map[string]interface{}{"service": "list"})
	if l, ok := listResp["list"].([]interface{}); ok {
		serversMutex.Lock()
		for _, it := range l {
			if m, ok2 := it.(map[string]interface{}); ok2 {
				name := getString(m["name"])
				serversList[name] = map[string]interface{}{
					"rank": getInt(m["rank"]),
					"addr": getString(m["addr"]),
				}
			}
		}
		serversMutex.Unlock()
	}

	// choose initial coordinator = MIN rank
	{
		minName := meuNome
		minRank := meuRank

		serversMutex.Lock()
		for name, info := range serversList {
			r := getInt(info["rank"])
			if r < minRank {
				minRank = r
				minName = name
			}
		}
		coordMutex.Lock()
		coordinator = minName
		coordMutex.Unlock()
		serversMutex.Unlock()

		logInfo("Coordenador inicial definido: %s (rank=%d)", coordinator, minRank)

		// if I'm the coordinator, announce
		if coordinator == meuNome {
			publicarCoordenador(pubSocket, meuNome)
		}
	}

	// heartbeat to REF
	go func() {
		for {
			_, _ = refRequest(map[string]interface{}{
				"service": "heartbeat",
				"user":    meuNome,
				"addr":    serverAddr,
			})
			time.Sleep(5 * time.Second)
		}
	}()
		// sincronizar lista de servidores com REF a cada 5s
go func() {
    for {
        time.Sleep(5 * time.Second)

        listResp, err := refRequest(map[string]interface{}{"service": "list"})
        if err != nil {
            continue
        }

        if l, ok := listResp["list"].([]interface{}); ok {
            serversMutex.Lock()
            updated := map[string]map[string]interface{}{}
            for _, it := range l {
                if m, ok2 := it.(map[string]interface{}); ok2 {
                    name := getString(m["name"])
                    updated[name] = map[string]interface{}{
                        "rank": getInt(m["rank"]),
                        "addr": getString(m["addr"]),
                    }
                }
            }
            serversList = updated
            serversMutex.Unlock()
        }
    }
}()

	// monitor coordinator liveness
	go func() {
		for {
			time.Sleep(4 * time.Second)

			coordMutex.Lock()
			current := coordinator
			coordMutex.Unlock()

			if current == meuNome || current == "" {
				continue
			}

			serversMutex.Lock()
			info, ok := serversList[current]
			serversMutex.Unlock()

			if !ok {
				// unknown coordinator -> start election
				startElection()
				continue
			}

			addr := getString(info["addr"])
			_, err := sendSrvReq(addr, "ping", map[string]interface{}{"from": meuNome}, 1200)
			if err != nil {
				logWarn("Coordenador %s parece inativo. Iniciando eleição...", current)
				startElection()
			}
		}
	}()

	// main loop: handle client/broker requests (repClient)
	for {
		reqMsg, err := repClient.RecvMessageBytes(0)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		if len(reqMsg) == 0 {
			continue
		}

		var req Envelope
		_ = msgpack.Unmarshal(reqMsg[0], &req)
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
				resp["description"] = "usuário inválido"
			} else {
				usersMutex.Lock()
				if _, exists := users[user]; !exists {
					users[user] = resp["timestamp"].(string)
					usersMutex.Unlock()

					persistUsers()
					publishReplicate("add_user", map[string]interface{}{
						"user":      user,
						"timestamp": resp["timestamp"].(string),
					})
				} else {
					usersMutex.Unlock()
				}
				resp["status"] = "sucesso"
			}

		case "channels":
			channelsMutex.Lock()
			list := []string{}
			for c := range channels {
				list = append(list, c)
			}
			channelsMutex.Unlock()
			resp["channels"] = list
			resp["status"] = "OK"

		case "channel":
			ch := getString(req.Data["name"])
			if ch == "" {
				resp["status"] = "erro"
				resp["description"] = "canal inválido"
			} else {
				channelsMutex.Lock()
				if channels[ch] {
					channelsMutex.Unlock()
					resp["status"] = "erro"
					resp["description"] = "canal já existe"
				} else {
					channels[ch] = true
					channelsMutex.Unlock()

					persistChannels()

					publishReplicate("add_channel", map[string]interface{}{
						"channel":   ch,
						"timestamp": resp["timestamp"].(string),
					})

					resp["status"] = "sucesso"
				}
			}

		case "publish":
			ch := getString(req.Data["channel"])
			user := getString(req.Data["user"])
			msg := getString(req.Data["message"])

			env := Envelope{
				Service: "publish",
				Data: map[string]interface{}{
					"user":      user,
					"channel":   ch,
					"message":   msg,
					"timestamp": resp["timestamp"],
				},
				Clock: incClock(),
			}

			packed, _ := msgpack.Marshal(env)
			if pubSocket != nil {
				if _, err := pubSocket.SendMessage(ch, packed); err != nil {
					logWarn("Erro publicando no proxy: %v", err)
				}
			}

			messages = append(messages, env)
			persistMessages()

			// Berkeley sync trigger
			msgCountSinceSyncMu.Lock()
			msgCountSinceSync++
			if msgCountSinceSync >= 10 {
				msgCountSinceSync = 0
				msgCountSinceSyncMu.Unlock()

				go func() {
					coordMutex.Lock()
					coord := coordinator
					coordMutex.Unlock()

					if coord == meuNome || coord == "" {
						return
					}

					serversMutex.Lock()
					info, ok := serversList[coord]
					serversMutex.Unlock()

					if !ok {
						return
					}

					addr := getString(info["addr"])
					// REQ para o coordenador
					rep, err := sendSrvReq(addr, "clock", map[string]interface{}{}, 2000)
					if err != nil {
						logWarn("Erro pedindo clock ao coordenador: %v", err)
						return
					}

					if coordTimeStr, ok := rep["time"].(string); ok {
						// ************ INÍCIO DA LÓGICA DE SINCRONIZAÇÃO BERKELEY ************
						coordTime, errT := time.Parse(time.RFC3339, coordTimeStr)
						if errT != nil {
							logWarn("Erro parseando tempo do coordenador: %v", errT)
							return
						}

						localTime := time.Now().UTC()
						offset := coordTime.Sub(localTime)
						newLocalTime := localTime.Add(offset)

						logInfo("Sincronização Berkeley: Coordenador=%s LocalAtual=%s Offset=%v NovoLocal=%s",
							coordTimeStr,
							localTime.Format(time.RFC3339),
							offset,
							newLocalTime.Format(time.RFC3339))
						// ************ FIM DA LÓGICA DE SINCRONIZAÇÃO BERKELEY ************
					}
				}()
			} else {
				msgCountSinceSyncMu.Unlock()
			}

			resp["status"] = "OK"

		case "users":
			usersMutex.Lock()
			list := []string{}
			for u := range users {
				list = append(list, u)
			}
			usersMutex.Unlock()
			resp["users"] = list
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

			packed, _ := msgpack.Marshal(env)
			if pubSocket != nil {
				if _, err := pubSocket.SendMessage(dst, packed); err != nil {
					logWarn("Erro publicando mensagem privada no proxy: %v", err)
				}
			}

			messages = append(messages, env)
			persistMessages()

			resp["status"] = "OK"

		case "election":
			// Request from another server asking election (we reply OK)
			data := req.Data
			// don't start election here; only reply OK
			updateClock(req.Clock)

			resp = map[string]interface{}{
				"election":  "OK",
				"timestamp": nowISO(),
			}

			// Optionally update coordinator if payload contains one (defensive)
			if coordName, ok := data["coordinator"].(string); ok && coordName != "" {
				coordMutex.Lock()
				coordinator = coordName
				coordMutex.Unlock()
				logInfo("Coordenador atualizado via REQ: %s", coordName)
			}

			outEnv := map[string]interface{}{
				"service": "election",
				"data":    resp,
				"clock":   incClock(),
			}

			out, _ := msgpack.Marshal(outEnv)
			_, _ = repClient.SendBytes(out, 0)
			continue // we've already replied explicitly

		default:
			resp["status"] = "erro"
			resp["message"] = "serviço desconhecido"
		}

		outEnv := map[string]interface{}{
			"service": req.Service,
			"data":    resp,
			"clock":   incClock(),
		}

		out, _ := msgpack.Marshal(outEnv)
		if repClient != nil {
			if _, err := repClient.SendBytes(out, 0); err != nil {
				logWarn("Erro respondendo cliente: %v", err)
			}
		}
	}
}
