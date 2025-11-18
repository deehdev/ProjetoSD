// server.go
package main

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
	"github.com/vmihailenco/msgpack/v5"
)

// ================================
//   Variáveis Globais
// ================================

var (
	serverName  string
	serverRank  int
	clock       int
	coordinator string

	serversMutex sync.Mutex
	serversList  = make(map[string]int)

	usersMutex sync.Mutex
	users      = make(map[string]string)

	channelsMutex sync.Mutex
	channels      = make(map[string]bool)

	messagesMutex sync.Mutex
	messages      = []Envelope{}

	refAddr       string
	brokerAddr    string
	proxyPubAddr  string
	proxySubAddr  string
	isCoordinator bool

	pubSocket *zmq.Socket
	subSocket *zmq.Socket
)

// ================================
//   Estruturas
// ================================

type Envelope struct {
	Service string                 `msgpack:"service"`
	Data    map[string]interface{} `msgpack:"data"`
}

type ReplicateMsg struct {
	Origin    string                 `json:"origin"`
	Action    string                 `json:"action"`
	Payload   map[string]interface{} `json:"payload"`
	Timestamp string                 `json:"timestamp"`
	Clock     int                    `json:"clock"`
}

// ================================
//   Relógio lógico
// ================================

func incClock() int {
	clock++
	return clock
}

func updateClock(received int) {
	if received > clock {
		clock = received
	}
}

// ================================
//   Auxiliares
// ================================

func randomString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
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

func ensureDataDir() {
	if _, err := os.Stat("data"); os.IsNotExist(err) {
		_ = os.Mkdir("data", 0755)
	}
}

// ================================
//   Persistência
// ================================

func saveJSON(path string, v interface{}) {
	b, _ := json.MarshalIndent(v, "", "  ")
	_ = ioutil.WriteFile(path, b, 0644)
}

func loadJSON(path string, v interface{}) error {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func persistUsers() {
	usersMutex.Lock()
	out := []map[string]string{}
	for u, ts := range users {
		out = append(out, map[string]string{"user": u, "timestamp": ts})
	}
	usersMutex.Unlock()
	saveJSON("data/users.json", out)
}

func persistChannels() {
	channelsMutex.Lock()
	out := []string{}
	for c := range channels {
		out = append(out, c)
	}
	channelsMutex.Unlock()
	saveJSON("data/channels.json", out)
}

func persistMessages() {
	messagesMutex.Lock()
	saveJSON("data/messages.json", messages)
	messagesMutex.Unlock()
}

func loadPersistentState() {
	ensureDataDir()

	var ul []map[string]string
	if loadJSON("data/users.json", &ul) == nil {
		for _, u := range ul {
			users[u["user"]] = u["timestamp"]
		}
	}

	var cl []string
	if loadJSON("data/channels.json", &cl) == nil {
		for _, c := range cl {
			channels[c] = true
		}
	}

	var ml []Envelope
	if loadJSON("data/messages.json", &ml) == nil {
		messages = ml
	}
}

// ================================
//   Replicação
// ================================

func publishReplicate(action string, payload map[string]interface{}) {
	rm := ReplicateMsg{
		Origin:    serverName,
		Action:    action,
		Payload:   payload,
		Timestamp: time.Now().Format(time.RFC3339),
		Clock:     incClock(),
	}
	b, _ := json.Marshal(rm)
	pubSocket.SendMessage("replicate", b)
}

func applyReplication(data []byte) {
	var rm ReplicateMsg
	if json.Unmarshal(data, &rm) != nil {
		return
	}
	if rm.Origin == serverName {
		return
	}

	updateClock(rm.Clock)

	switch rm.Action {

	case "add_user":
		usersMutex.Lock()
		if _, exists := users[rm.Payload["user"].(string)]; !exists {
			users[rm.Payload["user"].(string)] = rm.Payload["timestamp"].(string)
			usersMutex.Unlock()
			persistUsers()
		} else {
			usersMutex.Unlock()
		}

	case "add_channel":
		ch := rm.Payload["channel"].(string)
		channelsMutex.Lock()
		if !channels[ch] {
			channels[ch] = true
			channelsMutex.Unlock()
			persistChannels()
		} else {
			channelsMutex.Unlock()
		}

	case "add_message":
		messagesMutex.Lock()
		messages = append(messages, Envelope{
			Service: rm.Payload["service"].(string),
			Data:    rm.Payload["data"].(map[string]interface{}),
		})
		messagesMutex.Unlock()
		persistMessages()
	}
}

// ================================
//   SUB Listener
// ================================

func subListener() {
	for {
		msg, err := subSocket.RecvMessageBytes(0)
		if err != nil || len(msg) < 2 {
			continue
		}
		topic := string(msg[0])
		body := msg[1]

		if topic == "replicate" {
			applyReplication(body)
			continue
		}

		var env Envelope
		if msgpack.Unmarshal(body, &env) != nil {
			continue
		}

		if origin, ok := env.Data["origin"].(string); ok && origin == serverName {
			continue
		}

		messagesMutex.Lock()
		messages = append(messages, env)
		messagesMutex.Unlock()
		persistMessages()
	}
}

// ================================
//   Processar REQ → REP
// ================================

func handleRequest(b []byte) ([]byte, error) {
	var req Envelope
	if msgpack.Unmarshal(b, &req) != nil {
		return msgpack.Marshal(Envelope{
			Service: "error",
			Data:    map[string]interface{}{"status": "erro", "message": "msgpack inválido"},
		})
	}

	recvClock := int(getClock(req.Data["clock"]))
	updateClock(recvClock)

	resp := Envelope{
		Service: req.Service,
		Data: map[string]interface{}{
			"timestamp": time.Now().Format(time.RFC3339),
			"clock":     incClock(),
		},
	}

	switch req.Service {

	// ------------------------------------
	// LOGIN
	// ------------------------------------
	case "login":
		user := getString(req.Data["user"])
		if user == "" {
			resp.Data["status"] = "erro"
			resp.Data["description"] = "usuário inválido"
			break
		}

		usersMutex.Lock()
		_, exists := users[user]
		if exists {
			usersMutex.Unlock()
			resp.Data["status"] = "erro"
			resp.Data["description"] = "usuário já existe"
			break
		}

		users[user] = resp.Data["timestamp"].(string)
		usersMutex.Unlock()
		persistUsers()

		publishReplicate("add_user", map[string]interface{}{
			"user":      user,
			"timestamp": resp.Data["timestamp"].(string),
		})

		resp.Data["status"] = "sucesso"

	// ------------------------------------
	// USERS
	// ------------------------------------
	case "users":
		usersMutex.Lock()
		list := []string{}
		for u := range users {
			list = append(list, u)
		}
		usersMutex.Unlock()
		resp.Data["users"] = list

	// ------------------------------------
	// CREATE CHANNEL
	// ------------------------------------
	case "channel":
		ch := getString(req.Data["channel"])
		if ch == "" {
			resp.Data["status"] = "erro"
			resp.Data["description"] = "canal inválido"
			break
		}

		channelsMutex.Lock()
		if channels[ch] {
			channelsMutex.Unlock()
			resp.Data["status"] = "erro"
			resp.Data["description"] = "canal já existe"
			break
		}

		channels[ch] = true
		channelsMutex.Unlock()
		persistChannels()

		publishReplicate("add_channel", map[string]interface{}{
			"channel":   ch,
			"timestamp": resp.Data["timestamp"].(string),
		})

		resp.Data["status"] = "sucesso"

	// ------------------------------------
	// LIST CHANNELS
	// ------------------------------------
	case "channels":
		channelsMutex.Lock()
		list := []string{}
		for c := range channels {
			list = append(list, c)
		}
		channelsMutex.Unlock()
		resp.Data["channels"] = list

	// ------------------------------------
	// PUBLISH MESSAGE
	// ------------------------------------
	case "publish":
		user := getString(req.Data["user"])
		channel := getString(req.Data["channel"])
		message := getString(req.Data["message"])

		channelsMutex.Lock()
		exists := channels[channel]
		channelsMutex.Unlock()

		if !exists {
			resp.Data["status"] = "erro"
			resp.Data["message"] = "canal não existe"
			break
		}

		env := Envelope{
			Service: "publish",
			Data: map[string]interface{}{
				"user":      user,
				"channel":   channel,
				"message":   message,
				"timestamp": resp.Data["timestamp"].(string),
				"clock":     incClock(),
				"origin":    serverName,
			},
		}

		packed, _ := msgpack.Marshal(env)
		pubSocket.SendMessage(channel, packed)

		messagesMutex.Lock()
		messages = append(messages, env)
		messagesMutex.Unlock()
		persistMessages()

		resp.Data["status"] = "OK"

	// ------------------------------------
	// PRIVATE MESSAGE
	// ------------------------------------
	case "message":
		dst := getString(req.Data["dst"])

		usersMutex.Lock()
		_, existsUser := users[dst]
		usersMutex.Unlock()

		if !existsUser {
			resp.Data["status"] = "erro"
			resp.Data["message"] = "usuário não existe"
			break
		}

		env := Envelope{
			Service: "message",
			Data: map[string]interface{}{
				"src":       getString(req.Data["src"]),
				"dst":       dst,
				"message":   getString(req.Data["message"]),
				"timestamp": resp.Data["timestamp"].(string),
				"clock":     incClock(),
				"origin":    serverName,
			},
		}

		packed, _ := msgpack.Marshal(env)
		pubSocket.SendMessage(dst, packed)

		messagesMutex.Lock()
		messages = append(messages, env)
		messagesMutex.Unlock()
		persistMessages()

		resp.Data["status"] = "OK"

	default:
		resp.Data["status"] = "erro"
		resp.Data["message"] = "serviço desconhecido"
	}

	return msgpack.Marshal(resp)
}

func getClock(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	}
	return 0
}

// ================================
//   MAIN
// ================================

func main() {
	rand.Seed(time.Now().UnixNano())
	serverName = "server-" + randomString(4)

	refAddr = os.Getenv("REF_ADDR")
	brokerAddr = os.Getenv("BROKER_DEALER_ADDR")
	proxyPubAddr = os.Getenv("PROXY_PUB_ADDR")
	proxySubAddr = os.Getenv("PROXY_SUB_ADDR")

	loadPersistentState()

	// REP socket
	rep, _ := zmq.NewSocket(zmq.REP)
	rep.Connect(brokerAddr)

	// PUB → proxy
	pubSocket, _ = zmq.NewSocket(zmq.PUB)
	pubSocket.Connect(proxyPubAddr)

	// SUB ← proxy
	subSocket, _ = zmq.NewSocket(zmq.SUB)
	subSocket.SetSubscribe("")
	subSocket.Connect(proxySubAddr)
	go subListener()

	for {
		req, _ := rep.RecvMessageBytes(0)
		resp, _ := handleRequest(req[0])
		rep.SendBytes(resp, 0)
	}
}
