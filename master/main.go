package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

type Node struct {
	ID       string    `json:"id"`
	Address  string    `json:"address"`
	Port     int       `json:"port"`
	Status   string    `json:"status"`
	LastSeen time.Time `json:"last_seen"`
	Load     int       `json:"load"`
	Capacity int       `json:"capacity"`
}

type ClusterManager struct {
	nodes map[string]*Node
	mutex sync.RWMutex
}

func NewClusterManager() *ClusterManager {
	return &ClusterManager{
		nodes: make(map[string]*Node),
	}
}

func (cm *ClusterManager) RegisterNode(id, address string, port int) error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	node := &Node{
		ID:       id,
		Address:  address,
		Port:     port,
		Status:   "active",
		LastSeen: time.Now(),
		Load:     0,
		Capacity: 100,
	}

	cm.nodes[id] = node
	log.Printf("✅ Нода %s зарегистрирована: %s:%d", id, address, port)
	return nil
}

func (cm *ClusterManager) GetActiveNodes() []*Node {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()

	var activeNodes []*Node
	for _, node := range cm.nodes {
		if node.Status == "active" {
			activeNodes = append(activeNodes, node)
		}
	}
	return activeNodes
}

func (cm *ClusterManager) UpdateNodeLoad(id string, load int) error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	node, exists := cm.nodes[id]
	if !exists {
		return fmt.Errorf("нода %s не найдена", id)
	}

	node.Load = load
	node.LastSeen = time.Now()
	return nil
}

type LoadBalancer struct {
	clusterManager *ClusterManager
	currentIndex   int
	mutex          sync.Mutex
}

func NewLoadBalancer(cm *ClusterManager) *LoadBalancer {
	return &LoadBalancer{
		clusterManager: cm,
		currentIndex:   0,
	}
}

func (lb *LoadBalancer) GetNextNode() *Node {
	lb.mutex.Lock()
	defer lb.mutex.Unlock()

	nodes := lb.clusterManager.GetActiveNodes()
	if len(nodes) == 0 {
		return nil
	}

	node := nodes[lb.currentIndex%len(nodes)]
	lb.currentIndex++
	return node
}

type HTTPServer struct {
	clusterManager *ClusterManager
	loadBalancer   *LoadBalancer
	port           int
}

func NewHTTPServer(cm *ClusterManager, lb *LoadBalancer, port int) *HTTPServer {
	return &HTTPServer{
		clusterManager: cm,
		loadBalancer:   lb,
		port:           port,
	}
}

func (hs *HTTPServer) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/cluster/status", hs.clusterStatusHandler)
	mux.HandleFunc("/api/cluster/nodes", hs.clusterNodesHandler)
	mux.HandleFunc("/api/cluster/register", hs.registerNodeHandler)
	mux.HandleFunc("/api/balancer/status", hs.balancerStatusHandler)

	mux.HandleFunc("/", hs.proxyHandler)

	addr := fmt.Sprintf(":%d", hs.port)
	log.Printf("🚀 HTTP сервер запущен на порту %d", hs.port)
	return http.ListenAndServe(addr, mux)
}

func (hs *HTTPServer) clusterStatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	nodes := hs.clusterManager.GetActiveNodes()
	status := map[string]interface{}{
		"total_nodes":  len(hs.clusterManager.nodes),
		"active_nodes": len(nodes),
		"health":       len(nodes) > 0,
		"timestamp":    time.Now(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (hs *HTTPServer) clusterNodesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	nodes := hs.clusterManager.GetActiveNodes()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nodes)
}

func (hs *HTTPServer) registerNodeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		ID      string `json:"id"`
		Address string `json:"address"`
		Port    int    `json:"port"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if err := hs.clusterManager.RegisterNode(req.ID, req.Address, req.Port); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "registered"})
}

func (hs *HTTPServer) balancerStatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := map[string]interface{}{
		"strategy":      "round_robin",
		"active_nodes":  len(hs.clusterManager.GetActiveNodes()),
		"current_index": hs.loadBalancer.currentIndex,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (hs *HTTPServer) proxyHandler(w http.ResponseWriter, r *http.Request) {
	node := hs.loadBalancer.GetNextNode()
	if node == nil {
		http.Error(w, "No available nodes", http.StatusServiceUnavailable)
		return
	}

	targetURL := fmt.Sprintf("http://%s:%d%s", node.Address, node.Port, r.URL.Path)

	response := map[string]interface{}{
		"message": "Request proxied to node",
		"node": map[string]interface{}{
			"id":      node.ID,
			"address": node.Address,
			"port":    node.Port,
			"load":    node.Load,
		},
		"target_url": targetURL,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

type SocketServer struct {
	clusterManager *ClusterManager
	port           int
}

func NewSocketServer(cm *ClusterManager, port int) *SocketServer {
	return &SocketServer{
		clusterManager: cm,
		port:           port,
	}
}

func (ss *SocketServer) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", ss.port))
	if err != nil {
		return err
	}
	defer listener.Close()

	log.Printf("🔌 Сокет сервер запущен на порту %d", ss.port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("❌ Ошибка принятия соединения: %v", err)
			continue
		}

		go ss.handleConnection(conn)
	}
}

func (ss *SocketServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	log.Printf("🔗 Новое соединение от %s", conn.RemoteAddr())

	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		log.Printf("❌ Ошибка чтения: %v", err)
		return
	}

	var msg map[string]interface{}
	if err := json.Unmarshal(buffer[:n], &msg); err != nil {
		log.Printf("❌ Ошибка парсинга JSON: %v", err)
		return
	}

	ss.handleMessage(msg, conn)
}

func (ss *SocketServer) handleMessage(msg map[string]interface{}, conn net.Conn) {
	msgType, ok := msg["type"].(string)
	if !ok {
		log.Printf("❌ Неизвестный тип сообщения")
		return
	}

	switch msgType {
	case "register":
		ss.handleRegister(msg, conn)
	case "heartbeat":
		ss.handleHeartbeat(msg, conn)
	case "load_update":
		ss.handleLoadUpdate(msg, conn)
	default:
		log.Printf("❌ Неизвестный тип сообщения: %s", msgType)
	}
}

func (ss *SocketServer) handleRegister(msg map[string]interface{}, conn net.Conn) {
	id, _ := msg["id"].(string)
	address, _ := msg["address"].(string)
	port, _ := msg["port"].(float64)

	if id == "" || address == "" || port == 0 {
		log.Printf("❌ Неполные данные регистрации")
		return
	}

	remoteAddr := conn.RemoteAddr().String()
	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil && host != "" {
		address = host
	}

	err = ss.clusterManager.RegisterNode(id, address, int(port))
	if err != nil {
		log.Printf("❌ Ошибка регистрации ноды: %v", err)
		return
	}

	response := map[string]string{"status": "registered"}
	responseBytes, _ := json.Marshal(response)
	conn.Write(responseBytes)

	log.Printf("✅ Нода %s успешно зарегистрирована", id)
}

func (ss *SocketServer) handleHeartbeat(msg map[string]interface{}, conn net.Conn) {
	id, _ := msg["id"].(string)
	if id == "" {
		return
	}

	ss.clusterManager.mutex.Lock()
	if node, exists := ss.clusterManager.nodes[id]; exists {
		node.LastSeen = time.Now()
	}
	ss.clusterManager.mutex.Unlock()

	response := map[string]string{"status": "ok"}
	responseBytes, _ := json.Marshal(response)
	conn.Write(responseBytes)
}

func (ss *SocketServer) handleLoadUpdate(msg map[string]interface{}, conn net.Conn) {
	id, _ := msg["id"].(string)
	load, _ := msg["load"].(float64)

	if id == "" {
		return
	}

	err := ss.clusterManager.UpdateNodeLoad(id, int(load))
	if err != nil {
		log.Printf("❌ Ошибка обновления нагрузки: %v", err)
		return
	}

	response := map[string]string{"status": "updated"}
	responseBytes, _ := json.Marshal(response)
	conn.Write(responseBytes)
}

func main() {
	log.Println("🚀 Запуск центрального сервера...")

	clusterManager := NewClusterManager()

	loadBalancer := NewLoadBalancer(clusterManager)

	httpServer := NewHTTPServer(clusterManager, loadBalancer, 8080)
	go func() {
		if err := httpServer.Start(); err != nil {
			log.Fatalf("❌ Ошибка HTTP сервера: %v", err)
		}
	}()

	socketServer := NewSocketServer(clusterManager, 8081)
	if err := socketServer.Start(); err != nil {
		log.Fatalf("❌ Ошибка сокет сервера: %v", err)
	}
}
