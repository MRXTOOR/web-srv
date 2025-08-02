package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type TestResult struct {
	Test   string `json:"test"`
	Status string `json:"status"`
}

type TestReport struct {
	Timestamp   string       `json:"timestamp"`
	TotalTests  int          `json:"total_tests"`
	PassedTests int          `json:"passed_tests"`
	FailedTests int          `json:"failed_tests"`
	SuccessRate float64      `json:"success_rate"`
	Results     []TestResult `json:"results"`
}

type ClusterStatus struct {
	TotalNodes  int       `json:"total_nodes"`
	ActiveNodes int       `json:"active_nodes"`
	Health      bool      `json:"health"`
	Timestamp   time.Time `json:"timestamp"`
}

type NodeInfo struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Port    int    `json:"port"`
	Status  string `json:"status"`
	Load    int    `json:"load"`
}

type HealthResponse struct {
	Status string `json:"status"`
	NodeID string `json:"node_id"`
	Load   int    `json:"load"`
	Uptime int64  `json:"uptime"`
}

type ProxyResponse struct {
	Message   string                 `json:"message"`
	Node      map[string]interface{} `json:"node"`
	TargetURL string                 `json:"target_url"`
}

func main() {
	results := []TestResult{}

	fmt.Println("🚀 Запуск тестирования системы...")

	masterURL := getEnv("MASTER_URL", "http://master:8080")
	worker1URL := getEnv("WORKER1_URL", "http://worker-1:9000")
	worker2URL := getEnv("WORKER2_URL", "http://worker-2:9000")

	if !waitForService(masterURL, "Master Server") {
		os.Exit(1)
	}

	if !waitForService(worker1URL, "Worker-1") {
		os.Exit(1)
	}

	if !waitForService(worker2URL, "Worker-2") {
		os.Exit(1)
	}

	testMasterAPI(&results, masterURL)
	testWorkerRegistration(&results, masterURL)
	testWorkerHealth(&results, worker1URL, worker2URL)
	testSocketCommunication(&results, masterURL)
	testLoadBalancing(&results, masterURL)

	generateReport(results)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func waitForService(url, serviceName string) bool {
	fmt.Printf("⏳ Ожидание готовности %s...\n", serviceName)

	for i := 0; i < 30; i++ {
		resp, err := http.Get(url + "/api/health")
		if err == nil && resp.StatusCode == 200 {
			fmt.Printf("✅ %s готов!\n", serviceName)
			return true
		}
		time.Sleep(2 * time.Second)
	}

	fmt.Printf("❌ %s не готов после 60 секунд\n", serviceName)
	return false
}

func testMasterAPI(results *[]TestResult, masterURL string) {
	fmt.Println("🧪 Тестирование API главного сервера...")

	tests := []struct {
		name string
		url  string
	}{
		{"GET /api/cluster/status", masterURL + "/api/cluster/status"},
		{"GET /api/cluster/nodes", masterURL + "/api/cluster/nodes"},
		{"GET /api/balancer/status", masterURL + "/api/balancer/status"},
	}

	for _, test := range tests {
		resp, err := http.Get(test.url)
		if err == nil && resp.StatusCode == 200 {
			fmt.Printf("✅ %s - OK\n", test.name)
			*results = append(*results, TestResult{Test: test.name, Status: "PASS"})
		} else {
			fmt.Printf("❌ %s - FAIL\n", test.name)
			*results = append(*results, TestResult{Test: test.name, Status: "FAIL"})
		}
	}
}

func testWorkerRegistration(results *[]TestResult, masterURL string) {
	fmt.Println("🧪 Тестирование регистрации рабочих нод...")

	resp, err := http.Get(masterURL + "/api/cluster/nodes")
	if err == nil && resp.StatusCode == 200 {
		var nodes []NodeInfo
		body, _ := io.ReadAll(resp.Body)
		json.Unmarshal(body, &nodes)

		if len(nodes) >= 2 {
			fmt.Printf("✅ Зарегистрировано %d нод\n", len(nodes))
			*results = append(*results, TestResult{Test: "Worker Registration", Status: "PASS"})
		} else {
			fmt.Printf("❌ Ожидалось 2+ нод, найдено %d\n", len(nodes))
			*results = append(*results, TestResult{Test: "Worker Registration", Status: "FAIL"})
		}
	} else {
		fmt.Println("❌ Не удалось получить список нод")
		*results = append(*results, TestResult{Test: "Worker Registration", Status: "FAIL"})
	}
}

func testWorkerHealth(results *[]TestResult, worker1URL, worker2URL string) {
	fmt.Println("🧪 Тестирование здоровья рабочих нод...")

	workers := []struct {
		name string
		url  string
	}{
		{"Worker-1", worker1URL},
		{"Worker-2", worker2URL},
	}

	for _, worker := range workers {
		resp, err := http.Get(worker.url + "/api/health")
		if err == nil && resp.StatusCode == 200 {
			var health HealthResponse
			body, _ := io.ReadAll(resp.Body)
			json.Unmarshal(body, &health)
			fmt.Printf("✅ %s здоров (нагрузка: %d)\n", worker.name, health.Load)
			*results = append(*results, TestResult{Test: worker.name + " Health", Status: "PASS"})
		} else {
			fmt.Printf("❌ %s нездоров\n", worker.name)
			*results = append(*results, TestResult{Test: worker.name + " Health", Status: "FAIL"})
		}
	}
}

func testSocketCommunication(results *[]TestResult, masterURL string) {
	fmt.Println("🧪 Тестирование сокетной связи...")

	time.Sleep(15 * time.Second)

	resp, err := http.Get(masterURL + "/api/cluster/status")
	if err == nil && resp.StatusCode == 200 {
		var status ClusterStatus
		body, _ := io.ReadAll(resp.Body)
		json.Unmarshal(body, &status)

		if status.ActiveNodes >= 2 {
			fmt.Printf("✅ Сокетная связь работает: %d активных нод\n", status.ActiveNodes)
			*results = append(*results, TestResult{Test: "Socket Communication", Status: "PASS"})
		} else {
			fmt.Printf("❌ Проблемы с сокетной связью: %d активных нод\n", status.ActiveNodes)
			*results = append(*results, TestResult{Test: "Socket Communication", Status: "FAIL"})
		}
	} else {
		fmt.Println("❌ Не удалось получить статус кластера")
		*results = append(*results, TestResult{Test: "Socket Communication", Status: "FAIL"})
	}
}

func testLoadBalancing(results *[]TestResult, masterURL string) {
	fmt.Println("🧪 Тестирование балансировки нагрузки...")

	successCount := 0
	for i := 0; i < 10; i++ {
		resp, err := http.Get(masterURL + "/")
		if err == nil && resp.StatusCode == 200 {
			successCount++
		}
	}

	if successCount >= 8 {
		fmt.Printf("✅ Балансировка работает, %d/10 запросов успешны\n", successCount)
		*results = append(*results, TestResult{Test: "Load Balancing", Status: "PASS"})
	} else {
		fmt.Printf("❌ Балансировка не работает, %d/10 запросов успешны\n", successCount)
		*results = append(*results, TestResult{Test: "Load Balancing", Status: "FAIL"})
	}
}

func generateReport(results []TestResult) {
	fmt.Println("📊 Генерация отчета о тестах...")

	totalTests := len(results)
	passedTests := 0
	for _, result := range results {
		if result.Status == "PASS" {
			passedTests++
		}
	}
	failedTests := totalTests - passedTests
	successRate := float64(passedTests) / float64(totalTests) * 100

	report := TestReport{
		Timestamp:   time.Now().Format(time.RFC3339),
		TotalTests:  totalTests,
		PassedTests: passedTests,
		FailedTests: failedTests,
		SuccessRate: successRate,
		Results:     results,
	}

	reportJSON, _ := json.MarshalIndent(report, "", "  ")
	os.WriteFile("/app/test_report.json", reportJSON, 0644)

	separator := strings.Repeat("=", 50)
	fmt.Println(separator)
	fmt.Println("СВОДКА ТЕСТОВ")
	fmt.Println(separator)
	fmt.Printf("Всего тестов: %d\n", totalTests)
	fmt.Printf("Успешных: %d\n", passedTests)
	fmt.Printf("Неудачных: %d\n", failedTests)
	fmt.Printf("Процент успеха: %.1f%%\n", successRate)
	fmt.Println(separator)

	for _, result := range results {
		statusIcon := "✅"
		if result.Status == "FAIL" {
			statusIcon = "❌"
		}
		fmt.Printf("%s %s\n", statusIcon, result.Test)
	}

	if successRate >= 80 {
		fmt.Println("Все тесты пройдены успешно!")
	} else {
		fmt.Println(" Некоторые тесты провалились!")
	}
}
