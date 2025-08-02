use axum::{
    extract::State,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration, sleep};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower_http::cors::CorsLayer;
use tracing::{info, error};
use uuid::Uuid;

#[derive(Clone)]
struct NodeState {
    id: String,
    port: u16,
    load: Arc<Mutex<i32>>,
    master_address: String,
    master_port: u16,
}

#[derive(Serialize, Deserialize)]
struct RegisterMessage {
    #[serde(rename = "type")]
    message_type: String,
    id: String,
    address: String,
    port: u16,
}

#[derive(Serialize, Deserialize)]
struct HeartbeatMessage {
    #[serde(rename = "type")]
    message_type: String,
    id: String,
}

#[derive(Serialize, Deserialize)]
struct LoadUpdateMessage {
    #[serde(rename = "type")]
    message_type: String,
    id: String,
    load: i32,
}

#[derive(Serialize, Deserialize)]
struct ServerResponse {
    status: String,
}

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    node_id: String,
    load: i32,
    uptime: u64,
}

#[derive(Serialize)]
struct InfoResponse {
    node_id: String,
    port: u16,
    load: i32,
    capacity: i32,
    master_address: String,
}

#[derive(Serialize)]
struct StatusResponse {
    status: String,
    node_id: String,
    load: i32,
    active_connections: usize,
}

static mut START_TIME: u64 = 0;

fn get_uptime() -> u64 {
    unsafe {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        current_time - START_TIME
    }
}

async fn wait_for_master(master_address: &str, master_port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("{}:{}", master_address, master_port);
    let mut attempts = 0;
    let max_attempts = 30;
    
    while attempts < max_attempts {
        match TcpStream::connect(&addr).await {
            Ok(_) => {
                info!("✅ Мастер готов!");
                return Ok(());
            }
            Err(e) => {
                attempts += 1;
                info!("⏳ Ожидание мастера... (попытка {}/{}): {}", attempts, max_attempts, e);
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
    
    Err("Мастер не готов после всех попыток".into())
}

async fn send_to_master(message: &str) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("{}:{}", "master", 8081);
    let stream = TcpStream::connect(addr).await?;
    
    let (mut read, mut write) = stream.into_split();
    
    write.write_all(message.as_bytes()).await?;
    write.shutdown().await?;
    
    let mut buffer = [0; 1024];
    let n = read.read(&mut buffer).await?;
    if n > 0 {
        let response = String::from_utf8_lossy(&buffer[..n]);
        info!("Ответ от мастера: {}", response);
    }
    
    Ok(())
}

async fn register_node(state: &NodeState) -> Result<(), Box<dyn std::error::Error>> {
    let message = RegisterMessage {
        message_type: "register".to_string(),
        id: state.id.clone(),
        address: "0.0.0.0".to_string(),
        port: state.port,
    };
    
    let message_json = serde_json::to_string(&message)?;
    send_to_master(&message_json).await?;
    
    info!("✅ Нода зарегистрирована в кластере");
    Ok(())
}

async fn send_heartbeat(state: &NodeState) -> Result<(), Box<dyn std::error::Error>> {
    let message = HeartbeatMessage {
        message_type: "heartbeat".to_string(),
        id: state.id.clone(),
    };
    
    let message_json = serde_json::to_string(&message)?;
    send_to_master(&message_json).await?;
    
    Ok(())
}

async fn send_load_update(state: &NodeState) -> Result<(), Box<dyn std::error::Error>> {
    let load = *state.load.lock().await;
    let message = LoadUpdateMessage {
        message_type: "load_update".to_string(),
        id: state.id.clone(),
        load,
    };
    
    let message_json = serde_json::to_string(&message)?;
    send_to_master(&message_json).await?;
    
    Ok(())
}

async fn health_handler(State(state): State<NodeState>) -> Json<HealthResponse> {
    let load = *state.load.lock().await;
    let uptime = get_uptime();
    
    Json(HealthResponse {
        status: "healthy".to_string(),
        node_id: state.id.clone(),
        load,
        uptime,
    })
}

async fn info_handler(State(state): State<NodeState>) -> Json<InfoResponse> {
    let load = *state.load.lock().await;
    
    Json(InfoResponse {
        node_id: state.id.clone(),
        port: state.port,
        load,
        capacity: 100,
        master_address: state.master_address.clone(),
    })
}

async fn status_handler(State(state): State<NodeState>) -> Json<StatusResponse> {
    let load = *state.load.lock().await;
    
    Json(StatusResponse {
        status: "active".to_string(),
        node_id: state.id.clone(),
        load,
        active_connections: 0,
    })
}

async fn root_handler(State(state): State<NodeState>) -> Json<HashMap<String, String>> {
    let mut response = HashMap::new();
    response.insert("message".to_string(), "Worker node is running".to_string());
    response.insert("node_id".to_string(), state.id.clone());
    response.insert("port".to_string(), state.port.to_string());
    
    Json(response)
}

async fn simulate_load(state: &NodeState) {
    let mut interval = interval(Duration::from_secs(5));
    
    loop {
        interval.tick().await;
        
        let new_load = rand::random::<i32>() % 100;
        *state.load.lock().await = new_load;
        
        info!("📊 Нагрузка обновлена: {}", new_load);
        
        if let Err(e) = send_load_update(state).await {
            error!("❌ Ошибка отправки обновления нагрузки: {}", e);
        }
    }
}

async fn heartbeat_loop(state: &NodeState) {
    let mut interval = interval(Duration::from_secs(10));
    
    loop {
        interval.tick().await;
        
        if let Err(e) = send_heartbeat(state).await {
            error!("❌ Ошибка отправки heartbeat: {}", e);
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    
    unsafe {
        START_TIME = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
    
    info!("🚀 Запуск рабочей ноды...");
    
    let node_id = Uuid::new_v4().to_string();
    let port = 9000;
    
    let state = NodeState {
        id: node_id.clone(),
        port,
        load: Arc::new(Mutex::new(0)),
        master_address: "master".to_string(),
        master_port: 8081,
    };
    
    info!("📋 ID ноды: {}", node_id);
    info!("🔌 Порт: {}", port);
    info!("🎯 Мастер: {}:{}", state.master_address, state.master_port);
    
    info!("⏳ Ожидание готовности мастера...");
    if let Err(e) = wait_for_master(&state.master_address, state.master_port).await {
        error!("❌ Мастер не готов: {}", e);
        return;
    }
    
    if let Err(e) = register_node(&state).await {
        error!("❌ Ошибка регистрации: {}", e);
    }
    
    let state_clone = state.clone();
    tokio::spawn(async move {
        simulate_load(&state_clone).await;
    });
    
    let state_clone = state.clone();
    tokio::spawn(async move {
        heartbeat_loop(&state_clone).await;
    });
    
    let cors = CorsLayer::permissive();
    
    let app = Router::new()
        .route("/", get(root_handler))
        .route("/api/health", get(health_handler))
        .route("/api/info", get(info_handler))
        .route("/api/status", get(status_handler))
        .layer(cors)
        .with_state(state);
    
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("🌐 HTTP сервер запущен на {}", addr);
    
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
} 