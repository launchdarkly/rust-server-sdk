mod client_entity;
mod command_params;

use crate::command_params::CommandParams;
use actix_web::error::{ErrorBadRequest, ErrorInternalServerError};
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer, Responder, Result};
use client_entity::ClientEntity;
#[cfg(any(feature = "hypertls", feature = "rustls"))]
use eventsource_client::{https_connector, HttpsConnector};
use futures::executor;
use launchdarkly_server_sdk::Reference;
use serde::{self, Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{mpsc, Mutex};
use std::thread;

#[derive(Serialize)]
struct Status {
    capabilities: Vec<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct StreamingParameters {
    pub base_uri: Option<String>,
    pub initial_retry_delay_ms: Option<u64>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PollingParameters {
    pub base_uri: Option<String>,
    pub poll_interval_ms: Option<u64>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EventParameters {
    pub base_uri: Option<String>,
    pub capacity: Option<usize>,
    pub enable_diagnostics: bool,
    #[serde(default = "bool::default")]
    pub all_attributes_private: bool,
    pub global_private_attributes: Option<HashSet<Reference>>,
    pub flush_interval_ms: Option<u64>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TagParams {
    pub application_id: Option<String>,
    pub application_version: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ServiceEndpointParameters {
    pub streaming: Option<String>,
    pub polling: Option<String>,
    pub events: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Configuration {
    pub credential: String,

    pub start_wait_time_ms: Option<u64>,

    #[serde(default = "bool::default")]
    pub init_can_fail: bool,

    pub streaming: Option<StreamingParameters>,

    pub polling: Option<PollingParameters>,

    pub events: Option<EventParameters>,

    pub tags: Option<TagParams>,

    pub service_endpoints: Option<ServiceEndpointParameters>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct CreateInstanceParams {
    pub tag: Option<String>,

    pub configuration: Configuration,
}

async fn status() -> impl Responder {
    web::Json(Status {
        capabilities: vec![
            "server-side".to_string(),
            "server-side-polling".to_string(),
            "strongly-typed".to_string(),
            "all-flags-with-reasons".to_string(),
            "all-flags-client-side-only".to_string(),
            "all-flags-details-only-for-tracked-flags".to_string(),
            "tags".to_string(),
            "service-endpoints".to_string(),
            "context-type".to_string(),
        ],
    })
}

async fn shutdown(stopper: web::Data<mpsc::Sender<()>>) -> HttpResponse {
    match stopper.send(()) {
        Ok(_) => HttpResponse::NoContent().finish(),
        Err(_) => HttpResponse::InternalServerError().body("Unable to send shutdown signal"),
    }
}

async fn create_client(
    req: HttpRequest,
    create_instance_params: web::Json<CreateInstanceParams>,
    app_state: web::Data<AppState>,
) -> HttpResponse {
    let client_entity = match ClientEntity::new(
        create_instance_params.into_inner(),
        &app_state.https_connector,
    )
    .await
    {
        Ok(ce) => ce,
        Err(e) => return HttpResponse::InternalServerError().body(format!("{}", e)),
    };

    let mut counter = match app_state.counter.lock() {
        Ok(c) => c,
        Err(_) => return HttpResponse::InternalServerError().body("Unable to retrieve counter"),
    };

    let mut entities = match app_state.client_entities.lock() {
        Ok(h) => h,
        Err(_) => {
            return HttpResponse::InternalServerError().body("Unable to lock client_entities")
        }
    };

    *counter += 1;
    let client_resource = match req.url_for("client_path", &[counter.to_string()]) {
        Ok(sr) => sr,
        Err(_) => {
            return HttpResponse::InternalServerError()
                .body("Unable to generate client response URL")
        }
    };
    entities.insert(*counter, client_entity);

    let mut response = HttpResponse::Ok();
    response.insert_header(("Location", client_resource.to_string()));
    response.finish()
}

async fn do_command(
    req: HttpRequest,
    command_params: web::Json<CommandParams>,
    app_state: web::Data<AppState>,
) -> Result<HttpResponse> {
    let client_id = req
        .match_info()
        .get("id")
        .ok_or_else(|| ErrorBadRequest("Did not provide ID in URL"))?;

    let client_id = client_id.parse::<u32>().map_err(ErrorInternalServerError)?;

    let entities = app_state
        .client_entities
        .lock()
        .expect("Client entities cannot be locked");

    let entity = entities
        .get(&client_id)
        .ok_or_else(|| ErrorBadRequest("The specified client does not exist"))?;

    let result = entity
        .do_command(command_params.into_inner())
        .map_err(ErrorBadRequest)?;

    match result {
        Some(response) => Ok(HttpResponse::Ok().json(response)),
        None => Ok(HttpResponse::Accepted().finish()),
    }
}

async fn stop_client(req: HttpRequest, app_state: web::Data<AppState>) -> HttpResponse {
    if let Some(client_id) = req.match_info().get("id") {
        let client_id: u32 = match client_id.parse() {
            Ok(id) => id,
            Err(_) => return HttpResponse::BadRequest().body("Unable to parse client id"),
        };

        match app_state.client_entities.lock() {
            Ok(mut entities) => {
                entities.remove(&client_id);
            }
            Err(_) => {
                return HttpResponse::InternalServerError().body("Unable to retrieve handles")
            }
        };

        HttpResponse::NoContent().finish()
    } else {
        HttpResponse::BadRequest().body("No client id was provided in the URL")
    }
}

struct AppState {
    counter: Mutex<u32>,
    client_entities: Mutex<HashMap<u32, ClientEntity>>,
    https_connector: HttpsConnector,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    let (tx, rx) = mpsc::channel::<()>();

    let state = web::Data::new(AppState {
        counter: Mutex::new(0),
        client_entities: Mutex::new(HashMap::new()),
        https_connector: {
            #[cfg(not(any(feature = "hypertls", feature = "rustls")))]
            {
                compile_error!("one of the { \"hypertls\", \"rustls\" } features must be enabled");
            }
            #[cfg(any(feature = "hypertls", feature = "rustls"))]
            {
                https_connector()
            }
        },
    });

    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(tx.clone()))
            .app_data(state.clone())
            .route("/", web::get().to(status))
            .route("/", web::post().to(create_client))
            .route("/", web::delete().to(shutdown))
            .service(
                web::resource("/{id}")
                    .name("client_path")
                    .route(web::post().to(do_command))
                    .route(web::delete().to(stop_client)),
            )
    })
    .bind("127.0.0.1:8000")?
    .run();

    let handle = server.handle();

    thread::spawn(move || {
        // wait for shutdown signal
        if let Ok(()) = rx.recv() {
            executor::block_on(handle.stop(true))
        }
    });

    // run server
    server.await
}
