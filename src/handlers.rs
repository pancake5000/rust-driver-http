use hyper::{Body, Method, Request, Response, StatusCode};
use scylla::observability::history::HistoryCollector;
use url::form_urlencoded;

use scylla::client::execution_profile::ExecutionProfile;

use scylla::policies::load_balancing::NodeIdentifier;
use scylla::policies::{load_balancing, retry::DefaultRetryPolicy};
use scylla::statement::{Consistency, Statement, unprepared};

use futures::TryStreamExt;
use std::convert::Infallible;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use crate::models::{InsertResponse, Item, ItemValue, PageRequest, TokenRangeRequest};
use crate::state::AppState;

// Top-level router that delegates to smaller handler functions.
pub async fn handle(
    req: Request<Body>,
    state: Arc<AppState>,
) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path().to_string();
    match (req.method(), path.as_str()) {
        (&Method::POST, "/insert") => handle_insert(req, state).await,
        (&Method::POST, "/insert_batch") => handle_insert_batch(req, state).await,
        (&Method::POST, "/insert_prepared") => handle_insert_prepared(req, state).await,
        (&Method::GET, "/query_iter") => handle_query_iter(req, state).await,
        (&Method::GET, "/custom_query") => handle_custom_query(req, state).await,
        (&Method::POST, "/custom_query_paged") => handle_custom_query_paged(req, state).await,
        (&Method::GET, "/custom_query_paged_all") => {
            handle_custom_query_paged_all(req, state).await
        }
        (&Method::POST, "/custom_query_token_range") => {
            handle_custom_query_token_range(req, state).await
        }
        (&Method::GET, "/metadata") => handle_metadata(req, state).await,
        _ => {
            let mut not_found = Response::new(Body::from("Not Found"));
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

fn get_node(req: &Request<Body>) -> Option<load_balancing::NodeIdentifier> {
    if let Some(hv) = req.headers().get("node") {
        if let Ok(s) = hv.to_str() {
            if !s.is_empty() {
                let node_string = s.to_string();
                if let Ok(ipv4) = node_string.parse::<Ipv4Addr>() {
                    return Some(load_balancing::NodeIdentifier::NodeAddress(
                        SocketAddr::new(IpAddr::V4(ipv4), 9042),
                    ));
                }
            }
        }
    }
    None
}

fn is_debug(req: &Request<Body>) -> bool {
    if let Some(query) = req.uri().query() {
        for (key, value) in form_urlencoded::parse(query.as_bytes()) {
            if key == "debug" && value == "true" {
                return true;
            }
        }
    }
    false
}

fn exec_profile_with_single_target_lb(node_id: NodeIdentifier) -> ExecutionProfile {
    ExecutionProfile::builder()
        .consistency(Consistency::LocalOne)
        .request_timeout(Some(Duration::from_secs(42)))
        .load_balancing_policy(load_balancing::SingleTargetLoadBalancingPolicy::new(
            node_id, None,
        ))
        .retry_policy(Arc::new(DefaultRetryPolicy::new()))
        .build()
}

async fn handle_insert(
    req: Request<Body>,
    state: Arc<AppState>,
) -> Result<Response<Body>, Infallible> {
    let node_opt = get_node(&req);
    let is_debug = is_debug(&req);

    let whole = hyper::body::to_bytes(req.into_body())
        .await
        .unwrap_or_default();
    let history_listener = Arc::new(HistoryCollector::new());

    let result = match serde_json::from_slice::<Item>(&whole) {
        Ok(item) => {
            let mut statement = unprepared::Statement::from(
                "INSERT INTO demo.items (id, name, value) VALUES (?, ?, ?)",
            );

            if let Some(node_id) = node_opt {
                let execution_profile = exec_profile_with_single_target_lb(node_id);
                let profile_handle = execution_profile.into_handle();
                statement.set_execution_profile_handle(Some(profile_handle));
            }
            if is_debug {
                statement.set_history_listener(history_listener.clone());
            }
            let _ = state
                .session
                .query_unpaged(statement, (item.id, item.name, item.value))
                .await;
            let body = serde_json::to_string(&InsertResponse { success: true })
                .unwrap_or_else(|_| "{}".to_string());
            Ok(Response::new(Body::from(body)))
        }
        Err(e) => {
            let mut resp = Response::new(Body::from(format!("invalid json: {}", e)));
            *resp.status_mut() = StatusCode::BAD_REQUEST;
            Ok(resp)
        }
    };
    if is_debug {
        let structured_history = history_listener.clone_structured_history();
        println!("Request History: {structured_history}")
    };
    result
}

async fn handle_insert_batch(
    req: Request<Body>,
    state: Arc<AppState>,
) -> Result<Response<Body>, Infallible> {
    let node_opt = get_node(&req);
    let is_debug = is_debug(&req);
    let whole = hyper::body::to_bytes(req.into_body())
        .await
        .unwrap_or_default();
    let history_listener = Arc::new(HistoryCollector::new());
    let result = match serde_json::from_slice::<Vec<Item>>(&whole) {
        Ok(items) => {
            use scylla::statement::batch::Batch;
            let mut batch = Batch::new(scylla::statement::batch::BatchType::Logged);

            if let Some(node_id) = node_opt {
                let execution_profile = exec_profile_with_single_target_lb(node_id);
                let profile_handle = execution_profile.into_handle();
                batch.set_execution_profile_handle(Some(profile_handle));
            }
            if is_debug {
                batch.set_history_listener(history_listener.clone());
            }

            let mut values_vec: Vec<(uuid::Uuid, String, ItemValue)> =
                Vec::with_capacity(items.len());
            for item in items {
                batch.append_statement(state.prepared_insert.clone());
                values_vec.push((item.id, item.name, item.value));
            }

            match state.session.batch(&batch, values_vec).await {
                Ok(_) => {
                    let body = serde_json::to_string(&InsertResponse { success: true })
                        .unwrap_or_else(|_| "{}".to_string());
                    Ok(Response::new(Body::from(body)))
                }
                Err(e) => {
                    let mut resp = Response::new(Body::from(format!("batch error: {}", e)));
                    *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    Ok(resp)
                }
            }
        }
        Err(e) => {
            let mut resp = Response::new(Body::from(format!("invalid json: {}", e)));
            *resp.status_mut() = StatusCode::BAD_REQUEST;
            Ok(resp)
        }
    };
    if is_debug {
        let structured_history = history_listener.clone_structured_history();
        println!("Request History: {structured_history}")
    };
    result
}

async fn handle_insert_prepared(
    req: Request<Body>,
    state: Arc<AppState>,
) -> Result<Response<Body>, Infallible> {
    let node_opt = get_node(&req);
    let is_debug = is_debug(&req);
    let whole = hyper::body::to_bytes(req.into_body())
        .await
        .unwrap_or_default();
    let history_listener = Arc::new(HistoryCollector::new());
    let result = match serde_json::from_slice::<Item>(&whole) {
        Ok(item) => {
            let mut prep = state.prepared_insert.clone();

            if let Some(node_id) = node_opt {
                let execution_profile = exec_profile_with_single_target_lb(node_id);
                let profile_handle = execution_profile.into_handle();
                prep.set_execution_profile_handle(Some(profile_handle));
            }
            if is_debug {
                prep.set_history_listener(history_listener.clone());
            }

            let res = state
                .session
                .execute_unpaged(&prep, (item.id, item.name, item.value))
                .await;
            match res {
                Ok(_) => {
                    let body = serde_json::to_string(&InsertResponse { success: true })
                        .unwrap_or_else(|_| "{}".to_string());
                    Ok(Response::new(Body::from(body)))
                }
                Err(e) => {
                    let mut resp = Response::new(Body::from(format!("db error: {}", e)));
                    *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    Ok(resp)
                }
            }
        }
        Err(e) => {
            let mut resp = Response::new(Body::from(format!("invalid json: {}", e)));
            *resp.status_mut() = StatusCode::BAD_REQUEST;
            Ok(resp)
        }
    };
    if is_debug {
        let structured_history = history_listener.clone_structured_history();
        println!("Request History: {structured_history}")
    };
    result
}

async fn handle_query_iter(
    req: Request<Body>,
    state: Arc<AppState>,
) -> Result<Response<Body>, Infallible> {
    let node_opt = get_node(&req);
    let is_debug = is_debug(&req);
    let history_listener = Arc::new(HistoryCollector::new());

    let mut statement = Statement::from("SELECT id, name, value FROM demo.items");

    if let Some(node_id) = node_opt {
        let execution_profile = exec_profile_with_single_target_lb(node_id);
        let profile_handle = execution_profile.into_handle();
        statement.set_execution_profile_handle(Some(profile_handle));
    }
    if is_debug {
        statement.set_history_listener(history_listener.clone());
    }

    match state.session.query_iter(statement, ()).await {
        Ok(pager) => match pager.rows_stream::<(uuid::Uuid, String, ItemValue)>() {
            Ok(mut rows_stream) => {
                let mut out = Vec::new();
                while let Some(row_res) = rows_stream.try_next().await.unwrap_or(None) {
                    let (id, name, value) = row_res;
                    out.push(
                        serde_json::json!({"id": id.to_string(), "name": name, "value": value.0}),
                    );
                }
                let body = serde_json::to_string(&serde_json::json!({"rows": out}))
                    .unwrap_or_else(|_| "{}".to_string());
                Ok(Response::new(Body::from(body)))
            }
            Err(e) => {
                let mut resp =
                    Response::new(Body::from(format!("failed to get rows_stream: {}", e)));
                *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                Ok(resp)
            }
        },
        Err(e) => {
            let mut resp = Response::new(Body::from(format!("query_iter error: {}", e)));
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            Ok(resp)
        }
    }
}

async fn handle_metadata(
    _req: Request<Body>,
    state: Arc<AppState>,
) -> Result<Response<Body>, Infallible> {
    if let Err(e) = state.session.refresh_metadata().await {
        let mut resp = Response::new(Body::from(format!("Failed to refresh metadata {}", e)));
        *resp.status_mut() = StatusCode::NOT_FOUND;
        return Ok(resp);
    }
    if let Some(keyspace_metadata) = &state.session.get_cluster_state().get_keyspace("demo") {
        if let Some(table_metadata) = keyspace_metadata.tables.get("items"){
        //println!("{:#?}", table_metadata);
        
            let body = format!("{:#?}", table_metadata);
            Ok(Response::new(Body::from(body)))
        }
        else{
            let mut resp = Response::new(Body::from("Table 'items' not found in keyspace 'demo'"));
            *resp.status_mut() = StatusCode::NOT_FOUND;
            Ok(resp)
        }
    } else {
        let mut resp = Response::new(Body::from("Keyspace 'demo' not found"));
        *resp.status_mut() = StatusCode::NOT_FOUND;
        Ok(resp)
    }
}
// async fn handle_custom_query_paged(req: Request<Body>, state: Arc<AppState>) -> Result<Response<Body>, Infallible> {
//     use base64;
//     use scylla::response::PagingState;
//     use std::ops::ControlFlow;

//     let whole = hyper::body::to_bytes(req.into_body()).await.unwrap_or_default();
//     let params: PageRequest = serde_json::from_slice(&whole).unwrap_or(PageRequest { paging_state: None, page_size: Some(10) });
//     let whole = hyper::body::to_bytes(req.into_body()).await.unwrap_or_default();
//     let params: PageRequest = serde_json::from_slice(&whole).unwrap_or(PageRequest { paging_state: None, page_size: Some(10) });

//     let mut statement = scylla::statement::unprepared::Statement::new("SELECT text FROM demo.custom_texts");
//     if let Some(size) = params.page_size {
//         statement = statement.with_page_size(size);
//     }
//     let mut statement = scylla::statement::unprepared::Statement::new("SELECT text FROM demo.custom_texts");
//     if let Some(size) = params.page_size {
//         statement = statement.with_page_size(size);
//     }

//     let paging_state = PagingState::start();
//     let paging_state = PagingState::start();

//     let (page, paging_state_response) = match state.session.query_single_page(statement, (), paging_state).await {
//         Ok(res) => res,
//         Err(e) => {
//             let mut resp = Response::new(Body::from(format!("Paging error: {}", e)));
//             *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
//             return Ok(resp);
//         }
//     };
//     let (page, paging_state_response) = match state.session.query_single_page(statement, (), paging_state).await {
//         Ok(res) => res,
//         Err(e) => {
//             let mut resp = Response::new(Body::from(format!("Paging error: {}", e)));
//             *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
//             return Ok(resp);
//         }
//     };

//     let mut results = Vec::new();
//     if let Ok(rows_result) = page.into_rows_result() {
//         if let Ok(rows) = rows_result.rows::<(CustomText,)>() {
//             for row in rows {
//                 if let Ok((CustomText(text),)) = row {
//                     results.push(serde_json::json!({ "text": text }));
//                 }
//             }
//         }
//     }
//     let next_paging_state = match paging_state_response.into_paging_control_flow() {
//         ControlFlow::Break(()) => None,
//         ControlFlow::Continue(new_paging_state) => new_paging_state.as_bytes_slice().map(|bytes| base64::encode(bytes.as_ref())),
//     };
//     let body = serde_json::json!({
//         "results": results,
//         "next_paging_state": next_paging_state
//     });
//     Ok(Response::new(Body::from(serde_json::to_string(&body).unwrap())))
// }

// async fn handle_custom_query_paged_all(_req: Request<Body>, state: Arc<AppState>) -> Result<Response<Body>, Infallible> {
//     use scylla::statement::unprepared::Statement;
//     use scylla::response::PagingState;
//     use std::ops::ControlFlow;
//     use base64;
// async fn handle_custom_query_paged_all(_req: Request<Body>, state: Arc<AppState>) -> Result<Response<Body>, Infallible> {
//     use scylla::statement::unprepared::Statement;
//     use scylla::response::PagingState;
//     use std::ops::ControlFlow;
//     use base64;

// //     let mut statement = Statement::new("SELECT text FROM demo.custom_texts").with_page_size(10);
// //     let mut paging_state = PagingState::start();
// //     let mut results = Vec::new();

//     loop {
//         let (page, paging_state_response) = match state.session.query_single_page(statement.clone(), (), paging_state).await {
//             Ok(res) => res,
//             Err(e) => {
//                 let mut resp = Response::new(Body::from(format!("Paging error: {}", e)));
//                 *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
//                 return Ok(resp);
//             }
//         };
//         if let Ok(rows_result) = page.into_rows_result() {
//             if let Ok(rows) = rows_result.rows::<(CustomText,)>() {
//                 for row in rows {
//                     if let Ok((CustomText(text),)) = row {
//                         results.push(serde_json::json!({ "text": text }));
//                     }
//                 }
//             }
//         }
//         match paging_state_response.into_paging_control_flow() {
//             ControlFlow::Break(()) => break,
//             ControlFlow::Continue(new_paging_state) => {
//                 paging_state = new_paging_state;
//             }
//         }
//     }
//     let body = serde_json::json!({
//         "results": results
//     });
//     Ok(Response::new(Body::from(serde_json::to_string(&body).unwrap())))
// }

// async fn handle_custom_query_token_range(req: Request<Body>, state: Arc<AppState>) -> Result<Response<Body>, Infallible> {
//     use scylla::statement::unprepared::Statement;
//     use scylla::response::PagingState;
//     use std::ops::ControlFlow;
//     use base64;

//     let whole = hyper::body::to_bytes(req.into_body()).await.unwrap_or_default();
//     let params: TokenRangeRequest = match serde_json::from_slice(&whole) {
//         Ok(p) => p,
//         Err(e) => {
//             let mut resp = Response::new(Body::from(format!("invalid json: {}", e)));
//             *resp.status_mut() = StatusCode::BAD_REQUEST;
//             return Ok(resp);
//         }
//     };

//     let mut statement = Statement::new("SELECT id, text FROM demo.custom_texts WHERE token(id) > ? AND token(id) <= ?");
//     if let Some(size) = params.page_size {
//         statement = statement.with_page_size(size);
//     }
//     let paging_state = PagingState::start();

//     let (page, paging_state_response) = match state.session.query_single_page(statement, (params.start_token, params.end_token), paging_state).await {
//         Ok(res) => res,
//         Err(e) => {
//             let mut resp = Response::new(Body::from(format!("Paging error: {}", e)));
//             *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
//             return Ok(resp);
//         }
//     };

//     let mut results = Vec::new();
//     if let Ok(rows_result) = page.into_rows_result() {
//         if let Ok(rows) = rows_result.rows::<(uuid::Uuid, CustomText)>() {
//             for row in rows {
//                 if let Ok((id, CustomText(text))) = row {
//                     results.push(serde_json::json!({ "id": id, "text": text }));
//                 }
//             }
//         }
//     }
//     let next_paging_state = match paging_state_response.into_paging_control_flow() {
//         ControlFlow::Break(()) => None,
//         ControlFlow::Continue(new_paging_state) => new_paging_state.as_bytes_slice().map(|bytes| base64::encode(bytes.as_ref())),
//     };
//     let body = serde_json::json!({
//         "results": results,
//         "next_paging_state": next_paging_state
//     });
//     Ok(Response::new(Body::from(serde_json::to_string(&body).unwrap())))
// }
