use hyper::{Body, Method, Request, Response, StatusCode};
use std::convert::Infallible;
use std::sync::Arc;
use futures::TryStreamExt;
use scylla::statement::batch::{Batch, BatchType};

use crate::state::AppState;
use crate::models::{Item, InsertResponse, PageRequest, ItemValue};

// Top-level router that delegates to smaller handler functions.
pub async fn handle(req: Request<Body>, state: Arc<AppState>) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path().to_string();
    match (req.method(), path.as_str()) {
        (&Method::POST, "/insert") => handle_insert(req, state).await,
        (&Method::POST, "/insert_batch") => handle_insert_batch(req, state).await,
        (&Method::POST, "/insert_prepared") => handle_insert_prepared(req, state).await,
        (&Method::GET, "/query_iter") => handle_query_iter(req, state).await,
        // (&Method::POST, "/custom_query_paged") => handle_custom_query_paged(req, state).await,
        // (&Method::GET, "/custom_query_paged_all") => handle_custom_query_paged_all(req, state).await,
        _ => {
            let mut not_found = Response::new(Body::from("Not Found"));
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

async fn handle_insert(req: Request<Body>, state: Arc<AppState>) -> Result<Response<Body>, Infallible> {
    let whole = hyper::body::to_bytes(req.into_body()).await.unwrap_or_default();
    match serde_json::from_slice::<Item>(&whole) {
        Ok(item) => {
            let cql = "INSERT INTO demo.items (id, name, value) VALUES (?, ?, ?)";
            let _ = state.session.query_unpaged(cql, (item.id, item.name, item.value)).await;
            let body = serde_json::to_string(&InsertResponse { success: true }).unwrap_or_else(|_| "{}".to_string());
            Ok(Response::new(Body::from(body)))
        }
        Err(e) => {
            let mut resp = Response::new(Body::from(format!("invalid json: {}", e)));
            *resp.status_mut() = StatusCode::BAD_REQUEST;
            Ok(resp)
        }
    }
}

async fn handle_insert_batch(req: Request<Body>, state: Arc<AppState>) -> Result<Response<Body>, Infallible> {
    let whole = hyper::body::to_bytes(req.into_body()).await.unwrap_or_default();
    match serde_json::from_slice::<Vec<Item>>(&whole) {
        Ok(items) => {
            use scylla::statement::batch::Batch;
            let mut batch = Batch::new(scylla::statement::batch::BatchType::Logged);

            let mut values_vec: Vec<(uuid::Uuid, String, ItemValue)> = Vec::with_capacity(items.len());
            for item in items {
                batch.append_statement(state.prepared_insert.clone());
                values_vec.push((item.id, item.name, item.value));
            }

            match state.session.batch(&batch, values_vec).await {
                Ok(_) => {
                    let body = serde_json::to_string(&InsertResponse { success: true }).unwrap_or_else(|_| "{}".to_string());
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
    }
}

async fn handle_insert_prepared(req: Request<Body>, state: Arc<AppState>) -> Result<Response<Body>, Infallible> {
    let whole = hyper::body::to_bytes(req.into_body()).await.unwrap_or_default();
    match serde_json::from_slice::<Item>(&whole) {
        Ok(item) => {
            let prep = state.prepared_insert.clone();
            let res = state.session.execute_unpaged(&prep, (item.id, item.name, item.value)).await;
            match res {
                Ok(_) => {
                    let body = serde_json::to_string(&InsertResponse { success: true }).unwrap_or_else(|_| "{}".to_string());
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
    }
}

async fn handle_query_iter(_req: Request<Body>, state: Arc<AppState>) -> Result<Response<Body>, Infallible> {
    match state.session.query_iter("SELECT id, name, value FROM demo.items", ()).await {
        Ok(pager) => {
            match pager.rows_stream::<(uuid::Uuid, String, i64)>() {
                Ok(mut rows_stream) => {
                    let mut out = Vec::new();
                    while let Some(row_res) = rows_stream.try_next().await.unwrap_or(None) {
                        let (id, name, value) = row_res;
                        out.push(serde_json::json!({"id": id.to_string(), "name": name, "value": value}));
                    }
                    let body = serde_json::to_string(&serde_json::json!({"rows": out})).unwrap_or_else(|_| "{}".to_string());
                    Ok(Response::new(Body::from(body)))
                }
                Err(e) => {
                    let mut resp = Response::new(Body::from(format!("failed to get rows_stream: {}", e)));
                    *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    Ok(resp)
                }
            }
        }
        Err(e) => {
            let mut resp = Response::new(Body::from(format!("query_iter error: {}", e)));
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            Ok(resp)
        }
    }
}

// async fn handle_custom_query_paged(req: Request<Body>, state: Arc<AppState>) -> Result<Response<Body>, Infallible> {
//     use base64;
//     use scylla::response::PagingState;
//     use std::ops::ControlFlow;

//     let whole = hyper::body::to_bytes(req.into_body()).await.unwrap_or_default();
//     let params: PageRequest = serde_json::from_slice(&whole).unwrap_or(PageRequest { paging_state: None, page_size: Some(10) });

//     let mut statement = scylla::statement::unprepared::Statement::new("SELECT text FROM demo.custom_texts");
//     if let Some(size) = params.page_size {
//         statement = statement.with_page_size(size);
//     }
    
//     let paging_state = PagingState::start();

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
//         if let Ok(rows) = rows_result.rows::<(ItemValue,)>() {
//             for row in rows {
//                 if let Ok((ItemValue(text),)) = row {
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

//     let mut statement = Statement::new("SELECT text FROM demo.custom_texts").with_page_size(10);
//     let mut paging_state = PagingState::start();
//     let mut results = Vec::new();

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
//             if let Ok(rows) = rows_result.rows::<(ItemValue,)>() {
//                 for row in rows {
//                     if let Ok((ItemValue(text),)) = row {
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
