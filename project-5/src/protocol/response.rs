use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub enum Response {
    Ok(Option<String>),
    Err(String),
}
