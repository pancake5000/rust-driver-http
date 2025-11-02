use scylla::deserialize::value::DeserializeValue;
use scylla::frame::response::result::ColumnType;
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::CellWriter;
use scylla::serialize::writers::WrittenCellProof;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Item {
    pub id: uuid::Uuid,
    pub name: String,
    pub value: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertResponse {
    pub success: bool,
}

// Custom type with serialization/deserialization support
#[derive(Debug, PartialEq, Eq)]
pub struct CustomText<'a>(pub &'a str);

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for CustomText<'frame> {
    fn type_check(typ: &ColumnType) -> Result<(), scylla::deserialize::TypeCheckError> {
        <&str as DeserializeValue<'frame, 'metadata>>::type_check(typ)
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<scylla::deserialize::FrameSlice<'frame>>,
    ) -> Result<Self, scylla::deserialize::DeserializationError> {
        let s = <&str as DeserializeValue<'frame, 'metadata>>::deserialize(typ, v)?;
        Ok(Self(s))
    }
}

impl<'a> SerializeValue for CustomText<'a> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        buf: CellWriter<'b>,
    ) -> std::result::Result<WrittenCellProof<'b>, scylla::serialize::SerializationError> {
        <&str as SerializeValue>::serialize(&self.0, typ, buf)
    }
}

#[derive(Deserialize)]
pub struct PageRequest {
    pub paging_state: Option<String>, // base64-encoded
    pub page_size: Option<i32>,
}

#[derive(serde::Deserialize)]
pub struct TokenRangeRequest {
    pub start_token: i64,
    pub end_token: i64,
    pub page_size: Option<i32>,
}
