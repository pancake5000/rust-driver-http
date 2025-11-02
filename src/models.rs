use serde::{Deserialize, Serialize};
use scylla::deserialize::value::DeserializeValue;
use scylla::serialize::value::SerializeValue;
use scylla::frame::response::result::ColumnType;
use scylla::serialize::writers::CellWriter;
use scylla::serialize::writers::WrittenCellProof;

// Custom type for value field in demo.items with serialization support
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemValue(pub i64);

#[derive(Debug, Serialize, Deserialize)]
pub struct Item {
    pub id: uuid::Uuid,
    pub name: String,
    pub value: ItemValue,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InsertResponse {
    pub success: bool,
}

#[derive(Deserialize)]
pub struct PageRequest {
    pub paging_state: Option<String>, // base64-encoded
    pub page_size: Option<i32>,
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for ItemValue {
    fn type_check(
        typ: &ColumnType,
    ) -> Result<(), scylla::deserialize::TypeCheckError> {
        <i64 as DeserializeValue<'frame, 'metadata>>::type_check(typ)
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<scylla::deserialize::FrameSlice<'frame>>,
    ) -> Result<Self, scylla::deserialize::DeserializationError> {
        let val = <i64 as DeserializeValue<'frame, 'metadata>>::deserialize(typ, v)?;
        Ok(Self(val))
    }
}

impl SerializeValue for ItemValue {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        buf: CellWriter<'b>,
    ) -> std::result::Result<WrittenCellProof<'b>, scylla::serialize::SerializationError> {
        <i64 as SerializeValue>::serialize(&self.0, typ, buf)
    }
}