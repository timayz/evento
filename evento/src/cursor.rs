use base64::{
    Engine, alphabet,
    engine::{GeneralPurpose, general_purpose},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::ops::{Deref, DerefMut};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Edge<N> {
    pub cursor: Value,
    pub node: N,
}

#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct PageInfo {
    pub has_previous_page: bool,
    pub has_next_page: bool,
    pub start_cursor: Option<Value>,
    pub end_cursor: Option<Value>,
}

#[derive(Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReadResult<N> {
    pub edges: Vec<Edge<N>>,
    pub page_info: PageInfo,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Value(String);

impl Deref for Value {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl AsRef<[u8]> for Value {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

pub trait Cursor {
    type T: Serialize + DeserializeOwned;

    fn serialize(&self) -> Self::T;

    fn serialize_cursor(&self) -> Result<Value, ciborium::ser::Error<std::io::Error>> {
        let cursor = self.serialize();

        let mut cbor_encoded = vec![];
        ciborium::into_writer(&cursor, &mut cbor_encoded)?;

        let engine = GeneralPurpose::new(&alphabet::URL_SAFE, general_purpose::PAD);

        Ok(Value(engine.encode(cbor_encoded)))
    }

    fn deserialize_cursor(value: &Value) -> Result<Self::T, ciborium::de::Error<std::io::Error>> {
        let engine = GeneralPurpose::new(&alphabet::URL_SAFE, general_purpose::PAD);
        let decoded = engine
            .decode(value)
            .map_err(|e| ciborium::de::Error::Semantic(None, e.to_string()))?;

        ciborium::from_reader(&decoded[..])
    }
}

#[derive(Default, Serialize, Deserialize, Clone)]
pub struct Args {
    pub first: Option<u16>,
    pub after: Option<Value>,
    pub last: Option<u16>,
    pub before: Option<Value>,
}

impl Args {
    pub fn forward(first: u16, after: Option<Value>) -> Self {
        Self {
            first: Some(first),
            after,
            last: None,
            before: None,
        }
    }

    pub fn backward(last: u16, before: Option<Value>) -> Self {
        Self {
            first: None,
            after: None,
            last: Some(last),
            before,
        }
    }

    pub fn is_backward(&self) -> bool {
        (self.last.is_some() || self.before.is_some())
            && self.first.is_none()
            && self.after.is_none()
    }

    pub fn get_info(&self) -> (u16, Option<Value>) {
        if self.is_backward() {
            (self.last.unwrap_or(40), self.before.clone())
        } else {
            (self.first.unwrap_or(40), self.after.clone())
        }
    }
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("{0}")]
    Unknown(#[from] anyhow::Error),

    #[error("ciborium.ser >> {0}")]
    CiboriumSer(#[from] ciborium::ser::Error<std::io::Error>),

    #[error("ciborium.de >> {0}")]
    CiboriumDe(#[from] ciborium::de::Error<std::io::Error>),

    #[error("base64 decode: {0}")]
    Base64Decode(#[from] base64::DecodeError),
}

pub struct Reader<T> {
    data: Vec<T>,
    args: Args,
    order: Order,
}

impl<T> Reader<T>
where
    T: Cursor + Clone,
    T: Send + Unpin,
    T: Bind<T = T>,
{
    pub fn new(data: Vec<T>) -> Self {
        Self {
            data,
            args: Args::default(),
            order: Order::Asc,
        }
    }

    pub fn order(&mut self, order: Order) -> &mut Self {
        self.order = order;

        self
    }

    pub fn desc(&mut self) -> &mut Self {
        self.order(Order::Desc)
    }

    pub fn args(&mut self, args: Args) -> &mut Self {
        self.args = args;

        self
    }

    pub fn backward(&mut self, last: u16, before: Option<Value>) -> &mut Self {
        self.args(Args {
            last: Some(last),
            before,
            ..Default::default()
        })
    }

    pub fn forward(&mut self, first: u16, after: Option<Value>) -> &mut Self {
        self.args(Args {
            first: Some(first),
            after,
            ..Default::default()
        })
    }

    pub fn execute(&self) -> Result<ReadResult<T>, ReadError> {
        let is_order_desc = matches!(
            (&self.order, self.args.is_backward()),
            (Order::Asc, true) | (Order::Desc, false)
        );

        let mut data = self.data.clone().into_iter().collect::<Vec<_>>();
        T::sort_by(&mut data, is_order_desc);
        let (limit, cursor) = self.args.get_info();

        if let Some(cursor) = cursor.as_ref() {
            let cursor = T::deserialize_cursor(cursor)?;
            T::retain(&mut data, cursor, is_order_desc);
        }

        let data_len = data.len();
        data = data.into_iter().take((limit + 1).into()).collect();

        let has_more = data_len > data.len();
        if has_more {
            data.pop();
        }

        let mut edges = data
            .into_iter()
            .map(|node| Edge {
                cursor: node
                    .serialize_cursor()
                    .expect("Error while serialize_cursor in assert_read_result"),
                node,
            })
            .collect::<Vec<_>>();

        if self.args.is_backward() {
            edges = edges.into_iter().rev().collect();
        }

        let page_info = if self.args.is_backward() {
            PageInfo {
                has_previous_page: has_more,
                start_cursor: edges.first().map(|e| e.cursor.to_owned()),
                ..Default::default()
            }
        } else {
            PageInfo {
                has_next_page: has_more,
                end_cursor: edges.last().map(|e| e.cursor.to_owned()),
                ..Default::default()
            }
        };

        Ok(ReadResult { edges, page_info })
    }
}

impl<T> Deref for Reader<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> DerefMut for Reader<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

pub trait Bind {
    type T: Cursor + Clone;

    fn sort_by(data: &mut Vec<Self::T>, is_order_desc: bool);
    fn retain(
        data: &mut Vec<Self::T>,
        cursor: <<Self as Bind>::T as Cursor>::T,
        is_order_desc: bool,
    );
}

impl Bind for crate::Event {
    type T = Self;

    fn sort_by(data: &mut Vec<Self::T>, is_order_desc: bool) {
        if !is_order_desc {
            data.sort_by(|a, b| {
                if a.timestamp != b.timestamp {
                    return a.timestamp.cmp(&b.timestamp);
                }

                if a.version != b.version {
                    return a.version.cmp(&b.version);
                }

                a.id.cmp(&b.id)
            });
        } else {
            data.sort_by(|a, b| {
                if a.timestamp != b.timestamp {
                    return b.timestamp.cmp(&a.timestamp);
                }

                if a.version != b.version {
                    return b.version.cmp(&a.version);
                }

                b.id.cmp(&a.id)
            });
        }
    }

    fn retain(
        data: &mut Vec<Self::T>,
        cursor: <<Self as Bind>::T as Cursor>::T,
        is_order_desc: bool,
    ) {
        data.retain(|event| {
            if is_order_desc {
                event.timestamp < cursor.t
                    || (event.timestamp == cursor.t
                        && (event.version < cursor.v
                            || (event.version == cursor.v && event.id < cursor.i)))
            } else {
                event.timestamp > cursor.t
                    || (event.timestamp == cursor.t
                        && (event.version > cursor.v
                            || (event.version == cursor.v && event.id > cursor.i)))
            }
        });
    }
}
