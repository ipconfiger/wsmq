use actix::{Actor, StreamHandler};
use actix::prelude::*;
use actix_web_actors::ws;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use chrono::NaiveDate;
use chrono::prelude::*;
use sled::IVec;

#[derive(Debug, PartialEq)]
struct TransactionError;

pub fn today_ts() -> i64{
    let today = NaiveDate::from_ymd_opt(
        chrono::Local::now().year(),
        chrono::Local::now().month(),
        chrono::Local::now().day()
    ).unwrap();
    let dt = chrono::NaiveDateTime::new(today, chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap());
    let timestamp = dt.timestamp();
    timestamp
}

pub fn i64to_vec(n: i64) -> Vec<u8> {
    Vec::from(n.to_be_bytes())
}

pub fn vectu64(vc: Vec<u8>) -> u64 {
    u64::from_be_bytes(vc.try_into().unwrap())
}

pub fn make_key(topic: &str, nonce: u64) -> IVec {
    let mut nonce_vec = Vec::from(nonce.to_be_bytes());
    let mut topic_vec = Vec::from(topic.as_bytes());
    topic_vec.append(&mut nonce_vec);
    IVec::from(topic_vec)
}

#[derive(Debug, Clone)]
pub struct IdGenerator {
    max_id: Arc<Mutex<u64>>
}

impl IdGenerator {
    pub fn new(init_id: u64) -> IdGenerator {
        IdGenerator { 
            max_id: Arc::new(Mutex::new(init_id))
        }
    }
    
    pub fn init_with(&self, init_id: u64) {
        let mut max_id = self.max_id.lock().unwrap();
        *max_id = init_id;
    }

    pub fn gen_id(&self) -> u64 {
        let mut max_id = self.max_id.lock().unwrap();
        *max_id += 1;
        let new_id = *max_id;
        new_id
    }
}


#[derive(Serialize, Deserialize)]
pub struct Message {
    i: String,
    t: String,
    p: String,
    k: String,
    n: Option<u64>
}

#[derive(Serialize, Deserialize)]
pub struct ErrResp {
    pub rs: bool,
    pub detail: String
}

#[derive(Message)]
#[rtype(result = "()")]
struct InnerMessage(String);

pub struct WsSession{
    pub client_id: String,
    pub topic: String,
    pub db: sled::Db,
    pub offset: u64,
    pub range_idx: sled::Tree,
    pub day_idx: sled::Tree,
    pub main_idx: sled::Tree,
    pub id_generator: IdGenerator
}

impl Actor for WsSession {
    type Context = ws::WebsocketContext<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.text("{\"rs\":true,\"detail\":\"connected\"}");
        let flag_min = make_key(self.topic.as_str(), self.offset);
        let flag_max = make_key(self.topic.as_str(), u64::MAX);
        for key in self.main_idx.range(flag_min..flag_max){
            if let Ok((_k, main_key)) = key{
                if let Ok(Some(val)) = self.db.get(main_key){
                    if let Ok(json_text) = String::from_utf8(val.to_vec()){
                        println!("retain message:{}", json_text);
                        ctx.text(json_text);
                    }
                }
            }
        }
        
        let mut watcher = self.db.watch_prefix(self.topic.clone());
        let addr = ctx.address();
        let fut = async move {
            while let Some(event) = (&mut watcher).await {
                let json_msg = match event {
                    sled::Event::Insert { key, value }=>{
                        println!("got message key:{key:?}");
                        String::from_utf8(value.to_vec()).unwrap()},
                    sled::Event::Remove { key }=>format!("{key:?}")
                };
                if json_msg != "".to_string(){
                    addr.do_send::<InnerMessage>(InnerMessage(json_msg));
                }
            }
        };
        let fut = actix::fut::wrap_future::<_, Self>(fut);
        ctx.spawn(fut);
       
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        println!("client:{} disconnected", self.client_id);
    }

}


impl Handler<InnerMessage> for WsSession {
    type Result = ();

    fn handle(&mut self, msg: InnerMessage, ctx: &mut Self::Context) {
        ctx.text(msg.0);
    }
}

/// Handler for ws::Message message
impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WsSession {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Text(text)) => {
                match serde_json::from_str::<Message>(&text){
                    Ok(msg)=>{
                        println!("Will send Message to:{}", msg.t);
                        let mut msg = msg;
                        self.dispatch_message(&mut msg);
                    }
                    Err(err)=>{
                        println!("Invalid Message:{} error:{}", text, err);
                        ctx.text(serde_json::to_string(&ErrResp{rs: false, detail: format!("Invalid json:{err}")}).unwrap())
                    }
                };
            },
            Ok(ws::Message::Binary(bin)) => ctx.binary(bin),
            Ok(ws::Message::Close(_))=>{ctx.close(Some(ws::CloseReason{code:ws::CloseCode::Normal, description: Some("reset by peer".to_string())}));}
            _ => (),
        }
    }
}

impl WsSession {

    fn dispatch_message(&mut self, message: &mut Message) {
        let nonce = self.id_generator.gen_id();
        message.n = Some(nonce);
        let key = format!("{}-{}", message.t, message.i);
        let val = serde_json::to_string(message).unwrap();
        let idx_key = Vec::from(nonce.to_be_bytes());
        let today_timestamp_vec = i64to_vec(today_ts());
        // update today's last nonce index
        let new_idx_key = idx_key.clone();
        if let Ok(_k) = self.day_idx.insert(today_timestamp_vec, idx_key){
            println!("update today's last nonce success!");
            if let Ok(_) = self.range_idx.insert(new_idx_key, key.as_bytes()){
                println!("update range index success!");
                let main_key = make_key(self.topic.as_str(), nonce);
                if let Ok(_) = self.main_idx.insert(main_key, key.as_bytes()) {
                    println!("update main index success!");
                    if let Ok(_) = self.db.insert(key, val.into_bytes()){
                        println!("insert data success!");
                    }else{
                        eprintln!("insert data faild!");
                    }
                }else{
                    eprintln!("update main index faild!");
                }
            }else{
                eprintln!("update range index faild!");
            }
        }else{
            eprintln!("update today's last nonce faild!");
        };
    }
    
}