use actix::{Actor, StreamHandler};
use actix::prelude::*;
use actix_web_actors::ws;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use chrono::NaiveDate;
use chrono::prelude::*;


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

pub fn i64toVec(n: i64) -> Vec<u8> {
    Vec::from(n.to_le_bytes())
}

pub fn u64toVec(n: u64) -> Vec<u8> {
    Vec::from(n.to_le_bytes())
}

pub fn vecti64(vc: Vec<u8>) -> i64 {
    i64::from_le_bytes(vc.try_into().unwrap())
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
    pub id_generator: IdGenerator
}

impl Actor for WsSession {
    type Context = ws::WebsocketContext<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        let range = Vec::from(self.offset.to_le_bytes());
        for key in self.range_idx.range(range..){
            if let Ok((_k, v)) = key{
                if let Ok(Some(val)) = self.db.get(v){
                    if let Ok(json_text) = String::from_utf8(val.to_vec()){
                        println!("遗留消息:{}", json_text);
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
        let idx_key = Vec::from(nonce.to_le_bytes());
        let today_timestamp_vec = i64toVec(today_ts());
        let remove_flag_key = today_timestamp_vec.clone();
        let deleted = match self.day_idx.remove(today_timestamp_vec){
            Ok(Some(_k))=>true,
            Ok(None)=>true,
            Err(_)=>false
        };
        let remove_flag_val = idx_key.clone();
        if deleted {
            if let Ok(_k) = self.day_idx.insert(remove_flag_key, remove_flag_val){
                println!("update today's last nonce success!")
            }
        }
        
        match self.range_idx.insert(idx_key, key.as_bytes()) {
            Ok(_)=>{
                match self.db.insert(key, val.into_bytes()){
                    Ok(_rd)=>{
                        println!("Insert data Success");
                    }
                    Err(e)=>{
                        println!("Inset faild:{}", e);
                    }
                };
            },
            Err(err)=>{
                println!("insert index faild:{err:?}")
            }
        } 
        
    }
    
}