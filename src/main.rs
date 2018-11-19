extern crate clap;
extern crate futures;
extern crate zookeeper;
extern crate rdkafka;

//use std::cell::RefCell;
use std::sync::{Mutex};
use futures::{Future};
use futures::sync::oneshot;
use rdkafka::{error::KafkaError, message::{Message, Headers, Timestamp}, consumer::{Consumer}};

// default zookeeper port is 2181, address requires a port
#[cfg(test)] const TEST_ZOOKEEPER: &'static str = "ess01:2323";
#[cfg(test)] const TEST_BROKER: &'static str = "ess01:2424";
#[cfg(test)] const TEST_TOPIC: &'static str = "tmp_test_topic";

#[allow(unused)]
struct ConnectHandler {
  on_connect: Mutex<Option<oneshot::Sender<i32>>>,
}

impl ConnectHandler {
  #[allow(unused)]
  fn new(on_connect: oneshot::Sender<i32>) -> Self {
    Self {
      on_connect: Mutex::new(Some(on_connect)),
    }
  }
}

impl zookeeper::Watcher for ConnectHandler {
  fn handle(&self, e: zookeeper::WatchedEvent) {
    self.on_connect.lock().unwrap().take().unwrap().send(123).unwrap();
    panic!("connected {:?}", e);
  }
}

#[test] fn zookeeper_connect() {
  let timeout = std::time::Duration::from_millis(500);
  let (on_connect_event_tx, _on_connect_event_rx) = oneshot::channel();
  let zk = zookeeper::ZooKeeper::connect(TEST_ZOOKEEPER, timeout, ConnectHandler::new(on_connect_event_tx)).unwrap();
  //on_connect_event_rx.map(|_|()).wait().unwrap();
  //panic!("test done");

  let partitions: Vec<_> = zk.get_children(&format!("/brokers/topics/{}/partitions", TEST_TOPIC), false).unwrap()
  .into_iter().map(|x| x.parse::<u64>().unwrap()).collect();
  println!("partitions: {:?}", partitions);
  assert!(partitions.len() >= 1);

  let x = zk.get_children(&format!("/brokers/topics/{}/partitions/{}", TEST_TOPIC, partitions[0]), false).unwrap();
  println!("x: {:?}", x);

  let broker_ids: Vec<_> = zk.get_children("/brokers/ids", false).unwrap().into_iter().map(|x| x.parse::<u64>().unwrap()).collect();
  println!("broker_ids: {:?}", broker_ids);
}


struct Ctx {
}

impl rdkafka::client::ClientContext for Ctx {
  fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, message: &str) {
    println!("level: {:?}  fac: {:?}  message: {:?}", level, fac, message);
  }
  fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
    println!("error: {:?}  {}", error, reason);
  }
}

impl rdkafka::consumer::ConsumerContext for Ctx {
  fn pre_rebalance<'a>(&self, rebalance: &rdkafka::consumer::Rebalance<'a>) {
    println!("pre_rebalance rebalance: {:?}", rebalance);
  }
}

#[test] fn kafka_connect() {
  let mut conf = rdkafka::config::ClientConfig::new();
  conf.set("metadata.broker.list", TEST_BROKER);
  let client = rdkafka::client::Client::new(
    &conf,
    conf.create_native_config().unwrap(),
    rdkafka::types::RDKafkaType::RD_KAFKA_CONSUMER,
    Ctx {},
  ).unwrap();

  let timeout = Some(std::time::Duration::from_millis(1000));
  let metadata = client.fetch_metadata(None, timeout).unwrap();
  println!("topics: {:?}", metadata.topics().iter().map(|x| x.name()).collect::<Vec<_>>());
  println!("partitions: {:?}", metadata.topics().iter().map(|x|
    (x.name(), x.partitions().iter().map(|x| x.id()).collect::<Vec<_>>())
  ).collect::<Vec<_>>());

  let partition = 0;
  let watermarks = client.fetch_watermarks(TEST_TOPIC, partition, timeout).unwrap();
  println!("watermarks: {:?}", watermarks);
}


fn kafka_produce(broker: &str, topic: &str, data: &[u8]) {
  let mut conf = rdkafka::config::ClientConfig::new();
  conf.set("api.version.request", "true");
  conf.set("metadata.broker.list", broker);
  let record = rdkafka::producer::future_producer::FutureRecord::to(topic)
  .key("")
  .payload(data);
  use rdkafka::config::FromClientConfigAndContext;
  let p = rdkafka::producer::future_producer::FutureProducer::from_config_and_context(&conf, Ctx {}).unwrap();
  p.send(record, 0).wait().unwrap().unwrap();
}

fn cmd_produce(m: &clap::ArgMatches) {
  println!("produce");
  let broker = m.value_of("b").unwrap();
  let topic = m.value_of("t").unwrap();
  let mut buf = vec![];
  use std::io::Read;
  std::io::stdin().read_to_end(&mut buf).unwrap();
  println!("broker: {}  topic: {}  len: {}", broker, topic, buf.len());
  kafka_produce(broker, topic, &buf);
}


struct MsgShortView<'a, H: Headers, M: 'a + Message<Headers=H>>(&'a M);

impl<'a, H: Headers, M: 'a + Message<Headers=H>> std::fmt::Display for MsgShortView<'a, H, M> {
  fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
    let _hs: Vec<_> = (0..64).into_iter().map(|i| self.0.headers().map(|hs| hs.get(i))).filter(|x| x.is_some()).map(|x| x.unwrap()).collect();
    write!(f, "p/o: {}/{} {} {} {}",
      //self.0.topic(),
      self.0.partition(), self.0.offset(),
      //self.0.key().map(|x|x.len()),
      match self.0.timestamp() {
        Timestamp::NotAvailable => "NT".to_string(),
        Timestamp::CreateTime(t) => format!("ct {:13.13}", t),
        Timestamp::LogAppendTime(t) => format!("at {:13.13}", t),
      },
      //hs,
      match self.0.payload() {
        Some(x) => format!("{:4.4}", x.len()),
        None => "NA".into()
      },
      match self.0.payload().map(|x| String::from_utf8_lossy(&x[..x.len().min(64)])) {
        Some(x) => x.replace("\n", "").replace(" ", ""),
        None => "None".into()
      },
    )
  }
}

fn kafka_consume(broker: &str, topic: &str) {
  let mut conf = rdkafka::config::ClientConfig::new();
  conf.set("api.version.request", "true");
  conf.set("group.id", "a");
  conf.set("metadata.broker.list", broker);
  use rdkafka::config::FromClientConfigAndContext;
  /*
  StreamConsumer is more difficult to use and the async nature of streams makes the commit of the offsets
  more difficult.
  */
  //let c = rdkafka::consumer::stream_consumer::StreamConsumer::from_config_and_context(&conf, Ctx {}).unwrap();
  let c = rdkafka::consumer::base_consumer::BaseConsumer::from_config_and_context(&conf, Ctx {}).unwrap();  

  let timeout = Some(std::time::Duration::from_millis(10000));

  let topics = [topic];
  let metas: Vec<_> = topics.iter().map(|x| {
    let m = c.fetch_metadata(Some(x), timeout).unwrap();
    // Since we specify the topic, we expect exactly one entry
    assert!(m.topics().len() == 1);
    m
  }).collect();

  // Can either 'subscribe' or, if I need specific offsets 'assign'
  use rdkafka::topic_partition_list::{TopicPartitionList, Offset};
  let mut pl = TopicPartitionList::new();
  for m in metas.iter() {
    // We did assert before that we have 1 topic
    let t = &m.topics()[0];
    for p in t.partitions() {
      let w = c.fetch_watermarks(t.name(), p.id(), timeout).unwrap();
      // Beginning, End, Stored, Invalid, Offset(i64)
      pl.add_partition_offset(t.name(), p.id(), Offset::Offset((w.1 - 8).max(0)));
      println!("w: {:?}", w);
    }
  }
  //.subscribe(&[topic]).unwrap();
  c.assign(&pl).unwrap();
  let pos = c.position().unwrap();
  println!("pos: {:?}", pos);
  loop {
    match c.poll(timeout) {
      Some(mm) => {
        match mm {
          Ok(m) => {
            //println!("m: {:?}  {:?}", m.timestamp(), m.key().map(|x|x.len()));
            //println!("pos: {:?}", c.position().unwrap());
            println!("{}", MsgShortView(&m));
          }
          Err(KafkaError::PartitionEOF(x)) => {
            println!("EOF x: {}", x);
            break;
          }
          Err(x) => panic!(x)
        }
      }
      None => println!("Poll returned None")
    }
  }
}

fn cmd_consume(m: &clap::ArgMatches) {
  println!("consume");
  let broker = m.value_of("b").unwrap();
  let topic = m.value_of("t").unwrap();
  println!("broker: {}  topic: {}", broker, topic);
  kafka_consume(broker, topic);
}

fn main() {
  let app = clap::App::new("kaft")
  .author("Dominik Werder <dominik.werder@gmail.com>")
  .args_from_usage("
    -c=[CMD]  'p, c'
    -b=[BROKER]
    -t=[TOPIC]
  ");
  let m = app.get_matches();
  println!("{:?}", m);
  if let Some(c) = m.value_of("c") {
    match c {
      "p" => {
        cmd_produce(&m);
      }
      "c" => {
        cmd_consume(&m);
      }
      _ => panic!()
    }
  }
}
