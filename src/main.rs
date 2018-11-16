extern crate clap;
extern crate futures;
extern crate zookeeper;
extern crate rdkafka;

//use std::cell::RefCell;
use std::sync::{Mutex};
use futures::Future;
use futures::sync::oneshot;

#[allow(unused)]
struct ConnectHandler {
  on_connect: Mutex<Option<oneshot::Sender<i32>>>,
}

impl ConnectHandler {
  fn _new(on_connect: oneshot::Sender<i32>) -> Self {
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
  // default zookeeper port is 2181, address requires a port
  let addr = "ess01:2323";
  let (on_connect_event_tx, on_connect_event_rx) = oneshot::channel();
  let zk = zookeeper::ZooKeeper::connect(addr, std::time::Duration::from_millis(1000), ConnectHandler::new(on_connect_event_tx)).unwrap();
  //on_connect_event_rx.map(|_|()).wait().unwrap();
  //panic!("test done");

  let partitions: Vec<_> = zk.get_children("/brokers/topics/FREIA_detector/partitions", false).unwrap().into_iter().map(|x| x.parse::<u64>().unwrap()).collect();
  println!("partitions: {:?}", partitions);

  let broker_ids: Vec<_> = zk.get_children("/brokers/ids", false).unwrap().into_iter().map(|x| x.parse::<u64>().unwrap()).collect();
  println!("broker_ids: {:?}", broker_ids);
}


struct Ctx {
}

impl rdkafka::client::ClientContext for Ctx {
  fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
    println!("error: {:?}  {}", error, reason);
  }
}

#[test] fn kafka_connect() {
  let addr = "ess01:2424";
  let addr = "mpc1663:9092";
  let mut conf = rdkafka::config::ClientConfig::new();
  conf.set("metadata.broker.list", addr);
  let client = rdkafka::client::Client::new(
    &conf,
    conf.create_native_config().unwrap(),
    rdkafka::types::RDKafkaType::RD_KAFKA_CONSUMER,
    Ctx {},
  ).unwrap();

  let timeout = Some(std::time::Duration::from_millis(1000));
  let metadata = client.fetch_metadata(None, timeout).unwrap();
  println!("metadata; {:?}", metadata.topics().iter().map(|x| x.name()).collect::<Vec<_>>());

  let topic = "FREIA_detector";
  let partition = 0;
  let watermarks = client.fetch_watermarks(topic, partition, timeout).unwrap();
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

fn main() {
  let app = clap::App::new("kaft")
  .author("Dominik Werder <dominik.werder@gmail.com>")
  .args_from_usage("
    -c=[CMD]
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
      _ => panic!()
    }
  }
}
