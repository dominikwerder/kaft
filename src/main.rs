extern crate clap;
extern crate futures;
extern crate zookeeper;
extern crate rdkafka;
extern crate ncurses;
extern crate terminal_size;
extern crate chrono;

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
  let broker = m.value_of("broker").unwrap();
  let topic = m.value_of("topic").unwrap();
  let mut buf = vec![];
  use std::io::Read;
  std::io::stdin().read_to_end(&mut buf).unwrap();
  println!("broker: {}  topic: {}  len: {}", broker, topic, buf.len());
  kafka_produce(broker, topic, &buf);
}


struct MsgShortView<'a, H: Headers, M: 'a + Message<Headers=H>>(&'a M);

impl<'a, H: Headers, M: 'a + Message<Headers=H>> std::fmt::Display for MsgShortView<'a, H, M> {
  fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
    let columns = terminal_size::terminal_size().unwrap().0 .0 as usize;
    //println!("columns: {}", columns);
    let _hs: Vec<_> = (0..64).into_iter().map(|i| self.0.headers().map(|hs| hs.get(i))).filter(|x| x.is_some()).map(|x| x.unwrap()).collect();
    let s1 = format!("p/o: {}/{} {} {}",
      //self.0.topic(),
      self.0.partition(), self.0.offset(),
      //self.0.key().map(|x|x.len()),
      match self.0.timestamp() {
        Timestamp::NotAvailable => "NT".to_string(),
        Timestamp::CreateTime(t) => {
          let secs = (t / 1000) as i64;
          let ts = chrono::TimeZone::timestamp(&chrono::Utc, secs, ((t - 1000 * secs) * 1_000_000) as u32);
          format!("ct {} {}", t, ts.format("%Y-%m-%d %H:%M"))
        }
        Timestamp::LogAppendTime(t) => {
          let secs = (t / 1000) as i64;
          let ts = chrono::TimeZone::timestamp(&chrono::Utc, secs, ((t - 1000 * secs) * 1_000_000) as u32);
          format!("ct {} {}", t, ts.format("%Y-%m-%d %H:%M"))
        }
      },
      //hs,
      match self.0.payload() {
        Some(x) => format!("{:6.6}", x.len()),
        None => "NA".into()
      },
    );
    let datastring = match self.0.payload() {
      Some(x) => {
        let data = x.iter().map(|x| char::from(*x)).map(|x| {
          if x.is_ascii() && !x.is_ascii_control() { x } else { '.' }
          //let mut buf = [0u8; 4];
          //x.encode_utf8(&mut buf);
          //buf[0]
        })
        .filter(|x| !x.is_whitespace())
        ;
        use std::iter::FromIterator;
        let s1 = String::from_iter(data);
        s1
      }
      None => "None".into()
    };
    write!(f, "{} {:.w$}", s1, datastring, w = columns - s1.len() - 1)
  }
}

fn kafka_consume(broker: &str, topic: &str, rewind: i64, nmsg: u64) {
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
      let ix = if w.1 >= rewind { w.1 - rewind } else { 0 };
      let off = Offset::Offset(ix);
      pl.add_partition_offset(t.name(), p.id(), off);
      println!("  t: {}  p: {}  w: {:?}  off: {:?}", t.name(), p.id(), w, off);
    }
  }
  //.subscribe(&[topic]).unwrap();
  c.assign(&pl).unwrap();
  //let pos = c.position().unwrap();
  //println!("pos: {:?}", pos);
  let mut count = 0;
  loop {
    match c.poll(timeout) {
      Some(mm) => {
        match mm {
          Ok(m) => {
            //println!("m: {:?}  {:?}", m.timestamp(), m.key().map(|x|x.len()));
            //println!("pos: {:?}", c.position().unwrap());
            println!("{}", MsgShortView(&m));
            count += 1;
            if count >= nmsg { break }
          }
          Err(KafkaError::PartitionEOF(_p)) => {
            //println!("EOF p: {}", p);
            break
          }
          Err(x) => panic!(x)
        }
      }
      None => panic!("Poll returned None")
    }
  }
}

fn cmd_consume(m: &clap::ArgMatches) {
  let broker = m.value_of("broker").unwrap();
  let topic = m.value_of("topic").unwrap();
  let nmsg = m.value_of("nmsg").map_or(1u64, |x|x.parse().unwrap());
  let rewind = match m.value_of("rewind") {
    Some(x) => x.parse::<i64>().unwrap(),
    None => 0
  };
  println!("broker: {}  topic: {}", broker, topic);
  kafka_consume(broker, topic, rewind, nmsg);
}

fn cmd_cat_payload(m: &clap::ArgMatches) {
  let broker = m.value_of("broker").unwrap();
  let topic = m.value_of("topic").unwrap();
  let partition = m.value_of("partition").unwrap().parse::<i32>().unwrap();
  let offset = m.value_of("offset").unwrap().parse::<i64>().unwrap();
  //println!("cat_payload  broker: {}  topic: {}  partition: {}  offset: {}", broker, topic, partition, offset);
  let mut conf = rdkafka::config::ClientConfig::new();
  conf.set("api.version.request", "true");
  conf.set("group.id", "a");
  conf.set("metadata.broker.list", broker);
  use rdkafka::config::FromClientConfigAndContext;
  let c = rdkafka::consumer::base_consumer::BaseConsumer::from_config_and_context(&conf, Ctx {}).unwrap();  
  let timeout = Some(std::time::Duration::from_millis(10000));
  let topics = [topic];
  let metas: Vec<_> = topics.iter().map(|x| {
    let m = c.fetch_metadata(Some(x), timeout).unwrap();
    // Since we specify the topic, we expect exactly one entry
    assert!(m.topics().len() == 1);
    m
  }).collect();
  use rdkafka::topic_partition_list::{TopicPartitionList, Offset};
  let mut pl = TopicPartitionList::new();
  for m in metas.iter() {
    // We did assert before that we have 1 topic
    let t = &m.topics()[0];
    for p in t.partitions() {
      if p.id() == partition {
        //let w = c.fetch_watermarks(t.name(), p.id(), timeout).unwrap();
        // Beginning, End, Stored, Invalid, Offset(i64)
        let off = Offset::Offset(offset);
        pl.add_partition_offset(t.name(), p.id(), off);
        //println!("  t: {}  p: {}  w: {:?}  off: {:?}", t.name(), p.id(), w, off);
      }
    }
  }
  assert!(pl.count() == 1);
  //.subscribe(&[topic]).unwrap();
  c.assign(&pl).unwrap();
  //let pos = c.position().unwrap();
  //println!("pos: {:?}", pos);
  loop {
    match c.poll(timeout) {
      Some(mm) => {
        match mm {
          Ok(m) => {
            //println!("m: {:?}  {:?}", m.timestamp(), m.key().map(|x|x.len()));
            //println!("pos: {:?}", c.position().unwrap());
            use std::io::Write;
            std::io::stdout().write(m.payload().unwrap()).unwrap();
            break;
          }
          Err(KafkaError::PartitionEOF(_p)) => {
            //println!("EOF p: {}", p);
            break;
          }
          Err(x) => panic!(x)
        }
      }
      None => panic!("Poll returned None")
    }
  }
}

fn cmd_metadata(m: &clap::ArgMatches) {
  let broker = m.value_of("broker").unwrap();
  let mut conf = rdkafka::config::ClientConfig::new();
  conf.set("metadata.broker.list", broker);
  let client = rdkafka::client::Client::new(
    &conf,
    conf.create_native_config().unwrap(),
    rdkafka::types::RDKafkaType::RD_KAFKA_CONSUMER,
    Ctx {},
  ).unwrap();

  let timeout = Some(std::time::Duration::from_millis(1000));
  let metadata = client.fetch_metadata(None, timeout).unwrap();
  let mut items: Vec<_> = metadata.topics().iter().map(|topic| {
    if topic.name() == "__consumer_offsets" { return "  skipped __consumer_offsets".into() }
    let a: Vec<_> = topic.partitions().iter().map(|p| {
      let w = client.fetch_watermarks(topic.name(), p.id(), timeout).unwrap();
      format!("{:?} {:?}", p.id(), w)
    }).collect();
    format!("{}  {}", topic.name(), a.join(", "))
  }).collect();
  items.sort();
  for x in items {
    println!("{}", x);
  }
}

fn main() {
  let app = clap::App::new("kaft")
  .author("Dominik Werder <dominik.werder@gmail.com>")
  .args_from_usage("
    -c,--command=[CMD]  'p, c, m, catp'
    -b,--broker=[BROKER]
    -t,--topic=[TOPIC]
    -r,--rewind=[N]
    -p,--partition=[N]
    -o,--offset=[N]
    --nmsg=[N]
  ");
  let m = app.get_matches();
  //println!("{:?}", m);
  if let Some(c) = m.value_of("command") {
    match c {
      "p" => {
        cmd_produce(&m);
      }
      "c" => {
        cmd_consume(&m);
      }
      "m" => {
        cmd_metadata(&m);
      }
      "catp" => {
        cmd_cat_payload(&m);
      }
      _ => panic!("unknown command")
    }
  }
}
