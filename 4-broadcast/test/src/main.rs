use std::collections::{HashMap, HashSet};
use std::env;
use std::io::Write;

use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;
use rand::prelude::*;
use rand_pcg::Pcg64;
use serde::Serialize;
use serde_json::Value;
use sugars::{rc, refcell};

use dslib::node::LocalEventType;
use dslib::pynode::{JsonMessage, PyNodeFactory};
use dslib::system::System;
use dslib::test::{TestResult, TestSuite};

// UTILS -------------------------------------------------------------------------------------------

#[derive(Serialize)]
struct Message<'a> {
    text: &'a str,
}

#[derive(Clone)]
struct TestConfig {
    solution_path: String,
    node_count: u32,
    seed: u64,
    monkeys: u32,
    debug: bool,
}

fn init_logger(level: LevelFilter) {
    Builder::new()
        .filter(None, level)
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

fn build_system(config: &TestConfig) -> System<JsonMessage> {
    let mut sys = System::with_seed(config.seed);
    let mut node_ids = Vec::new();
    for n in 0..config.node_count {
        node_ids.push(format!("{}", n));
    }
    let node_factory = PyNodeFactory::new(&config.solution_path, "BroadcastNode");
    for node_id in node_ids.iter() {
        let node = node_factory.build(node_id, (node_id, node_ids.clone()), config.seed);
        sys.add_node(rc!(refcell!(node)));
    }
    return sys;
}

fn check(sys: System<JsonMessage>, config: &TestConfig) -> TestResult {
    let mut sent = HashMap::new();
    let mut delivered = HashMap::new();
    let mut all_sent = HashSet::new();
    let mut all_delivered = HashSet::new();
    let mut histories = HashMap::new();
    for node in sys.get_node_ids() {
        let mut history = Vec::new();
        let mut sent_msgs = Vec::new();
        let mut delivered_msgs = Vec::new();
        for e in sys.get_local_events(&node) {
            match e.tip {
                LocalEventType::LocalMessageReceive => {
                    let m = e.msg.unwrap();
                    let data: Value = serde_json::from_str(&m.data).unwrap();
                    let message = data["text"].as_str().unwrap().to_string();
                    sent_msgs.push(message.clone());
                    all_sent.insert(message.clone());
                    history.push(message);
                }
                LocalEventType::LocalMessageSend => {
                    let m = e.msg.unwrap();
                    let data: Value = serde_json::from_str(&m.data).unwrap();
                    let message = data["text"].as_str().unwrap().to_string();
                    delivered_msgs.push(message.clone());
                    all_delivered.insert(message.clone());
                    history.push(message);
                }
            }
        }
        sent.insert(node.clone(), sent_msgs);
        delivered.insert(node.clone(), delivered_msgs);
        histories.insert(node, history);
    }

    if config.debug {
        println!(
            "Messages sent across network: {}",
            sys.get_network_message_count()
        );
        println!("Node histories:");
        for node in sys.get_node_ids() {
            println!(
                "- [node {}] {}",
                node,
                histories.get(&node).unwrap().join(", ")
            );
        }
    }

    // NO DUPLICATION
    let mut no_duplication = true;
    for (_, delivered_msgs) in &delivered {
        let mut uniq = HashSet::new();
        for msg in delivered_msgs {
            if uniq.contains(msg) {
                println!("{}", "Message is duplicated!");
                no_duplication = false;
            };
            uniq.insert(msg);
        }
    }

    // NO CREATION
    let mut no_creation = true;
    for (_, delivered_msgs) in &delivered {
        for msg in delivered_msgs {
            if !all_sent.contains(msg) {
                println!("{}", "Message was not sent!");
                no_creation = false;
            }
        }
    }

    // VALIDITY
    let mut validity = true;
    for (node, sent_msgs) in &sent {
        if sys.node_is_crashed(&node) {
            continue;
        }
        let delivered_msgs = delivered.get(node).unwrap();
        for msg in sent_msgs {
            if !delivered_msgs.contains(msg) {
                println!("Node {} has not delivered its own message {}!", node, msg);
                validity = false;
            }
        }
    }

    // UNIFORM AGREEMENT
    let mut uniform_agreement = true;
    for msg in all_delivered.iter() {
        for (node, delivered_msgs) in &delivered {
            if sys.node_is_crashed(&node) {
                continue;
            }
            if !delivered_msgs.contains(&msg) {
                println!("Message {} is not delivered by correct node {}!", msg, node);
                uniform_agreement = false;
            }
        }
    }

    // CAUSAL ORDER
    let mut causal_order = true;
    for (src, sent_msgs) in &sent {
        for msg in sent_msgs.iter() {
            if !all_delivered.contains(msg) {
                continue;
            }
            // build sender past for send message event
            let mut src_past = HashSet::new();
            for e in histories.get(src).unwrap() {
                if e != msg {
                    src_past.insert(e.clone());
                } else {
                    break;
                }
            }
            // check that other correct nodes have delivered all past events before delivering the message
            for (dst, delivered_msgs) in &delivered {
                if sys.node_is_crashed(&dst) {
                    continue;
                }
                let mut dst_past = HashSet::new();
                for e in delivered_msgs {
                    if e != msg {
                        dst_past.insert(e.clone());
                    } else {
                        break;
                    }
                }
                if !dst_past.is_superset(&src_past) {
                    let missing = src_past
                        .difference(&dst_past)
                        .cloned()
                        .collect::<Vec<String>>();
                    println!(
                        "Causal order violation: {} not delivered [{}] before [{}]",
                        dst,
                        missing.join(", "),
                        msg
                    );
                    causal_order = false;
                }
            }
        }
    }

    if no_duplication & no_creation & validity & uniform_agreement & causal_order {
        Ok(true)
    } else {
        let mut violated = Vec::new();
        if !no_duplication {
            violated.push("NO DUPLICATION")
        }
        if !no_creation {
            violated.push("NO CREATION")
        }
        if !validity {
            violated.push("VALIDITY")
        }
        if !uniform_agreement {
            violated.push("UNIFORM AGREEMENT")
        }
        if !causal_order {
            violated.push("CAUSAL ORDER")
        }
        Err(format!("Violated {}", violated.join(", ")))
    }
}

// TESTS -------------------------------------------------------------------------------------------

fn test_normal(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.send_local(JsonMessage::from("SEND", &Message { text: "0:Hello" }), "0");
    sys.step_until_no_events();
    check(sys, config)
}

fn test_sender_crash(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.send_local(JsonMessage::from("SEND", &Message { text: "0:Hello" }), "0");
    // let 2 messages to deliver (sender and one other node)
    sys.step();
    if sys.get_local_events("0").len() == 1 {
        sys.steps(2);
    } else {
        sys.step();
    }
    // crash source node
    sys.crash_node("0");
    sys.step_until_no_events();
    check(sys, config)
}

fn test_sender_crash2(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.send_local(JsonMessage::from("SEND", &Message { text: "0:Hello" }), "0");
    // let 1 message to deliver (sender only)
    sys.step();
    if sys.get_local_events("0").len() == 1 {
        sys.step();
    }
    sys.crash_node("0");
    sys.step_until_no_events();
    check(sys, config)
}

fn test_two_crashes(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.send_local(JsonMessage::from("SEND", &Message { text: "0:Hello" }), "0");
    // simulate that 0 and 1 communicated only with each other and then crashed
    for n in 2..config.node_count {
        sys.disconnect_node(&n.to_string());
    }
    sys.steps(config.node_count.pow(2));
    sys.crash_node("0");
    sys.crash_node("1");
    sys.step_until_no_events();
    check(sys, config)
}

fn test_two_crashes2(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.send_local(JsonMessage::from("SEND", &Message { text: "0:Hello" }), "0");
    // simulate that 1 and 2 communicated only with 0 and then crashed
    sys.drop_outgoing("1");
    sys.drop_outgoing("2");
    sys.steps(config.node_count.pow(2));
    sys.crash_node("1");
    sys.crash_node("2");
    sys.step_until_no_events();
    check(sys, config)
}

fn test_causal_order(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    sys.set_delays(100., 200.);
    sys.send_local(
        JsonMessage::from("SEND", &Message { text: "0:Hello!" }),
        "0",
    );
    while sys.get_local_events("1").len() == 0 {
        sys.step();
    }
    sys.set_delays(10., 20.);
    sys.send_local(JsonMessage::from("SEND", &Message { text: "1:How?" }), "1");
    while sys.get_local_events("0").len() < 3 {
        sys.step();
    }
    sys.set_delay(1.);
    sys.send_local(JsonMessage::from("SEND", &Message { text: "0:Fine!" }), "0");
    sys.step_until_no_events();
    check(sys, config)
}

fn test_chaos_monkey(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    for i in 1..=config.monkeys {
        let mut run_config = config.clone();
        run_config.seed = rand.next_u64();
        println!("- Run {} (seed: {})", i, run_config.seed);
        let mut sys = build_system(config);
        let victim1 = rand.gen_range(0..config.node_count).to_string();
        let mut victim2 = rand.gen_range(0..config.node_count).to_string();
        while victim2 == victim1 {
            victim2 = rand.gen_range(0..config.node_count).to_string();
        }
        for i in 0..10 {
            let user = rand.gen_range(0..config.node_count).to_string();
            let message = format!("{}:{}", user, i);
            sys.send_local(
                JsonMessage::from("SEND", &Message { text: &message }),
                &user,
            );
            if i % 2 == 0 {
                sys.set_delays(10., 20.);
            } else {
                sys.set_delays(1., 2.);
            }
            for _ in 1..10 {
                if rand.gen_range(0.0..1.0) > 0.3 {
                    sys.drop_outgoing(&victim1);
                } else {
                    sys.pass_outgoing(&victim1);
                }
                if rand.gen_range(0.0..1.0) > 0.3 {
                    sys.drop_outgoing(&victim2);
                } else {
                    sys.pass_outgoing(&victim2);
                }
                sys.steps(rand.gen_range(1..5));
            }
        }
        sys.crash_node(&victim1);
        sys.crash_node(&victim2);
        sys.step_until_no_events();
        check(sys, config)?;
    }
    Ok(true)
}

#[allow(dead_code)]
fn test_scalability(config: &TestConfig) -> TestResult {
    let sys_sizes = [
        config.node_count,
        config.node_count * 2,
        config.node_count * 4,
        config.node_count * 10,
    ];
    let mut msg_counts = Vec::new();
    for node_count in sys_sizes {
        let mut run_config = config.clone();
        run_config.node_count = node_count;
        let mut sys = build_system(&run_config);
        sys.send_local(
            JsonMessage::from("SEND", &Message { text: "0:Hello!" }),
            "0",
        );
        sys.step_until_no_events();
        msg_counts.push(sys.get_network_message_count());
    }
    println!("\nMessage count:");
    for i in 0..sys_sizes.len() {
        let baseline = (sys_sizes[i] * (sys_sizes[i] - 1)) as u64;
        println!(
            "- N={}: {} (baseline {})",
            sys_sizes[i], msg_counts[i], baseline
        );
    }
    Ok(true)
}

// CLI -----------------------------------------------------------------------------------------------------------------

/// Broadcast Homework Tests
#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    /// Path to Python file with solution
    #[clap(long = "impl", short = 'i', default_value = "../solution.py")]
    solution_path: String,

    /// Test to run (optional)
    #[clap(long = "test", short)]
    test: Option<String>,

    /// Print execution trace
    #[clap(long, short)]
    debug: bool,

    /// Random seed used in tests
    #[clap(long, short, default_value = "123")]
    seed: u64,

    /// Number of nodes used in tests
    #[clap(long, short, default_value = "5")]
    node_count: u32,

    /// Number of chaos monkey runs
    #[clap(long, short, default_value = "10")]
    monkeys: u32,

    /// Path to dslib directory
    #[clap(long = "lib", short = 'l', default_value = "../../dslib")]
    dslib_path: String,
}

// MAIN --------------------------------------------------------------------------------------------

fn main() {
    let args = Args::parse();
    if args.debug {
        init_logger(LevelFilter::Trace);
    }
    env::set_var("PYTHONPATH", format!("{}/python", args.dslib_path));
    let config = TestConfig {
        solution_path: args.solution_path,
        node_count: args.node_count,
        seed: args.seed,
        monkeys: args.monkeys,
        debug: args.debug,
    };
    let mut tests = TestSuite::new();

    tests.add("NORMAL", test_normal, config.clone());
    tests.add("SENDER CRASH", test_sender_crash, config.clone());
    tests.add("SENDER CRASH 2", test_sender_crash2, config.clone());
    tests.add("TWO CRASHES", test_two_crashes, config.clone());
    tests.add("TWO CRASHES 2", test_two_crashes2, config.clone());
    tests.add("CAUSAL ORDER", test_causal_order, config.clone());
    tests.add("CHAOS MONKEY", test_chaos_monkey, config.clone());
    tests.add("SCALABILITY", test_scalability, config.clone());

    if args.test.is_none() {
        tests.run();
    } else {
        tests.run_test(&args.test.unwrap());
    }
}
