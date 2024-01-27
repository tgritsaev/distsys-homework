use std::collections::{HashMap, HashSet};
use std::env;
use std::io::Write;

use assertables::{assume, assume_eq};
use clap::Parser;
use decorum::R64;
use env_logger::Builder;
use log::LevelFilter;
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use rand_pcg::Pcg64;
use serde::{Deserialize, Serialize};
use sugars::{rc, refcell};

use dslib::pynode::{JsonMessage, PyNodeFactory};
use dslib::system::System;
use dslib::test::{TestResult, TestSuite};

// MESSAGES ------------------------------------------------------------------------------------------------------------

#[derive(Serialize)]
struct GetMessage<'a> {
    key: &'a str,
}

#[derive(Deserialize)]
struct GetRespMessage<'a> {
    key: &'a str,
    value: Option<&'a str>,
}

#[derive(Serialize)]
struct PutMessage<'a> {
    key: &'a str,
    value: &'a str,
}

#[derive(Deserialize)]
struct PutRespMessage<'a> {
    key: &'a str,
    value: &'a str,
}

#[derive(Serialize)]
struct DeleteMessage<'a> {
    key: &'a str,
}

#[derive(Deserialize)]
struct DeleteRespMessage<'a> {
    key: &'a str,
    value: Option<&'a str>,
}

#[derive(Serialize)]
struct DumpKeysMessage {}

#[derive(Deserialize)]
struct DumpKeysRespMessage {
    keys: HashSet<String>,
}

#[derive(Serialize)]
struct CountRecordsMessage {}

#[derive(Deserialize)]
struct CountRecordsRespMessage {
    count: u64,
}

#[derive(Serialize)]
struct NodeAddedMessage<'a> {
    id: &'a str,
}

#[derive(Serialize)]
struct NodeRemovedMessage<'a> {
    id: &'a str,
}

// UTILS ---------------------------------------------------------------------------------------------------------------

#[derive(Copy, Clone)]
struct TestConfig<'a> {
    node_factory: &'a PyNodeFactory,
    node_count: u32,
    seed: u64,
}

fn init_logger(level: LevelFilter) {
    Builder::new()
        .filter(None, level)
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
}

fn build_system(config: &TestConfig, measure_max_size: bool) -> System<JsonMessage> {
    let mut sys = System::with_seed(config.seed);
    sys.set_delays(0.01, 0.1);
    let mut node_ids = Vec::new();
    for n in 0..config.node_count {
        node_ids.push(format!("{}", n));
    }
    for node_id in node_ids.iter() {
        let mut node = config
            .node_factory
            .build(node_id, (node_id, node_ids.clone()), config.seed);
        if measure_max_size {
            node.set_max_size_freq(1000000);
        }
        sys.add_node(rc!(refcell!(node)));
    }
    return sys;
}

fn add_node(node_id: &str, sys: &mut System<JsonMessage>, config: &TestConfig) {
    let mut node_ids = Vec::new();
    for id in sys.get_node_ids() {
        node_ids.push(id);
    }
    node_ids.push(node_id.to_string());
    let node = config
        .node_factory
        .build(node_id, (node_id, node_ids), config.seed);
    sys.add_node(rc!(refcell!(node)));
}

fn check_get(
    sys: &mut System<JsonMessage>,
    node: &str,
    key: &str,
    expected: Option<&str>,
    max_steps: u32,
) -> TestResult {
    sys.send_local(JsonMessage::from("GET", &GetMessage { key }), node);
    let res = sys.step_until_local_message_max_steps(node, max_steps);
    assume!(res.is_ok(), format!("GET_RESP is not returned by {}", node))?;
    let msgs = res.unwrap();
    let msg = msgs.first().unwrap();
    assume_eq!(msg.tip, "GET_RESP")?;
    let data: GetRespMessage = serde_json::from_str(&msg.data).unwrap();
    assume_eq!(data.key, key)?;
    assume_eq!(data.value, expected)?;
    Ok(true)
}

fn check_put(
    sys: &mut System<JsonMessage>,
    node: &str,
    key: &str,
    value: &str,
    max_steps: u32,
) -> TestResult {
    sys.send_local(JsonMessage::from("PUT", &PutMessage { key, value }), node);
    let res = sys.step_until_local_message_max_steps(node, max_steps);
    assume!(res.is_ok(), format!("PUT_RESP is not returned by {}", node))?;
    let msgs = res.unwrap();
    let msg = msgs.first().unwrap();
    assume_eq!(msg.tip, "PUT_RESP")?;
    let data: PutRespMessage = serde_json::from_str(&msg.data).unwrap();
    assume_eq!(data.key, key)?;
    assume_eq!(data.value, value)?;
    Ok(true)
}

fn check_delete(
    sys: &mut System<JsonMessage>,
    node: &str,
    key: &str,
    expected: Option<&str>,
    max_steps: u32,
) -> TestResult {
    sys.send_local(JsonMessage::from("DELETE", &DeleteMessage { key }), node);
    let res = sys.step_until_local_message_max_steps(node, max_steps);
    assume!(
        res.is_ok(),
        format!("DELETE_RESP is not returned by {}", node)
    )?;
    let msgs = res.unwrap();
    let msg = msgs.first().unwrap();
    assume_eq!(msg.tip, "DELETE_RESP")?;
    let data: DeleteRespMessage = serde_json::from_str(&msg.data).unwrap();
    assume_eq!(data.key, key)?;
    assume_eq!(data.value, expected)?;
    Ok(true)
}

fn dump_keys(sys: &mut System<JsonMessage>, node: &str) -> Result<HashSet<String>, String> {
    sys.send_local(JsonMessage::from("DUMP_KEYS", &DumpKeysMessage {}), node);
    let res = sys.step_until_local_message_max_steps(node, 100);
    assume!(
        res.is_ok(),
        format!("DUMP_KEYS_RESP is not returned by {}", node)
    )?;
    let msgs = res.unwrap();
    let msg = msgs.first().unwrap();
    assume_eq!(msg.tip, "DUMP_KEYS_RESP")?;
    let data: DumpKeysRespMessage = serde_json::from_str(&msg.data).unwrap();
    Ok(data.keys)
}

fn key_distribution(
    sys: &mut System<JsonMessage>,
) -> Result<HashMap<String, HashSet<String>>, String> {
    let mut dist = HashMap::new();
    for node in sys.get_node_ids() {
        dist.insert(node.clone(), dump_keys(sys, &node)?);
    }
    Ok(dist)
}

fn count_records(sys: &mut System<JsonMessage>, node: &str) -> Result<u64, String> {
    sys.send_local(
        JsonMessage::from("COUNT_RECORDS", &CountRecordsMessage {}),
        node,
    );
    let res = sys.step_until_local_message_max_steps(node, 100);
    assume!(
        res.is_ok(),
        format!("COUNT_RECORDS_RESP is not returned by {}", node)
    )?;
    let msgs = res.unwrap();
    let msg = msgs.first().unwrap();
    assume_eq!(msg.tip, "COUNT_RECORDS_RESP")?;
    let data: CountRecordsRespMessage = serde_json::from_str(&msg.data).unwrap();
    Ok(data.count)
}

fn send_node_added(sys: &mut System<JsonMessage>, added: &str) {
    for node in sys.get_node_ids() {
        sys.send_local(
            JsonMessage::from("NODE_ADDED", &NodeAddedMessage { id: added }),
            &node,
        );
    }
}

fn send_node_removed(sys: &mut System<JsonMessage>, removed: &str) {
    for node in sys.get_node_ids() {
        sys.send_local(
            JsonMessage::from("NODE_REMOVED", &NodeRemovedMessage { id: removed }),
            &node,
        );
    }
}

fn step_until_stabilized(
    sys: &mut System<JsonMessage>,
    nodes: &Vec<String>,
    expected_keys: u64,
    steps_per_iter: u32,
    max_steps: u32,
) -> TestResult {
    let mut stabilized = false;
    let mut steps = 0;
    let mut counts = HashMap::new();
    let mut total_count: u64 = 0;
    for node in nodes.iter() {
        let count = count_records(sys, node)?;
        counts.insert(node, count);
        total_count += count;
    }

    while !stabilized && steps <= max_steps {
        sys.steps(steps_per_iter);
        steps += steps_per_iter;
        total_count = 0;
        let mut count_changed = false;
        for node in nodes.iter() {
            let count = count_records(sys, node)?;
            if *counts.get(node).unwrap() != count {
                count_changed = true;
            }
            counts.insert(node, count);
            total_count += count;
        }
        if total_count == expected_keys && !count_changed {
            stabilized = true;
        }
    }

    assume!(
        stabilized,
        format!(
            "Keys distribution is not stabilized (keys observed = {}, expected = {})",
            total_count, expected_keys
        )
    )
}

fn check(
    sys: &mut System<JsonMessage>,
    nodes: &Vec<String>,
    expected: &HashMap<String, String>,
    check_values: bool,
    check_distribution: bool,
) -> TestResult {
    let mut stored_keys = HashSet::new();
    let mut node_key_counts = Vec::new();
    for node in nodes.iter() {
        let node_count = count_records(sys, &node)?;
        let node_keys = dump_keys(sys, &node)?;
        assume_eq!(node_keys.len() as u64, node_count)?;
        stored_keys.extend(node_keys);
        node_key_counts.push(node_count);
    }

    // all keys are stored
    assume!(
        expected.len() == stored_keys.len() && expected.keys().all(|k| stored_keys.contains(k)),
        "Stored keys do not mach expected"
    )?;

    // each key is stored on a single node
    assume!(
        node_key_counts.iter().sum::<u64>() == stored_keys.len() as u64,
        "Keys are not stored on a single node"
    )?;

    // check values
    if check_values {
        println!("\nChecking values:");
        for node in nodes.iter() {
            for (k, v) in expected.iter() {
                check_get(sys, &node, k, Some(v), 100)?;
            }
        }
        println!("OK")
    }

    // check keys distribution
    if check_distribution {
        let target_count = (expected.len() as f64 / node_key_counts.len() as f64).round();
        let max_count = *node_key_counts.iter().max().unwrap();
        let min_count = *node_key_counts.iter().min().unwrap();
        let deviations: Vec<f64> = node_key_counts
            .iter()
            .map(|x| (target_count - *x as f64).abs() / target_count)
            .collect();
        let avg_deviation = deviations.iter().sum::<f64>() / node_key_counts.len() as f64;
        let max_deviation = deviations
            .iter()
            .map(|x| R64::from_inner(*x))
            .max()
            .unwrap();
        println!("\nStored keys per node:");
        println!("  - target: {}", target_count);
        println!("  - min: {}", min_count);
        println!("  - max: {}", max_count);
        println!("  - average deviation from target: {:.3}", avg_deviation);
        println!("  - max deviation from target: {:.3}", max_deviation);
        assume!(
            max_deviation <= 0.1,
            "Max deviation from target is above 10%"
        )?;
    }

    Ok(true)
}

fn check_moved_keys(
    sys: &mut System<JsonMessage>,
    before: &HashMap<String, HashSet<String>>,
    after: &HashMap<String, HashSet<String>>,
    target: u64,
) -> TestResult {
    let mut total_count = 0;
    let mut not_moved_count = 0;
    let empty = HashSet::new();
    for node in sys.get_node_ids() {
        let b = before.get(&node).unwrap_or(&empty);
        let a = after.get(&node).unwrap_or(&empty);
        let not_moved: HashSet<String> = a.intersection(&b).cloned().collect();
        not_moved_count += not_moved.len() as u64;
        total_count += b.len() as u64;
    }
    let moved_count = total_count - not_moved_count;
    let deviation = (moved_count as f64 - target as f64) / target as f64;
    println!("\nMoved keys:");
    println!("  - target: {}", target);
    println!("  - observed: {}", moved_count);
    println!("  - deviation: {:.3}", deviation);
    assume!(
        deviation <= 0.1,
        format!("Deviation from target is above 10%")
    )
}

const SYMBOLS: [char; 36] = [
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
    't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
];
const WEIGHTS: [usize; 36] = [
    13, 16, 3, 8, 8, 5, 6, 23, 4, 8, 24, 12, 2, 1, 1, 10, 5, 8, 10, 1, 24, 3, 1, 8, 12, 22, 5, 20,
    18, 5, 5, 2, 1, 3, 16, 22,
];

fn random_string(length: usize, rand: &mut Pcg64) -> String {
    let dist = WeightedIndex::new(&WEIGHTS).unwrap();
    rand.sample_iter(&dist)
        .take(length)
        .map(|x| SYMBOLS[x])
        .collect()
}

// TESTS ---------------------------------------------------------------------------------------------------------------

fn test_single_node(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    let node = "0";
    let key = random_string(8, &mut rand).to_uppercase();
    let value = random_string(8, &mut rand);
    let max_steps = 10;

    check_get(&mut sys, node, &key, None, max_steps)?;
    check_put(&mut sys, node, &key, &value, max_steps)?;
    check_get(&mut sys, node, &key, Some(&value), max_steps)?;
    check_delete(&mut sys, node, &key, Some(&value), max_steps)?;
    check_get(&mut sys, node, &key, None, max_steps)?;
    check_delete(&mut sys, node, &key, None, max_steps)
}

fn test_inserts(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    // insert random key-value pairs from each node
    let mut kv = HashMap::new();
    for node in sys.get_node_ids() {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        check_put(&mut sys, &node, &k, &v, 100)?;
        kv.insert(k, v);
    }

    // check that all key-values can be read from each node
    let nodes = sys.get_node_ids();
    check(&mut sys, &nodes, &kv, true, false)
}

fn test_deletes(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut kv = HashMap::new();

    // insert random key-value pairs from each node
    for node in sys.get_node_ids() {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        check_put(&mut sys, &node, &k, &v, 100)?;
        kv.insert(k, v);
    }

    // delete each key from one node and check that key is not present from another
    for (k, v) in kv.iter() {
        let read_node = sys.get_node_ids().choose(&mut rand).unwrap().clone();
        let mut delete_node = sys.get_node_ids().choose(&mut rand).unwrap().clone();
        while delete_node == read_node {
            delete_node = sys.get_node_ids().choose(&mut rand).unwrap().clone();
        }
        check_get(&mut sys, &read_node, k, Some(v), 100)?;
        check_delete(&mut sys, &delete_node, k, Some(v), 100)?;
        check_get(&mut sys, &read_node, k, None, 100)?;
    }

    kv.clear();
    let nodes = sys.get_node_ids();
    check(&mut sys, &nodes, &kv, false, false)
}

fn test_memory_overhead(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, true);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    // insert random key-value pairs
    let keys_count = 10000;
    let mut kv = HashMap::new();
    for _ in 0..keys_count {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        let node = sys.get_node_ids().choose(&mut rand).unwrap().clone();
        check_put(&mut sys, &node, &k, &v, 100)?;
        kv.insert(k, v);
    }

    let mut total_mem_size = 0;
    for node in sys.get_node_ids() {
        total_mem_size += sys.get_max_size(&node)
    }
    let mem_size_per_key = total_mem_size as f64 / keys_count as f64;
    println!("Mem size per key: {}", mem_size_per_key);
    assume!(
        mem_size_per_key <= 300.,
        format!("Too big memory overhead (probably you use naive key->node mapping)")
    )
}

fn test_node_added(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    // insert random key-value pairs
    let keys_count = 100;
    let mut kv = HashMap::new();
    for _ in 0..keys_count {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        let node = sys.get_node_ids().choose(&mut rand).unwrap().clone();
        check_put(&mut sys, &node, &k, &v, 100)?;
        kv.insert(k, v);
    }

    // add new node to the system
    let added = format!("{}", sys.get_node_ids().len());
    add_node(&added, &mut sys, config);
    send_node_added(&mut sys, &added);

    // run the system until key the distribution is stabilized
    let nodes = sys.get_node_ids();
    step_until_stabilized(&mut sys, &nodes, kv.len() as u64, 100, 1000)?;

    check(&mut sys, &nodes, &kv, true, false)
}

fn test_node_removed(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    // insert random key-value pairs
    let keys_count = 100;
    let mut kv = HashMap::new();
    for _ in 0..keys_count {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        let node = sys.get_node_ids().choose(&mut rand).unwrap().clone();
        check_put(&mut sys, &node, &k, &v, 100)?;
        kv.insert(k, v);
    }

    // remove a node from the system
    let removed = sys.get_node_ids().choose(&mut rand).unwrap().clone();
    let count = count_records(&mut sys, &removed)?;
    assume!(count > 0, "Node stores no records, bad distribution")?;
    send_node_removed(&mut sys, &removed);

    // run the system until key the distribution is stabilized
    let nodes: Vec<String> = sys
        .get_node_ids()
        .into_iter()
        .filter(|x| *x != removed)
        .collect();
    step_until_stabilized(&mut sys, &nodes, kv.len() as u64, 100, 1000)?;

    check(&mut sys, &nodes, &kv, true, false)
}

fn test_node_removed_after_crash(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    // insert random key-value pairs
    let keys_count = 100;
    let mut kv = HashMap::new();
    for _ in 0..keys_count {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        let node = sys.get_node_ids().choose(&mut rand).unwrap().clone();
        check_put(&mut sys, &node, &k, &v, 100)?;
        kv.insert(k, v);
    }

    // crash a node and remove it from the system (stored keys are lost)
    let crashed = sys.get_node_ids().choose(&mut rand).unwrap().clone();
    let crashed_keys = dump_keys(&mut sys, &crashed)?;
    assume!(
        crashed_keys.len() > 0,
        "Node stores no records, bad distribution"
    )?;
    for k in crashed_keys {
        kv.remove(&k);
    }
    sys.crash_node(&crashed);
    send_node_removed(&mut sys, &crashed);

    // run the system until key the distribution is stabilized
    let nodes: Vec<String> = sys
        .get_node_ids()
        .into_iter()
        .filter(|x| *x != crashed)
        .collect();
    step_until_stabilized(&mut sys, &nodes, kv.len() as u64, 100, 1000)?;

    check(&mut sys, &nodes, &kv, true, false)
}

fn test_migration(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut nodes = sys.get_node_ids().clone();

    // insert random key-value pairs
    let keys_count = 1000;
    let mut kv = HashMap::new();
    for _ in 0..keys_count {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        let node = sys.get_node_ids().choose(&mut rand).unwrap().clone();
        check_put(&mut sys, &node, &k, &v, 100)?;
        kv.insert(k, v);
    }

    // add new N nodes to the system
    for i in 0..config.node_count {
        let added = format!("{}", config.node_count + i);
        add_node(&added, &mut sys, config);
        send_node_added(&mut sys, &added);
        nodes.push(added);
        step_until_stabilized(&mut sys, &nodes, kv.len() as u64, 100, 1000)?;
    }

    check(&mut sys, &nodes, &kv, false, false)?;

    // remove old N nodes
    for i in 0..config.node_count {
        let removed = format!("{}", i);
        send_node_removed(&mut sys, &removed);
        nodes.remove(0);
        step_until_stabilized(&mut sys, &nodes, kv.len() as u64, 100, 1000)?;
    }

    check(&mut sys, &nodes, &kv, false, false)
}

fn test_scale_up_down(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut nodes = sys.get_node_ids().clone();

    // insert random key-value pairs
    let keys_count = 1000;
    let mut kv = HashMap::new();
    for _ in 0..keys_count {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        let node = sys.get_node_ids().choose(&mut rand).unwrap().clone();
        check_put(&mut sys, &node, &k, &v, 100)?;
        kv.insert(k, v);
    }

    // add new N nodes to the system
    for i in 0..config.node_count {
        let added = format!("{}", config.node_count + i);
        add_node(&added, &mut sys, config);
        send_node_added(&mut sys, &added);
        nodes.push(added);
        step_until_stabilized(&mut sys, &nodes, kv.len() as u64, 100, 1000)?;
    }

    check(&mut sys, &nodes, &kv, false, false)?;

    // remove new N nodes
    for i in 0..config.node_count {
        let removed = format!("{}", config.node_count + i);
        send_node_removed(&mut sys, &removed);
        nodes.remove(config.node_count as usize);
        step_until_stabilized(&mut sys, &nodes, kv.len() as u64, 100, 1000)?;
    }

    check(&mut sys, &nodes, &kv, false, false)
}

fn test_distribution(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);

    // insert random key-value pairs
    let keys_count = 10000;
    let mut kv = HashMap::new();
    for _ in 0..keys_count {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        let node = sys.get_node_ids().choose(&mut rand).unwrap().clone();
        check_put(&mut sys, &node, &k, &v, 100)?;
        kv.insert(k, v);
    }

    let nodes = sys.get_node_ids();
    check(&mut sys, &nodes, &kv, false, true)
}

fn test_distribution_node_added(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut kv = HashMap::new();

    // insert random key-value pairs
    let keys_count = 10000;
    for _ in 0..keys_count {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        let node = sys.get_node_ids().choose(&mut rand).unwrap().clone();
        check_put(&mut sys, &node, &k, &v, 100)?;
        kv.insert(k, v);
    }
    let dist_before = key_distribution(&mut sys)?;

    // add new node to the system
    let added = format!("{}", sys.get_node_ids().len());
    add_node(&added, &mut sys, config);
    send_node_added(&mut sys, &added);

    // run the system until key the distribution is stabilized
    let nodes = sys.get_node_ids();
    step_until_stabilized(&mut sys, &nodes, kv.len() as u64, 100, 1000)?;
    let dist_after = key_distribution(&mut sys)?;

    let target_moved_keys = (keys_count as f64 / nodes.len() as f64).round() as u64;
    check_moved_keys(&mut sys, &dist_before, &dist_after, target_moved_keys)?;

    check(&mut sys, &nodes, &kv, false, true)
}

fn test_distribution_node_removed(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config, false);
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut kv = HashMap::new();

    // insert random key-value pairs
    let keys_count = 10000;
    for _ in 0..keys_count {
        let k = random_string(8, &mut rand).to_uppercase();
        let v = random_string(8, &mut rand);
        let node = sys.get_node_ids().choose(&mut rand).unwrap().clone();
        check_put(&mut sys, &node, &k, &v, 100)?;
        kv.insert(k, v);
    }
    let dist_before = key_distribution(&mut sys)?;

    // remove a node from the system
    let removed = sys.get_node_ids().choose(&mut rand).unwrap().clone();
    let count = count_records(&mut sys, &removed)?;
    assume!(count > 0, "Node stores no records, bad distribution")?;
    send_node_removed(&mut sys, &removed);

    // run the system until key the distribution is stabilized
    let nodes: Vec<String> = sys
        .get_node_ids()
        .into_iter()
        .filter(|x| *x != removed)
        .collect();
    step_until_stabilized(&mut sys, &nodes, kv.len() as u64, 100, 1000)?;
    let dist_after = key_distribution(&mut sys)?;

    let target_moved_keys = (keys_count as f64 / (nodes.len() + 1) as f64).round() as u64;
    check_moved_keys(&mut sys, &dist_before, &dist_after, target_moved_keys)?;

    check(&mut sys, &nodes, &kv, false, true)
}

// CLI -----------------------------------------------------------------------------------------------------------------

/// Sharded KV Store Homework Tests
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

    /// Number of nodes used in tests
    #[clap(long, short, default_value = "10")]
    node_count: u32,

    /// Random seed used in tests
    #[clap(long, short, default_value = "123")]
    seed: u64,

    /// Path to dslib directory
    #[clap(long = "lib", short = 'l', default_value = "../../dslib")]
    dslib_path: String,
}

// MAIN ----------------------------------------------------------------------------------------------------------------

fn main() {
    let args = Args::parse();
    if args.debug {
        init_logger(LevelFilter::Trace);
    }
    env::set_var("PYTHONPATH", format!("{}/python", args.dslib_path));
    env::set_var("PYTHONHASHSEED", args.seed.to_string());
    let node_factory = PyNodeFactory::new(&args.solution_path, "StorageNode");
    let config = TestConfig {
        node_factory: &node_factory,
        node_count: args.node_count,
        seed: args.seed,
    };
    let mut single_config = config.clone();
    single_config.node_count = 1;
    let mut tests = TestSuite::new();

    tests.add("SINGLE NODE", test_single_node, single_config);
    tests.add("INSERTS", test_inserts, config);
    tests.add("DELETES", test_deletes, config);
    tests.add("MEMORY OVERHEAD", test_memory_overhead, config);
    tests.add("NODE ADDED", test_node_added, config);
    tests.add("NODE REMOVED", test_node_removed, config);
    tests.add(
        "NODE REMOVED AFTER CRASH",
        test_node_removed_after_crash,
        config,
    );
    tests.add("MIGRATION", test_migration, config);
    tests.add("SCALE UP DOWN", test_scale_up_down, config);
    tests.add("DISTRIBUTION", test_distribution, config);
    tests.add(
        "DISTRIBUTION NODE ADDED",
        test_distribution_node_added,
        config,
    );
    tests.add(
        "DISTRIBUTION NODE REMOVED",
        test_distribution_node_removed,
        config,
    );

    if args.test.is_none() {
        tests.run();
    } else {
        tests.run_test(&args.test.unwrap());
    }
}
