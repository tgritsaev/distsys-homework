use std::collections::{HashMap, HashSet};
use std::env;
use std::io::Write;

use assertables::{assume, assume_eq};
use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;
use rand::prelude::*;
use rand_pcg::Pcg64;
use serde::{Deserialize, Serialize};
use sugars::{rc, refcell};

use dslib::pynode::{JsonMessage, PyNodeFactory};
use dslib::system::System;
use dslib::test::{TestResult, TestSuite};

// UTILS -------------------------------------------------------------------------------------------

#[derive(Serialize)]
struct JoinMessage<'a> {
    seed: &'a str,
}

#[derive(Serialize)]
struct LeaveMessage {}

#[derive(Serialize)]
struct GetMembersMessage {}

#[derive(Deserialize)]
struct MembersMessage {
    members: Vec<String>,
}

#[derive(Clone)]
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

fn build_system(config: &TestConfig) -> System<JsonMessage> {
    let mut sys = System::with_seed(config.seed);
    sys.set_delays(0.01, 0.1);
    for n in 0..config.node_count {
        let node_id = format!("{}", n);
        let node = config
            .node_factory
            .build(&node_id, (&node_id,), config.seed);
        sys.add_node(rc!(refcell!(node)));
        let clock_skew = sys.gen_range(0.0..10.0);
        sys.set_clock_skew(&node_id, clock_skew);
    }
    return sys;
}

fn recover_node(node_id: &str, sys: &mut System<JsonMessage>, config: &TestConfig) {
    let node = config.node_factory.build(node_id, (node_id,), config.seed);
    sys.add_node(rc!(refcell!(node)));
}

fn step_until_stabilized(sys: &mut System<JsonMessage>, group: HashSet<String>) -> TestResult {
    let max_time = sys.time() + 300.; // timeout is 5 minutes
    let mut stabilized = HashSet::new();
    let mut memberlists = HashMap::new();

    while stabilized.len() < group.len() && sys.time() < max_time {
        sys.step_for_duration(5.);
        stabilized.clear();
        for node in group.iter() {
            sys.send_local(
                JsonMessage::from("GET_MEMBERS", &GetMembersMessage {}),
                &node,
            );
            let res = sys.step_until_local_message_with_timeout(&node, 10.);
            assume!(
                res.is_ok(),
                format!("Members list is not returned by {}", &node)
            )?;
            let msgs = res.unwrap();
            let msg = msgs.first().unwrap();
            assume!(msg.tip == "MEMBERS", "Wrong message type")?;
            let data: MembersMessage = serde_json::from_str(&msg.data).unwrap();
            let members: HashSet<String> = data.members.clone().into_iter().collect();
            if members.eq(&group) {
                stabilized.insert(node.clone());
            }
            memberlists.insert(node.clone(), data.members);
        }
        // if !cont {
        //     break;
        // }
    }

    if stabilized != group && group.len() <= 10 {
        println!("Members lists:");
        for node in sys.get_node_ids() {
            if group.contains(&node) {
                let members = memberlists.get_mut(&node).unwrap();
                members.sort();
                println!("- [{}] {}", node, members.join(", "));
            }
        }
        let mut expected = group.clone().into_iter().collect::<Vec<_>>();
        expected.sort();
        println!("Expected group: {}", expected.join(", "));
    }
    assume_eq!(stabilized, group, "Group members lists are not stabilized")?;
    Ok(true)
}

// TESTS -------------------------------------------------------------------------------------------

fn test_simple(config: &TestConfig) -> TestResult {
    let mut sys = build_system(config);
    let seed = "0";
    for node in sys.get_node_ids() {
        sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), &node);
    }
    let group = sys.get_node_ids().clone().into_iter().collect();
    step_until_stabilized(&mut sys, group)
}

fn test_random_seed(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = Vec::new();
    for node in sys.get_node_ids() {
        let seed = match group.len() {
            0 => &node,
            _ => group.choose(&mut rand).unwrap(),
        };
        sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), &node);
        group.push(node);
    }
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_node_join(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.get_node_ids();
    group.shuffle(&mut rand);
    let new_node = group.remove(rand.gen_range(0..group.len()));
    let seed = &group.get(0).unwrap();

    for node in &group {
        if *node != new_node {
            sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
        }
    }
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // node joins the system
    sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), &new_node);
    group.push(new_node);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_node_leave(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.get_node_ids();
    group.shuffle(&mut rand);
    let seed = &group.get(0).unwrap();

    for node in &group {
        sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
    }
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // node leaves the system
    let left_node = group.remove(rand.gen_range(0..group.len()));
    sys.send_local(JsonMessage::from("LEAVE", &LeaveMessage {}), &left_node);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_node_crash(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.get_node_ids();
    group.shuffle(&mut rand);
    let seed = &group.get(0).unwrap();

    for node in &group {
        sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
    }
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // node crashes
    let crashed_node = group.remove(rand.gen_range(0..group.len()));
    sys.crash_node(&crashed_node);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_seed_node_crash(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.get_node_ids();
    group.shuffle(&mut rand);
    let seed = &group.get(0).cloned().unwrap();

    for node in &group {
        sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
    }
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // seed node crashes
    group.remove(0);
    sys.crash_node(&seed);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_node_crash_recover(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.get_node_ids();
    group.shuffle(&mut rand);
    let seed = &group.get(0).cloned().unwrap();

    for node in &group {
        sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
    }
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // node crashes
    let crashed_node = group.remove(rand.gen_range(0..group.len()));
    sys.crash_node(&crashed_node);
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // node recovers
    recover_node(&crashed_node, &mut sys, &config);
    sys.send_local(
        JsonMessage::from("JOIN", &JoinMessage { seed }),
        &crashed_node,
    );
    group.push(crashed_node);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_node_offline(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.get_node_ids();
    group.shuffle(&mut rand);
    let seed = &group.get(0).unwrap();

    for node in &group {
        sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
    }
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // node goes offline
    let offline_node = group.remove(rand.gen_range(0..group.len()));
    sys.disconnect_node(&offline_node);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_seed_node_offline(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.get_node_ids();
    group.shuffle(&mut rand);
    let seed = &group.get(0).cloned().unwrap();

    for node in &group {
        sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
    }
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // seed node goes offline
    group.remove(0);
    sys.disconnect_node(&seed);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_node_offline_recover(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.get_node_ids();
    group.shuffle(&mut rand);
    let seed = &group.get(0).unwrap();

    for node in &group {
        sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
    }
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // node goes offline
    let offline_node = group.remove(rand.gen_range(0..group.len()));
    sys.disconnect_node(&offline_node);
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // node goes back online
    sys.connect_node(&offline_node);
    group.push(offline_node);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_network_partition(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.get_node_ids();
    group.shuffle(&mut rand);
    let seed = &group.get(0).unwrap();

    for node in &group {
        sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
    }
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // network is partitioned
    let (group1, group2): (Vec<_>, Vec<_>) = group
        .iter()
        .map(|s| &**s)
        .partition(|_| rand.gen_range(0.0..1.0) > 0.6);
    sys.make_partition(&group1, &group2);
    step_until_stabilized(&mut sys, group1.into_iter().map(String::from).collect())?;
    step_until_stabilized(&mut sys, group2.into_iter().map(String::from).collect())
}

fn test_network_partition_recover(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.get_node_ids();
    group.shuffle(&mut rand);
    let seed = &group.get(0).unwrap();

    for node in &group {
        sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
    }
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // network is partitioned
    let (group1, group2): (Vec<_>, Vec<_>) = group
        .iter()
        .map(|s| &**s)
        .partition(|_| rand.gen_range(0.0..1.0) > 0.6);
    sys.make_partition(&group1, &group2);
    step_until_stabilized(&mut sys, group1.into_iter().map(String::from).collect())?;
    step_until_stabilized(&mut sys, group2.into_iter().map(String::from).collect())?;

    // network is recovered
    sys.reset_network();
    step_until_stabilized(&mut sys, group.into_iter().map(String::from).collect())
}

fn test_node_cannot_receive(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.get_node_ids();
    group.shuffle(&mut rand);
    let seed = &group.get(0).unwrap();

    for node in &group {
        sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
    }
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // node goes partially offline (cannot receive incoming messages)
    let blocked_node = group.remove(rand.gen_range(0..group.len()));
    sys.drop_incoming(&blocked_node);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_node_cannot_send(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.get_node_ids();
    group.shuffle(&mut rand);
    let seed = &group.get(0).unwrap();

    for node in &group {
        sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
    }
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // node goes partially offline (cannot send outgoing messages)
    let blocked_node = group.remove(rand.gen_range(0..group.len()));
    sys.drop_outgoing(&blocked_node);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_two_nodes_cannot_communicate(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.get_node_ids();
    group.shuffle(&mut rand);
    let seed = &group.get(0).unwrap();

    for node in &group {
        sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
    }
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // two nodes cannot communicate with each other
    let node1 = *seed;
    let mut node2 = group.get(rand.gen_range(0..group.len())).unwrap();
    while node1 == node2 {
        node2 = group.get(rand.gen_range(0..group.len())).unwrap();
    }
    sys.disable_link(&node1, &node2);
    sys.disable_link(&node2, &node1);
    // run for a while
    sys.steps(1000);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_slow_network(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.get_node_ids();
    group.shuffle(&mut rand);
    let seed = &group.get(0).unwrap();

    for node in &group {
        sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
    }
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // slow down network for a while
    sys.set_delays(0.1, 1.0);
    sys.steps(200);
    sys.set_delays(0.01, 0.1);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_flaky_network(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.get_node_ids();
    group.shuffle(&mut rand);
    let seed = &group.get(0).unwrap();

    for node in &group {
        sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
    }
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // make network unreliable for a while
    sys.set_drop_rate(0.5);
    sys.steps(1000);
    sys.set_drop_rate(0.0);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_flaky_network_on_start(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.get_node_ids();
    group.shuffle(&mut rand);
    let seed = &group.get(0).unwrap();

    // make network unreliable from the start
    sys.set_drop_rate(0.2);
    for node in &group {
        sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
    }
    sys.steps(1000);
    sys.set_drop_rate(0.0);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_flaky_network_and_crash(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.get_node_ids();
    group.shuffle(&mut rand);
    let seed = &group.get(0).unwrap();

    for node in &group {
        sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
    }
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    // make network unreliable for a while + crash node
    sys.set_drop_rate(0.5);
    let crashed_node = group.remove(rand.gen_range(0..group.len()));
    sys.crash_node(&crashed_node);
    sys.steps(1000);
    sys.set_drop_rate(0.0);
    step_until_stabilized(&mut sys, group.into_iter().collect())
}

fn test_chaos_monkey(config: &TestConfig) -> TestResult {
    let mut rand = Pcg64::seed_from_u64(config.seed);
    let mut sys = build_system(config);
    let mut group = sys.get_node_ids();
    group.shuffle(&mut rand);
    let seed = &group.get(0).unwrap();

    for node in &group {
        sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
    }
    step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

    for _ in 0..5 {
        let p = rand.gen_range(0.0..1.0);
        // do some nasty things
        match p {
            p if p < 0.25 => {
                // crash node
                let crashed_node = group.remove(rand.gen_range(0..group.len()));
                sys.crash_node(&crashed_node);
            }
            p if p < 0.5 => {
                // disconnect node
                let offline_node = group.remove(rand.gen_range(0..group.len()));
                sys.disconnect_node(&offline_node);
            }
            p if p < 0.75 => {
                // partially disconnect node (cannot receive)
                let blocked_node = group.remove(rand.gen_range(0..group.len()));
                sys.drop_incoming(&blocked_node);
            }
            _ => {
                // two nodes cannot communicate with each other
                let node1 = group.get(rand.gen_range(0..group.len())).unwrap();
                let mut node2 = group.get(rand.gen_range(0..group.len())).unwrap();
                while node1 == node2 {
                    node2 = group.get(rand.gen_range(0..group.len())).unwrap();
                }
                sys.disable_link(&node1, &node2);
                sys.disable_link(&node2, &node1);
            }
        }
        step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;
    }
    Ok(true)
}

fn test_scalability_normal(config: &TestConfig) -> TestResult {
    let sys_sizes = [
        config.node_count,
        config.node_count * 2,
        config.node_count * 5,
        config.node_count * 10,
    ];
    let mut measurements = Vec::new();
    for node_count in sys_sizes {
        let mut run_config = config.clone();
        run_config.node_count = node_count;
        let mut rand = Pcg64::seed_from_u64(config.seed);
        let mut sys = build_system(&run_config);
        let mut group = sys.get_node_ids();
        group.shuffle(&mut rand);
        let seed = &group.get(0).unwrap();
        for node in &group {
            sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
        }

        step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;
        let init_time = sys.time();
        let init_net_traffic = sys.get_network_traffic();
        let init_msg_count = sys.get_network_message_count();
        let mut init_loads = HashMap::new();
        for node in sys.get_node_ids() {
            init_loads.insert(
                node.clone(),
                sys.get_sent_message_count(&node) + sys.get_received_message_count(&node),
            );
        }

        sys.step_for_duration(10.0);

        let mut loads = Vec::new();
        for node in sys.get_node_ids() {
            let load = sys.get_sent_message_count(&node) + sys.get_received_message_count(&node);
            loads.push(load - init_loads.get(&node).unwrap());
        }
        let min_load = *loads.iter().min().unwrap();
        let max_load = *loads.iter().max().unwrap();
        let duration = sys.time() - init_time;
        measurements.push((
            duration,
            (sys.get_network_traffic() - init_net_traffic) as f64 / duration,
            (sys.get_network_message_count() - init_msg_count) as f64 / duration,
            max_load as f64 / duration,
            max_load as f64 / min_load as f64,
        ));
    }
    let mut scaling_ok = true;
    let mut load_ratio_ok = true;
    for i in 0..sys_sizes.len() {
        let (time, traffic, message_count, max_load, load_ratio) = measurements[i];
        println!("- N = {}: time - {:.2}, traffic/s - {:.2}, messages/s - {:.2}, max node messages/s - {:.2}, max/min node load - {:.2}",
                 sys_sizes[i], time, traffic, message_count, max_load, load_ratio);
        if load_ratio > 5.0 {
            load_ratio_ok = false;
        }
        if i > 0 {
            let size_ratio = sys_sizes[i] as f64 / sys_sizes[i - 1] as f64;
            let traffic_ratio = traffic / measurements[i - 1].1;
            let messages_ratio = message_count / measurements[i - 1].2;
            if traffic_ratio > 2.0 * size_ratio || messages_ratio > 2.0 * size_ratio {
                scaling_ok = false;
            }
        }
    }
    assume!(scaling_ok, "Bad network load scaling")?;
    assume!(load_ratio_ok, "Bad max/min node load")?;
    Ok(true)
}

fn test_scalability_crash(config: &TestConfig) -> TestResult {
    let sys_sizes = [
        config.node_count,
        config.node_count * 2,
        config.node_count * 5,
        config.node_count * 10,
    ];
    let mut measurements = Vec::new();
    for node_count in sys_sizes {
        let mut run_config = config.clone();
        run_config.node_count = node_count;
        let mut rand = Pcg64::seed_from_u64(config.seed);
        let mut sys = build_system(&run_config);
        let mut group = sys.get_node_ids();
        group.shuffle(&mut rand);
        let seed = &group.get(0).unwrap();
        for node in &group {
            sys.send_local(JsonMessage::from("JOIN", &JoinMessage { seed }), node);
        }

        step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;
        let init_time = sys.time();
        let init_net_traffic = sys.get_network_traffic();
        let init_msg_count = sys.get_network_message_count();
        let mut init_loads = HashMap::new();
        for node in sys.get_node_ids() {
            init_loads.insert(
                node.clone(),
                sys.get_sent_message_count(&node) + sys.get_received_message_count(&node),
            );
        }

        let crashed_node = group.remove(rand.gen_range(0..group.len()));
        sys.crash_node(&crashed_node);
        step_until_stabilized(&mut sys, group.clone().into_iter().collect())?;

        let mut loads = Vec::new();
        for node in sys.get_node_ids() {
            if node != crashed_node {
                let load =
                    sys.get_sent_message_count(&node) + sys.get_received_message_count(&node);
                loads.push(load - init_loads.get(&node).unwrap());
            }
        }
        let min_load = *loads.iter().min().unwrap();
        let max_load = *loads.iter().max().unwrap();
        let duration = sys.time() - init_time;
        measurements.push((
            duration,
            (sys.get_network_traffic() - init_net_traffic) as f64 / duration,
            (sys.get_network_message_count() - init_msg_count) as f64 / duration,
            max_load as f64 / duration,
            max_load as f64 / min_load as f64,
        ));
    }
    let mut scaling_ok = true;
    let mut load_ratio_ok = true;
    for i in 0..sys_sizes.len() {
        let (time, traffic, message_count, max_load, load_ratio) = measurements[i];
        println!("- N = {}: time - {:.2}, traffic/s - {:.2}, messages/s - {:.2}, max node messages/s - {:.2}, max/min node load - {:.2}",
                 sys_sizes[i], time, traffic, message_count, max_load, load_ratio);
        if load_ratio > 5.0 {
            load_ratio_ok = false;
        }
        if i > 0 {
            let size_ratio = sys_sizes[i] as f64 / sys_sizes[i - 1] as f64;
            let traffic_ratio = traffic / measurements[i - 1].1;
            let messages_ratio = message_count / measurements[i - 1].2;
            if traffic_ratio > 2.0 * size_ratio || messages_ratio > 2.0 * size_ratio {
                scaling_ok = false;
            }
        }
    }
    assume!(scaling_ok, "Bad network load scaling")?;
    assume!(load_ratio_ok, "Bad max/min node load")?;
    Ok(true)
}

// CLI -----------------------------------------------------------------------------------------------------------------

/// Membership Homework Tests
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
    #[clap(long, short, default_value = "10")]
    node_count: u32,

    /// Number of chaos monkey runs
    #[clap(long, short, default_value = "100")]
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
    env::set_var("PYTHONHASHSEED", args.seed.to_string());
    let node_factory = PyNodeFactory::new(&args.solution_path, "GroupMember");
    let config = TestConfig {
        node_factory: &node_factory,
        node_count: args.node_count,
        seed: args.seed,
    };
    let mut tests = TestSuite::new();

    tests.add("SIMPLE", test_simple, config.clone());
    tests.add("RANDOM SEED", test_random_seed, config.clone());
    tests.add("NODE JOIN", test_node_join, config.clone());
    tests.add("NODE LEAVE", test_node_leave, config.clone());
    tests.add("NODE CRASH", test_node_crash, config.clone());
    tests.add("SEED NODE CRASH", test_seed_node_crash, config.clone());
    tests.add(
        "NODE CRASH RECOVER",
        test_node_crash_recover,
        config.clone(),
    );
    tests.add("NODE OFFLINE", test_node_offline, config.clone());
    tests.add("SEED NODE OFFLINE", test_seed_node_offline, config.clone());
    tests.add(
        "NODE OFFLINE RECOVER",
        test_node_offline_recover,
        config.clone(),
    );
    tests.add(
        "NODE CANNOT RECEIVE",
        test_node_cannot_receive,
        config.clone(),
    );
    tests.add("NODE CANNOT SEND", test_node_cannot_send, config.clone());
    tests.add("NETWORK PARTITION", test_network_partition, config.clone());
    tests.add(
        "NETWORK PARTITION RECOVER",
        test_network_partition_recover,
        config.clone(),
    );
    tests.add(
        "TWO NODES CANNOT COMMUNICATE",
        test_two_nodes_cannot_communicate,
        config.clone(),
    );
    tests.add("SLOW NETWORK", test_slow_network, config.clone());
    tests.add("FLAKY NETWORK", test_flaky_network, config.clone());
    tests.add(
        "FLAKY NETWORK ON START",
        test_flaky_network_on_start,
        config.clone(),
    );
    tests.add(
        "FLAKY NETWORK AND CRASH",
        test_flaky_network_and_crash,
        config.clone(),
    );
    let mut rand = Pcg64::seed_from_u64(config.seed);
    for run in 1..=args.monkeys {
        let mut run_config = config.clone();
        run_config.seed = rand.next_u64();
        tests.add(
            &format!("CHAOS MONKEY (run {})", run),
            test_chaos_monkey,
            run_config,
        );
    }
    tests.add(
        "SCALABILITY NORMAL",
        test_scalability_normal,
        config.clone(),
    );
    tests.add("SCALABILITY CRASH", test_scalability_crash, config.clone());

    if args.test.is_none() {
        tests.run();
    } else {
        tests.run_test(&args.test.unwrap());
    }
}
