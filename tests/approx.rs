mod utils;
use chameleon::{query_utils, ApproxOptType, ExecParams, ExecStats, Executor, Parser, DB};
use std::collections::HashMap;
use std::path::Path;
use utils::{DBVal, ResultSet};

const DB_PATH: &str = "data/test-data/approx-test";

#[test]
fn approx_one_lookahead() {
    let db = DB::new(Path::new(DB_PATH));
    let parser = Parser::new(&db);
    let query = parser
        .parse(
            "select a from table1 where (b < 0.1) and (c < 0.1 or (d < 0.1 and e < 0.1))",
            &Default::default(),
        )
        .unwrap();

    let selectivities = query.filter.as_ref().and_then(|f| {
        let mut selectivities = HashMap::new();
        query_utils::estimate_selectivities(&f, &mut selectivities);
        Some(selectivities)
    });

    let mut exec = Executor::new(&db, selectivities, None);

    let mut approx_exec_params: ExecParams = Default::default();
    approx_exec_params.approx_opt_type = ApproxOptType::OnePredLookahead;
    let mut approx_exec_stats = ExecStats::new();
    let approx_result = utils::process_dbresults(exec.run(
        query.clone(),
        &approx_exec_params,
        &mut approx_exec_stats,
    ));

    let exec_params = Default::default();
    let mut exec_stats = ExecStats::new();
    let result = utils::process_dbresults(exec.run(query.clone(), &exec_params, &mut exec_stats));

    assert_eq!(result, approx_result);

    println!(
        "time taken: {} ms num preds evaled: {}",
        exec_stats.pred_only_time_ms, exec_stats.num_preds_evaled
    );
    println!(
        "approx time taken: {} ms num preds evaled: {}",
        approx_exec_stats.pred_only_time_ms, approx_exec_stats.num_preds_evaled
    );
}

#[test]
fn approx_better() {
    let db = DB::new(Path::new(DB_PATH));
    let parser = Parser::new(&db);
    let query = parser
        .parse(
            "select a from table1 where (a < 0.82) and (b < 0.313 or (c < 0.469 and d < 0.984))",
            &Default::default(),
        )
        .unwrap();

    let selectivities = query.filter.as_ref().and_then(|f| {
        let mut selectivities = HashMap::new();
        //query_utils::estimate_selectivities(&f, &mut selectivities);
        selectivities.insert("table1.a < 0.82".to_string(), 0.82);
        selectivities.insert("table1.b < 0.313".to_string(), 0.313);
        selectivities.insert("table1.c < 0.469".to_string(), 0.469);
        selectivities.insert("table1.d < 0.984".to_string(), 0.984);
        Some(selectivities)
    });

    let mut exec = Executor::new(&db, selectivities, None);

    let mut approx_exec_params: ExecParams = Default::default();
    let mut approx_exec_stats = ExecStats::new();
    let approx_result = utils::process_dbresults(exec.run(
        query.clone(),
        &approx_exec_params,
        &mut approx_exec_stats,
    ));

    let exec_params = Default::default();
    let mut exec_stats = ExecStats::new();
    let result = utils::process_dbresults(exec.run(query.clone(), &exec_params, &mut exec_stats));

    assert_eq!(result, approx_result);

    println!(
        "time taken: {} ms num preds evaled: {}",
        exec_stats.pred_only_time_ms, exec_stats.num_preds_evaled
    );
    println!(
        "approx time taken: {} ms num preds evaled: {}",
        approx_exec_stats.pred_only_time_ms, approx_exec_stats.num_preds_evaled
    );
}
