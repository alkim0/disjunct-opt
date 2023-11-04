mod utils;

use chameleon::{query_utils, ApproxOptType, ExecParams, ExecStats, Executor, Parser, DB};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use utils::{DBVal, ResultSet};

const DB_PATH: &str = "data/test-data/approx-test";

#[test]
fn tdacb_test() {
    let db = DB::new(Path::new(DB_PATH));
    let parser = Parser::new(&db);
    //let query = parser
    //    .parse("select a from table1 where (b < 0.1) and ((c < 0.1) or (d < 0.1))")
    //    .unwrap();
    let query = parser
        .parse(
            "select a from table1 where (a < 0.1) and (b < 0.1 or (c < 0.1 and d < 0.1))",
            &Default::default(),
        )
        .unwrap();
    //let query = parser.parse("select a from table1 where b < 0.1").unwrap();

    let selectivities = query.filter.as_ref().and_then(|f| {
        let mut selectivities = HashMap::new();
        query_utils::estimate_selectivities(&f, &mut selectivities);
        Some(selectivities)
    });

    let mut exec = Executor::new(&db, selectivities, None);

    let exec_params = Default::default();
    let mut exec_stats = ExecStats::new();

    let result = utils::process_dbresults(exec.run(query.clone(), &exec_params, &mut exec_stats));

    let mut exec_params: ExecParams = Default::default();
    exec_params.approx_opt_type = ApproxOptType::Tdacb;
    let mut exec_stats = ExecStats::new();
    let tdacb_result =
        utils::process_dbresults(exec.run(query.clone(), &exec_params, &mut exec_stats));

    assert_eq!(result, tdacb_result);

    println!(
        "Number of plans considered: {}",
        exec_stats.num_plans_considered
    );
}
