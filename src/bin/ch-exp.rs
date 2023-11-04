use chameleon::{bin_utils, ApproxOptType, ExecParams, ExecStats, Executor, Parser, Table, DB};
use clap::Parser as ClapParser;
use gethostname::gethostname;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

#[derive(ClapParser)]
struct Args {
    #[arg(short = 't', long, default_value_t = 3)]
    num_trials: usize,

    #[arg(long)]
    output_prefix: Option<String>,

    #[arg(short, long)]
    output: Option<PathBuf>,

    #[arg(long)]
    db_path: Option<PathBuf>,

    #[arg(long)]
    no_output: bool,

    #[arg(long)]
    stats_dir: Option<PathBuf>,

    #[arg(long)]
    debug: bool,

    #[arg(long, value_parser = bin_utils::parse_comma_string_list)]
    planner_type: Option<std::vec::Vec<String>>,
}

#[derive(Debug, Serialize)]
struct Record {
    planner_type: String,
    trial: usize,
    plan_time_ms: u128,
    exec_time_ms: u128,
    pred_eval_time_ms: u128,
    num_pred_eval: u128,
}

const QUERY: &str = "
select	sum(ol_amount) as revenue
from	orderline join item on orderline.ol_i_id = item.i_id
where	ol_quantity >= 1
        and ol_quantity <= 10
        and i_price between 1 and 400000
        and (
            (
              i_data like '%a'
              and ol_w_id in (1,2,3)
            ) or (
              i_data like '%b'
              and ol_w_id in (1,2,4)
            ) or (
              i_data like '%c'
              and ol_w_id in (1,5,3)
            )
        )
        ";

//const QUERY: &str = "
//select	sum(ol_amount) as revenue
//from	orderline join item on orderline.ol_i_id = item.i_id
//where	(
//          i_data like '%a'
//          and ol_quantity >= 1
//          and ol_quantity <= 10
//          and i_price between 1 and 400000
//          and ol_w_id in (1,2,3)
//        ) or (
//          i_data like '%b'
//          and ol_quantity >= 1
//          and ol_quantity <= 10
//          and i_price between 1 and 400000
//          and ol_w_id in (1,2,4)
//        ) or (
//          i_data like '%c'
//          and ol_quantity >= 1
//          and ol_quantity <= 10
//          and i_price between 1 and 400000
//          and ol_w_id in (1,5,3)
//        )
//        ";

fn main() {
    let args = Args::parse().with_defaults();

    let db = DB::new(args.db_path.as_ref().unwrap());
    let parser = Parser::new(&db);
    let (selectivities, costs) = get_stats(&args, &parser);
    let mut exec = Executor::new(&db, Some(selectivities), Some(costs));

    let mut records = vec![];
    let query = parser.parse(QUERY, &Default::default()).unwrap();
    let mut exec_stats = ExecStats::new();
    query
        .table
        .table
        .eval_join(&ExecParams::default(), &mut exec_stats, None);

    for trial in 0..args.num_trials {
        let mut outputs = vec![];
        for planner_type in args.planner_type.as_ref().unwrap() {
            println!("Running trial {} planner type {:?}", trial, planner_type);
            bin_utils::drop_caches();

            let exec_params = build_exec_params(&planner_type);
            let mut exec_stats = ExecStats::new();
            let result = exec.run_without_eval_join(query.clone(), &exec_params, &mut exec_stats);
            outputs.push(result);

            let record = Record {
                trial,
                planner_type: planner_type.to_string(),
                plan_time_ms: exec_stats.plan_time_ms,
                exec_time_ms: exec_stats.total_time_ms,
                pred_eval_time_ms: exec_stats.pred_only_time_ms,
                num_pred_eval: exec_stats.num_preds_evaled,
            };
            println!("{:?}", record);

            records.push(record);
        }

        if outputs.len() >= 2 {
            for output in &outputs {
                assert_eq!(&outputs[0], output);
            }
        }
    }

    bin_utils::write_records(args.output.as_ref().unwrap(), records).unwrap();
}

fn build_exec_params(planner_type: &str) -> ExecParams {
    let mut exec_params = ExecParams::default();
    match planner_type {
        "eval_pred" => {}
        "tdacb" => exec_params.approx_opt_type = ApproxOptType::Tdacb,
        "no_opt" => exec_params.disable_or_opt = true,
        "bdc" => exec_params.approx_opt_type = ApproxOptType::BDCWithBestD,
        "greedy_d3" => exec_params.approx_opt_type = ApproxOptType::OnePredLookahead,
        _ => panic!("Unknown planner type {}", planner_type),
    }
    exec_params
}

#[derive(Serialize, Deserialize)]
struct PredAtomStat {
    selectivity: f64,
    cost: f64,
}

fn get_stats(args: &Args, parser: &Parser) -> (HashMap<String, f64>, HashMap<String, f64>) {
    let mut selectivities = HashMap::new();
    let mut costs = HashMap::new();

    let stats_dir = args.stats_dir.as_ref().unwrap();
    if !stats_dir.exists() {
        fs::create_dir_all(&stats_dir).unwrap();
    }

    let query = parser.parse(QUERY, &Default::default()).unwrap();
    let pred_atoms = query
        .filter
        .as_ref()
        .map(|filter| filter.get_all_atoms())
        .unwrap_or(vec![]);

    let mut table_evaled = false;
    for pred_atom in pred_atoms {
        let stat_path = stats_dir.join(pred_atom.expr.to_string());

        let stat: PredAtomStat = if stat_path.exists() {
            serde_json::from_str(&fs::read_to_string(stat_path).unwrap()).unwrap()
        } else {
            if !table_evaled {
                let mut exec_stats = ExecStats::new();
                query
                    .table
                    .table
                    .eval_join(&ExecParams::default(), &mut exec_stats, None);
                table_evaled = true;
            }

            let table_len = query.table.table.len();
            println!("total table {} len: {}", query.table.table, table_len);
            let mut exec_stats = ExecStats::new();
            let result = pred_atom.eval(
                &RoaringBitmap::from_sorted_iter(0..(table_len as u32)).unwrap(),
                &ExecParams::default(),
                &mut exec_stats,
            );

            let selectivity = result.len() as f64 / table_len as f64;
            let stat = PredAtomStat {
                selectivity,
                cost: 1.,
            };

            fs::write(&stat_path, serde_json::to_string(&stat).unwrap()).unwrap();
            stat
        };

        selectivities.insert(pred_atom.expr.to_string(), stat.selectivity);
        costs.insert(pred_atom.expr.to_string(), stat.cost);
    }

    (selectivities, costs)
}

impl Args {
    fn with_defaults(mut self) -> Self {
        self.output
            .get_or_insert(bin_utils::default_output_dir().join(format!(
                    "{}-{}-{}.csv",
                    self.output_prefix
                        .as_ref()
                        .map(|s| s.as_str())
                        .unwrap_or("ch-exp"),
                    gethostname().to_string_lossy(),
                    chrono::Local::now().format("%FT%H%M%S%z")
                )));

        self.db_path.get_or_insert(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("data")
                .join("ch-benchmark"),
        );

        self.stats_dir.get_or_insert(bin_utils::default_stats_dir());

        self.planner_type.get_or_insert(vec![
            "eval_pred".into(),
            "no_opt".into(),
            "tdacb".into(),
            "bdc".into(),
            "greedy_d3".into(),
        ]);

        self
    }
}
