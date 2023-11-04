mod utils;

use chameleon::{ApproxOptType, ExecParams, ExecStats, Executor, Parser, DB};
use std::collections::HashSet;
use std::path::Path;
use utils::{DBVal, ResultSet};

const DB_PATH: &str = "data/test-data/group-by-test";

#[test]
fn basic_group_by() {
    let db = DB::new(Path::new(DB_PATH));
    let mut exec = Executor::new(&db, None, None);
    let parser = Parser::new(&db);
    let query = parser
        .parse("select a from table1 group by b", &Default::default())
        .unwrap();
    let mut exec_stats = ExecStats::new();
    let exec_params = Default::default();
    let result = utils::process_dbresults(exec.run(query, &exec_params, &mut exec_stats));

    fn new_group(s: &str) -> Vec<DBVal> {
        vec![DBVal::Str(s.to_string())]
    }

    fn new_rows(rows: &[i32]) -> HashSet<Vec<DBVal>> {
        let mut records = HashSet::new();
        for row in rows {
            records.insert(vec![DBVal::Int(*row)]);
        }
        records
    }

    let expected: ResultSet = vec![
        (new_group("a"), new_rows(&[1, 5, 3, 8])),
        (new_group("b"), new_rows(&[2, -3, 6])),
        (new_group("c"), new_rows(&[3])),
    ]
    .into_iter()
    .collect();

    assert_eq!(expected, result);
}

#[test]
fn no_group_aggr() {
    let db = DB::new(Path::new(DB_PATH));
    let mut exec = Executor::new(&db, None, None);
    let parser = Parser::new(&db);
    fn new_group() -> Vec<DBVal> {
        vec![]
    }

    fn new_rows(rows: &[(i32, f32)]) -> HashSet<Vec<DBVal>> {
        let mut records = HashSet::new();
        for row in rows {
            records.insert(vec![DBVal::Int(row.0), DBVal::Float(row.1)]);
        }
        records
    }

    let expected: Vec<ResultSet> = vec![
        vec![(new_group(), new_rows(&[(3, 8.838)]))],
        vec![(new_group(), new_rows(&[(1, 22.533333333333335)]))],
        vec![(new_group(), new_rows(&[(-3, -1.5)]))],
        vec![(new_group(), new_rows(&[(8, 42.2)]))],
        vec![(new_group(), new_rows(&[(25, 70.704)]))],
    ]
    .into_iter()
    .map(|x| x.into_iter().collect())
    .collect();

    let queries = [
        "select avg(a), avg(c) from table1",
        "select avg(a), avg(c) from table1 where b = 'b'",
        "select min(a), min(c) from table1",
        "select max(a), max(c) from table1",
        "select sum(a), sum(c) from table1",
    ];
    for (i, query) in queries.iter().enumerate() {
        let query = parser.parse(query, &Default::default()).unwrap();
        let exec_params = Default::default();
        let mut exec_stats = ExecStats::new();
        let result = utils::process_dbresults(exec.run(query, &exec_params, &mut exec_stats));
        assert_eq!(expected[i], result);
    }
}

#[test]
fn simple_group() {
    let db = DB::new(Path::new(DB_PATH));
    let mut exec = Executor::new(&db, None, None);
    let parser = Parser::new(&db);
    fn new_group(s: &str) -> Vec<DBVal> {
        vec![DBVal::Str(s.to_string())]
    }

    fn new_rows(rows: &[(i32, f32)]) -> HashSet<Vec<DBVal>> {
        let mut records = HashSet::new();
        for row in rows {
            records.insert(vec![DBVal::Int(row.0), DBVal::Float(row.1)]);
        }
        records
    }

    let expected: Vec<ResultSet> = vec![vec![
        (
            new_group("a"),
            new_rows(&[(1, 1.5), (5, 0.0), (3, 0.234), (8, 2.87)]),
        ),
        (new_group("b"), new_rows(&[(2, 23.5), (-3, 42.2), (6, 1.9)])),
        (new_group("c"), new_rows(&[(3, -1.5)])),
    ]]
    .into_iter()
    .map(|x| x.into_iter().collect())
    .collect();

    let queries = ["select a, c from table1 group by b"];
    for (i, query) in queries.iter().enumerate() {
        let query = parser.parse(query, &Default::default()).unwrap();
        let exec_params = Default::default();
        let mut exec_stats = ExecStats::new();
        let result = utils::process_dbresults(exec.run(query, &exec_params, &mut exec_stats));
        assert_eq!(expected[i], result);
    }
}

#[test]
fn group_aggr() {
    let db = DB::new(Path::new(DB_PATH));
    let mut exec = Executor::new(&db, None, None);
    let parser = Parser::new(&db);

    fn new_group(s: &str) -> Vec<DBVal> {
        vec![DBVal::Str(s.to_string())]
    }

    fn new_rows(rows: &[(i32, f32)]) -> HashSet<Vec<DBVal>> {
        let mut records = HashSet::new();
        for row in rows {
            records.insert(vec![DBVal::Int(row.0), DBVal::Float(row.1)]);
        }
        records
    }

    let expected: Vec<ResultSet> = vec![
        vec![
            (new_group("a"), new_rows(&[(4, 1.151)])),
            (new_group("b"), new_rows(&[(1, 22.53333333333333)])),
            (new_group("c"), new_rows(&[(3, -1.5)])),
        ],
        vec![(new_group("b"), new_rows(&[(1, 22.53333333333333)]))],
        vec![
            (new_group("a"), new_rows(&[(5, 1.0346666666666666)])),
            (new_group("b"), new_rows(&[(6, 1.9)])),
            (new_group("c"), new_rows(&[(3, -1.5)])),
        ],
        vec![
            (new_group("a"), new_rows(&[(1, 0.0)])),
            (new_group("b"), new_rows(&[(-3, 1.9)])),
            (new_group("c"), new_rows(&[(3, -1.5)])),
        ],
        vec![
            (new_group("a"), new_rows(&[(8, 2.87)])),
            (new_group("b"), new_rows(&[(6, 42.2)])),
            (new_group("c"), new_rows(&[(3, -1.5)])),
        ],
        vec![
            (new_group("a"), new_rows(&[(17, 4.604)])),
            (new_group("b"), new_rows(&[(5, 67.6)])),
            (new_group("c"), new_rows(&[(3, -1.5)])),
        ],
    ]
    .into_iter()
    .map(|x| x.into_iter().collect())
    .collect();

    let queries = [
        "select avg(a), avg(c) from table1 group by b",
        "select avg(a), avg(c) from table1 where b = 'b' group by b",
        "select avg(a), avg(c) from table1 where a > 2 group by b",
        "select min(a), min(c) from table1 group by b",
        "select max(a), max(c) from table1 group by b",
        "select sum(a), sum(c) from table1 group by b",
    ];
    for (i, query) in queries.iter().enumerate() {
        let query = parser.parse(query, &Default::default()).unwrap();
        let exec_params = Default::default();
        let mut exec_stats = ExecStats::new();
        let result = utils::process_dbresults(exec.run(query, &exec_params, &mut exec_stats));
        assert_eq!(expected[i], result);
    }
}

#[test]
fn multi_groups() {
    let db = DB::new(Path::new(DB_PATH));
    let mut exec = Executor::new(&db, None, None);
    let parser = Parser::new(&db);

    fn new_group(a: &str, b: &str) -> Vec<DBVal> {
        vec![DBVal::Str(a.to_string()), DBVal::Str(b.to_string())]
    }

    fn new_rows(rows: &[(i32, f32)]) -> HashSet<Vec<DBVal>> {
        let mut records = HashSet::new();
        for row in rows {
            records.insert(vec![DBVal::Int(row.0), DBVal::Float(row.1)]);
        }
        records
    }

    let expected: ResultSet = vec![
        (
            new_group("a", "a"),
            new_rows(&[(1, 1.5), (5, 0.0), (8, 2.87)]),
        ),
        (new_group("b", "a"), new_rows(&[(2, 23.5), (-3, 42.2)])),
        (new_group("c", "b"), new_rows(&[(3, -1.5)])),
        (new_group("a", "b"), new_rows(&[(3, 0.234)])),
        (new_group("b", "d"), new_rows(&[(6, 1.9)])),
    ]
    .into_iter()
    .collect();

    let query = "select a, c from table1 group by b, d";
    let query = parser.parse(query, &Default::default()).unwrap();
    let exec_params = Default::default();
    let mut exec_stats = ExecStats::new();
    let result = utils::process_dbresults(exec.run(query, &exec_params, &mut exec_stats));
    assert_eq!(expected, result);
}
