mod utils;

use chameleon::{ApproxOptType, ExecParams, ExecStats, Executor, Parser, DB};
use std::collections::HashSet;
use std::path::Path;
use utils::{DBVal, ResultSet};

const DB_PATH: &str = "data/test-data/cdb-test";

#[test]
fn basic_column_ref() {
    let db = DB::new(Path::new(DB_PATH));
    let mut exec = Executor::new(&db, None, None);
    let parser = Parser::new(&db);

    fn new_group() -> Vec<DBVal> {
        vec![]
    }

    fn new_rows(rows: &[(i32, i32)]) -> HashSet<Vec<DBVal>> {
        let mut records = HashSet::new();
        for row in rows {
            records.insert(vec![DBVal::Int(row.0), DBVal::Int(row.1)]);
        }
        records
    }

    let expected: ResultSet = vec![(
        new_group(),
        new_rows(&[(5, 7), (4, 4), (3, 3), (6, 5), (0, 6), (-3, 0)]),
    )]
    .into_iter()
    .collect();

    let query = "select a, c from table1";
    let query = parser.parse(query, &Default::default()).unwrap();
    let mut exec_stats = ExecStats::new();
    let result = utils::process_dbresults(exec.run(query, &Default::default(), &mut exec_stats));
    assert_eq!(expected, result);
}

#[test]
fn simple_equality() {
    let db = DB::new(Path::new(DB_PATH));
    let mut exec = Executor::new(&db, None, None);
    let parser = Parser::new(&db);

    fn new_group() -> Vec<DBVal> {
        vec![]
    }

    fn new_rows(rows: &[(i32, i32)]) -> HashSet<Vec<DBVal>> {
        let mut records = HashSet::new();
        for row in rows {
            records.insert(vec![DBVal::Int(row.0), DBVal::Int(row.1)]);
        }
        records
    }

    let expected: ResultSet = vec![(new_group(), new_rows(&[(3, 3), (0, 6)]))]
        .into_iter()
        .collect();

    let query = "select a, c from table1 where e = 1";
    let query = parser.parse(query, &Default::default()).unwrap();
    let mut exec_stats = ExecStats::new();
    let result = utils::process_dbresults(exec.run(query, &Default::default(), &mut exec_stats));
    assert_eq!(expected, result);
}

#[test]
fn simple_and() {
    let db = DB::new(Path::new(DB_PATH));
    let mut exec = Executor::new(&db, None, None);
    let parser = Parser::new(&db);

    fn new_group() -> Vec<DBVal> {
        vec![]
    }

    fn new_rows(rows: &[(i32, i32)]) -> HashSet<Vec<DBVal>> {
        let mut records = HashSet::new();
        for row in rows {
            records.insert(vec![DBVal::Int(row.0), DBVal::Int(row.1)]);
        }
        records
    }

    let expected: ResultSet = vec![(new_group(), new_rows(&[(0, 6)]))]
        .into_iter()
        .collect();

    let query = "select a, c from table1 where e = 1 and b = 'charmeleon'";
    let query = parser.parse(query, &Default::default()).unwrap();
    let mut exec_stats = ExecStats::new();
    let result = utils::process_dbresults(exec.run(query, &Default::default(), &mut exec_stats));
    assert_eq!(expected, result);
}

#[test]
fn simple_like() {
    let db = DB::new(Path::new(DB_PATH));
    let mut exec = Executor::new(&db, None, None);
    let parser = Parser::new(&db);

    fn new_group() -> Vec<DBVal> {
        vec![]
    }

    fn new_rows(rows: &[(i32, i32)]) -> HashSet<Vec<DBVal>> {
        let mut records = HashSet::new();
        for row in rows {
            records.insert(vec![DBVal::Int(row.0), DBVal::Int(row.1)]);
        }
        records
    }

    let expected: ResultSet = vec![(new_group(), new_rows(&[(5, 7), (4, 4), (3, 3)]))]
        .into_iter()
        .collect();

    let query = "select a, c from table1 where b like '%saur'";
    let query = parser.parse(query, &Default::default()).unwrap();
    let mut exec_stats = ExecStats::new();
    let result = utils::process_dbresults(exec.run(query, &Default::default(), &mut exec_stats));
    assert_eq!(expected, result);
}
