use approx::Ulps;
use chameleon::{DBCol, DBResult};
use chrono::{DateTime, Duration, Utc};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Debug, PartialOrd, Clone)]
pub enum X {
    I(i32),
    F(f64),
    S(String),
    s(&'static str),
    B(bool),
}

impl PartialEq for X {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (X::I(a), X::I(b)) => a == b,
            (X::F(a), X::F(b)) => Ulps::default().max_ulps(4).eq(a, b),
            (X::S(a), X::S(b)) => a == b,
            (X::S(a), X::s(b)) => *a == b.to_string(),
            (X::s(a), X::S(b)) => a.to_string() == *b,
            (X::s(a), X::s(b)) => a == b,
            (X::B(a), X::B(b)) => a == b,
            _ => {
                panic!("Incomparable {:?} {:?}", self, other);
            }
        }
    }
}

impl Ord for X {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other)
            .expect(&format!("Incomparable {:?} {:?}", self, other))
    }
}

impl Eq for X {}

#[derive(Debug, PartialOrd, Eq)]
pub struct DebugVals<T> {
    elems: Vec<T>,
    subset_ok: bool,
}

impl<T: PartialEq> PartialEq for DebugVals<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self.subset_ok, other.subset_ok) {
            (false, false) => self.elems == other.elems,
            (true, false) => self.elems.iter().all(|elem| other.elems.contains(elem)),
            (false, true) => other.elems.iter().all(|elem| self.elems.contains(elem)),
            _ => {
                panic!("You messed up");
            }
        }
    }
}

impl<T: PartialOrd> DebugVals<T> {
    // subset_ok says whether it is ok for this DebugVals to be a subset of the other it is
    // compared to.
    pub fn new(mut elems: Vec<T>, subset_ok: bool) -> Self {
        elems.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
        Self { elems, subset_ok }
    }
}

#[derive(Debug, PartialOrd, PartialEq, Clone)]
pub enum DBVal {
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Str(String),
    Bool(bool),
    DateTime(DateTime<Utc>),
    Duration(Duration),
}

impl Eq for DBVal {}

impl Hash for DBVal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            DBVal::Float(f) => {
                f.to_bits().hash(state);
            }
            DBVal::Double(d) => {
                d.to_bits().hash(state);
            }
            DBVal::Int(i) => {
                i.hash(state);
            }
            DBVal::Long(l) => {
                l.hash(state);
            }
            DBVal::Bool(b) => {
                b.hash(state);
            }
            DBVal::Str(s) => {
                s.hash(state);
            }
            DBVal::DateTime(d) => {
                d.hash(state);
            }
            DBVal::Duration(d) => {
                d.hash(state);
            }
        }
    }
}

pub type ResultSet = HashMap<Vec<DBVal>, HashSet<Vec<DBVal>>>;

pub fn print_result_set(rs: &ResultSet, tag: &str) {
    println!("{}:", tag);
    let mut keys: Vec<&Vec<DBVal>> = rs.keys().collect();
    keys.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
    for key in keys {
        println!("    {:?}: {{", key);
        let mut vals: Vec<&Vec<DBVal>> = rs[key].iter().collect();
        vals.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
        for val in vals {
            println!("        {:?}", val);
        }
        println!("    }}");
    }
}

// Returns as a hash map of rows
pub fn process_dbresults(results: Vec<DBResult>) -> ResultSet {
    assert!(!results.is_empty());

    let mut parsed_results: HashMap<Vec<DBVal>, HashSet<Vec<DBVal>>> = HashMap::new();
    let groups = results[0].cols.keys();
    //for (i, result) in results.iter().enumerate() {
    //    eprintln!("{} {:?}", i, result);
    //}
    for group in groups {
        let mut rows = HashSet::new();
        let col = results[0].cols.get(group).unwrap();
        for i in 0..col.len() {
            let mut row = vec![];
            for result in &results {
                let col = result.cols.get(group).expect("Col didn't have group");
                row.push(match col {
                    DBCol::Int(vals) => DBVal::Int(vals[i]),
                    DBCol::Long(vals) => DBVal::Long(vals[i]),
                    DBCol::Float(vals) => DBVal::Float(vals[i]),
                    DBCol::Double(vals) => DBVal::Double(vals[i]),
                    DBCol::Str(vals) => DBVal::Str(vals[i].clone()),
                    DBCol::Bool(vals) => DBVal::Bool(vals[i]),
                    DBCol::DateTime(vals) => DBVal::DateTime(vals[i]),
                    DBCol::Duration(vals) => DBVal::Duration(vals[i]),
                });
            }
            rows.insert(row);
        }
        let group = group
            .iter()
            .map(|col| match col {
                DBCol::Int(vals) => DBVal::Int(vals[0]),
                DBCol::Long(vals) => DBVal::Long(vals[0]),
                DBCol::Float(vals) => DBVal::Float(vals[0]),
                DBCol::Double(vals) => DBVal::Double(vals[0]),
                DBCol::Str(vals) => DBVal::Str(vals[0].clone()),
                DBCol::Bool(vals) => DBVal::Bool(vals[0]),
                DBCol::DateTime(vals) => DBVal::DateTime(vals[0]),
                DBCol::Duration(vals) => DBVal::Duration(vals[0]),
            })
            .collect();
        parsed_results.insert(group, rows);
    }
    parsed_results
}

//// A struct that allows floats to be the set and allows comparison between floats
//#[derive(Debug)]
//pub struct FloatSet<T> {
//    elems: Vec<T>,
//}
//
//impl<T: PartialOrd + Clone> FloatSet<T> {
//    pub fn new(elems: &Vec<T>) -> Self {
//        let mut elems = elems.iter().cloned().collect::<Vec<T>>();
//        elems.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
//        Self { elems }
//    }
//}
//
//impl<T: PartialOrd + fmt::Debug> PartialEq for FloatSet<T> {
//    fn eq(&self, other: &Self) -> bool {
//        if self.elems.len() != other.elems.len() {
//            false
//        } else {
//            self.elems.iter().zip(other.elems.iter()).all(|(a, b)| {
//                let c = a.partial_cmp(b).unwrap();
//                println!("{:?} {:?} {:?}", a, b, c);
//                if a.partial_cmp(b).unwrap() == Ordering::Equal {
//                    true
//                } else {
//                    ulps_eq!(a, b)
//                }
//            })
//        }
//    }
//}
//
//impl<T: PartialOrd + fmt::Debug> Eq for FloatSet<T> {}
//
//
//
//#[derive]
//pub struct
