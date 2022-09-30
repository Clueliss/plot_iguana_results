#![feature(let_else, iter_intersperse)]
#![feature(slice_group_by)]

use regex::Regex;
use rio_api::{
    model::{Literal, NamedNode, Subject, Term},
    parser::TriplesParser,
};
use std::{
    fs::File,
    io::{BufReader, BufWriter, Write},
    path::Path,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::BuildHasherDefault;

type QId = u32;
type RunId = u32;

fn swdf() -> std::io::Result<()> {
    let qid_regex =
        Regex::new(r#"^http://iguana-benchmark\.eu/resource/(?P<run>[0-9]+)/[0-9]+/[0-9]/0/sparql(?P<qid>[0-9]+)$"#)
            .unwrap();

    const FILES: [(&str, &str); 4] = [
        (
            "Blazegraph",
            "/home/liss/Dokumente/Benchmarking/swdf/blazegraph/cold/results_blazegraph-swdf.nt",
        ),
        (
            "Fuseki",
            "/home/liss/Dokumente/Benchmarking/swdf/fuseki/cold/results_fuseki-swdf.nt",
        ),
        (
            "GraphDB",
            "/home/liss/Dokumente/Benchmarking/swdf/graphdb/cold/results_graphdb-swdf.nt",
        ),
        (
            "Tentris",
            "/home/liss/Dokumente/Benchmarking/swdf/tentris/cold/results_tentris-1.3.0-entry-removal-swdf.nt",
        ),
    ];

    let mut qps_bw = BufWriter::new(File::create("results_swdf-qps.csv")?);
    writeln!(qps_bw, "Triplestore,Percentage,QpS")?;

    let mut failed_bw = BufWriter::new(File::create("results_swdf-failed.csv")?);
    writeln!(failed_bw, "Triplestore,Failed")?;

    for (triplestore, path) in FILES {
        let f = BufReader::new(File::open(path)?);

        let mut failed = 0;
        let mut triples = Vec::new();

        rio_turtle::NTriplesParser::new(f).parse_all::<std::io::Error>(&mut |triple| {
            let Subject::NamedNode(NamedNode { iri }) = triple.subject else {
                panic!();
            };

            let Some(id) = qid_regex.captures(iri).and_then(|caps| {
                let qid = caps.name("qid")?.as_str().parse::<u32>().ok()?;
                let run = caps.name("run")?.as_str().parse::<u32>().ok()?;

                Some((qid, run))
            }) else {
                return Ok(());
            };

            let NamedNode { iri: predicate_iri } = triple.predicate;

            match predicate_iri {
                "http://iguana-benchmark.eu/properties/failed" => {
                    let Term::Literal(Literal::Typed { value: f, datatype: NamedNode { iri: "http://www.w3.org/2001/XMLSchema#long" } }) = triple.object else {
                        panic!();
                    };

                    failed += f.parse::<usize>().unwrap();
                }
                "http://iguana-benchmark.eu/properties/QPS" => {
                    let Term::Literal(Literal::Typed { value: qps, datatype: NamedNode { iri: "http://www.w3.org/2001/XMLSchema#double" } }) = triple.object else {
                        panic!();
                    };

                    triples.push((id, qps.parse::<f64>().unwrap()));
                }
                _ => return Ok(()),
            }

            Ok(())
        })?;

        triples.sort_unstable_by_key(|(q, _)| *q);

        writeln!(failed_bw, "{triplestore},{failed}")?;

        for (_run, chunk) in triples.chunks(30).enumerate() {
            for (p, data) in chunk.chunks(3).enumerate() {
                let avg_qps = data.iter().map(|(_, qps)| qps).sum::<f64>() / data.len() as f64;

                writeln!(qps_bw, "{triplestore},{percentage},{avg_qps}", percentage = (p + 1) * 10,)?;
            }
        }
    }

    Ok(())
}

fn dbpedia() -> std::io::Result<()> {
    let qid_regex =
        Regex::new(r#"^http://iguana-benchmark\.eu/resource/(?P<run>[0-9]+)/[0-9]+/[0-9]/0/sparql(?P<qid>[0-9]+)$"#)
            .unwrap();

    const FILES: [(&str, &str); 4] = [
        (
            "Blazegraph",
            "/home/liss/Dokumente/Benchmarking/dbpedia/results_blazegraph-dbpedia2015.nt",
        ),
        (
            "Fuseki",
            "/home/liss/Dokumente/Benchmarking/dbpedia/results_fuseki-dbpedia2015.nt",
        ),
        (
            "GraphDB",
            "/home/liss/Dokumente/Benchmarking/dbpedia/results_graphdb-dbpedia2015.nt",
        ),
        (
            "Tentris",
            "/home/liss/Dokumente/Benchmarking/dbpedia/results_tentris-1.3.0-entry-removal-dbpedia2015.nt",
        ),
    ];

    let mut failed_bw = BufWriter::new(File::create("results_dbpedia-failed.csv")?);
    writeln!(failed_bw, "Triplestore,Failed")?;

    let mut qps_bw = BufWriter::new(File::create("results_dbpedia-qps.csv")?);
    writeln!(qps_bw, "Triplestore,Query,QpS")?;

    let mut qps_all_bw = BufWriter::new(File::create("results_dbpedia-qps-all.csv")?);
    writeln!(qps_all_bw, "Triplestore,Query,QpS")?;

    let mut qps_all_no_warmup_bw = BufWriter::new(File::create("results_dbpedia-qps-all-no-warmup.csv")?);
    writeln!(qps_all_no_warmup_bw, "Triplestore,Query,QpS")?;

    let mut qps_zoom_bw = BufWriter::new(File::create("results_dbpedia-qps-warmup.csv")?);
    writeln!(qps_zoom_bw, "Triplestore,Query,QpS")?;

    for (triplestore, path) in FILES {
        let mut failed: usize = 0;
        let mut qpss: BTreeMap<QId, Vec<f64>> = Default::default();

        rio_turtle::NTriplesParser::new(BufReader::new(File::open(path)?)).parse_all::<std::io::Error>(&mut |triple| {
            let Subject::NamedNode(NamedNode { iri }) = triple.subject else {
                panic!();
            };

            let Some(qid) = qid_regex.captures(iri).and_then(|caps| {
                let qid = caps.name("qid")?.as_str().parse::<u32>().ok()?;
                let _ = caps.name("run")?;

                Some(qid)
            }) else {
                return Ok(());
            };

            let NamedNode { iri: predicate_iri } = triple.predicate;

            match predicate_iri {
                "http://iguana-benchmark.eu/properties/failed" => {
                    let Term::Literal(Literal::Typed { value: f, datatype: NamedNode { iri: "http://www.w3.org/2001/XMLSchema#long" } }) = triple.object else {
                        panic!();
                    };

                    failed += f.parse::<usize>().unwrap();
                }
                "http://iguana-benchmark.eu/properties/QPS" => {
                    let Term::Literal(Literal::Typed { value: qps, datatype: NamedNode { iri: "http://www.w3.org/2001/XMLSchema#double" } }) = triple.object else {
                        panic!();
                    };

                    let qps = qps.parse().unwrap();

                    qpss.entry(qid)
                        .or_default()
                        .push(qps);
                }
                _ => return Ok(()),
            }

            Ok(())
        })?;

        let avg_rtr_variance = qpss.values()
            .map(|run_qpss| {
                let run_avg_qps = run_qpss.iter().sum::<f64>() / run_qpss.len() as f64;
                let run_variance = run_qpss.iter()
                    .map(|qps| (qps - run_avg_qps).powi(2))
                    .sum::<f64>() / run_qpss.len() as f64;

                run_variance
            })
            .sum::<f64>() / qpss.len() as f64;

        println!("run to run variance: {avg_rtr_variance}");

        let qpss: Vec<_> = qpss.into_iter()
            .map(|(qid, qpss)| {
                let nq = qpss.len() as f64;
                let avg_qps = qpss.into_iter().sum::<f64>() / nq;

                (qid, avg_qps)
            })
            .collect();

        writeln!(failed_bw, "{triplestore},{failed}")?;

        for (qid, qps) in qpss.iter() {
            writeln!(qps_all_bw, "{triplestore},{qid},{qps}")?;
        }

        for (qid, qps) in qpss.iter().skip(200) {
            writeln!(qps_all_no_warmup_bw, "{triplestore},{qid},{qps}")?;
        }

        let chunk_sz = 400;
        let qps_chunks: Vec<_> = qpss.chunks(chunk_sz).collect();

        for chunk in &qps_chunks {
            let (qid, _) = chunk[0];
            let qps = chunk.iter().map(|(_, qps)| qps).sum::<f64>() / chunk.len() as f64;

            writeln!(qps_bw, "{triplestore},{qid},{qps}")?;
        }


        let qps_chunks_zoom = qpss.chunks(10);

        for chunk in qps_chunks_zoom.take(40) {
            let (qid, _) = chunk[0];

            let qps = chunk.iter().map(|(_, qps)| qps).sum::<f64>() / chunk.len() as f64;
            writeln!(qps_zoom_bw, "{triplestore},{qid},{qps}")?;
        }
    }

    Ok(())
}

fn main() -> std::io::Result<()> {
    swdf()?;
    dbpedia()?;

    Ok(())
}
