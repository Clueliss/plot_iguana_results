#![feature(let_else, iter_intersperse, iter_advance_by, slice_group_by)]
#![feature(array_windows)]

mod util;

use regex::Regex;
use rio_api::{
    model::{Literal, NamedNode, Subject, Term},
    parser::TriplesParser,
};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufReader, BufWriter, Write},
};
use chrono::NaiveDate;
use util::*;

type QId = u32;
type RunId = u32;

fn swdf() -> std::io::Result<()> {
    let qid_regex =
        Regex::new(r#"^http://iguana-benchmark\.eu/resource/(?P<run>[0-9]+)/1/(?P<qid>[0-9]+)/0/sparql0$"#).unwrap();

    const FILES: [(&str, &str); 5] = [
        (
            "Blazegraph",
            "/home/liss/Netzwerk/lpf-sabertooth/home/documents/Uni/bachelor/thesis/Benchmarking/swdf/blazegraph/cold/results_blazegraph-swdf.nt",
        ),
        (
            "Fuseki",
            "/home/liss/Netzwerk/lpf-sabertooth/home/documents/Uni/bachelor/thesis/Benchmarking/swdf/fuseki/cold/results_fuseki-swdf.nt",
        ),
        (
            "GraphDB",
            "/home/liss/Netzwerk/lpf-sabertooth/home/documents/Uni/bachelor/thesis/Benchmarking/swdf/graphdb/cold/results_graphdb-swdf.nt",
        ),
        (
            "Tentris",
            "/home/liss/Netzwerk/lpf-sabertooth/home/documents/Uni/bachelor/thesis/Benchmarking/swdf/tentris/cold/results_tentris-1.3.0-entry-removal-swdf.nt",
        ),
        (
            "Tentris no Bulk Removal",
            "/home/liss/Netzwerk/lpf-sabertooth/home/documents/Uni/bachelor/thesis/Benchmarking/swdf/tentris-no-bulk/cold/results_tentris-1.3.0-entry-removal-swdf.nt",
        ),
    ];

    let mut qps_bw = BufWriter::new(File::create("results_swdf-qps.csv")?);
    writeln!(qps_bw, "Triplestore,Percentage,QpS")?;

    let mut failed_bw = BufWriter::new(File::create("results_swdf-failed.csv")?);
    writeln!(failed_bw, "Triplestore,Failed")?;

    for (triplestore, path) in FILES {
        let f = BufReader::new(File::open(path)?);

        let mut failed: BTreeMap<RunId, usize> = Default::default();
        let mut qpss: BTreeMap<QId, Vec<f64>> = Default::default();

        rio_turtle::NTriplesParser::new(f).parse_all::<std::io::Error>(&mut |triple| {
            let Subject::NamedNode(NamedNode { iri }) = triple.subject else {
                panic!();
            };

            let Some((qid, run)) = qid_regex.captures(iri).and_then(|caps| {
                let qid = caps.name("qid")?.as_str().parse::<QId>().ok()?;
                let run = caps.name("run")?.as_str().parse::<RunId>().ok()?;

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

                    *failed.entry(run).or_default() += f.parse::<usize>().unwrap();
                }
                "http://iguana-benchmark.eu/properties/totalTime" => {
                    let Term::Literal(Literal::Typed { value: qps, datatype: NamedNode { iri: "http://www.w3.org/2001/XMLSchema#double" } }) = triple.object else {
                        panic!();
                    };

                    let qps = qps.parse::<f64>().unwrap();

                    qpss.entry(qid)
                        .or_default()
                        .push(qps);
                }
                _ => return Ok(()),
            }

            Ok(())
        })?;

        let avg_failed = failed.values().sum::<usize>() as f64 / failed.len() as f64;
        writeln!(failed_bw, "{triplestore},{avg_failed}")?;

        let qpss: Vec<_> = qpss
            .into_iter()
            .map(|(qid, qpss)| {
                let avg_qps = average(qpss.iter());

                (qid, avg_qps)
            })
            .collect();

        for (p, variants) in qpss.chunks(3).enumerate() {
            let avg_qps = average(variants.iter().map(|(_, qps)| qps));
            writeln!(
                qps_bw,
                "{triplestore},{percentage},{avg_qps}",
                percentage = (p + 1) * 10
            )?;
        }
    }

    Ok(())
}

fn dbpedia_fixed() -> std::io::Result<()> {
    let qid_regex =
        Regex::new(r#"^http://iguana-benchmark\.eu/resource/(?P<run>[0-9]+)/1/(?P<qid>[0-9]+)/0/sparql0$"#).unwrap();

    const FILES: [(&str, &str); 4] = [
        (
            "Blazegraph",
            "/home/liss/Netzwerk/lpf-sabertooth/home/documents/Uni/bachelor/thesis/Benchmarking/dbpedia-fixed/results_blazegraph-dbpedia2015-fixed.nt",
        ),
        (
            "Fuseki",
            "/home/liss/Netzwerk/lpf-sabertooth/home/documents/Uni/bachelor/thesis/Benchmarking/dbpedia-fixed/results_fuseki-dbpedia2015-fixed.nt",
        ),
        (
            "GraphDB",
            "/home/liss/Netzwerk/lpf-sabertooth/home/documents/Uni/bachelor/thesis/Benchmarking/dbpedia-fixed/results_graphdb-dbpedia2015-fixed.nt",
        ),
        (
            "Tentris",
            "/home/liss/Netzwerk/lpf-sabertooth/home/documents/Uni/bachelor/thesis/Benchmarking/dbpedia-fixed/results_tentris-1.3.0-entry-removal-dbpedia2015-fixed.nt",
        ),
    ];

    let mut qps_bw = BufWriter::new(File::create("results_dbpedia-fixed-qps.csv")?);
    writeln!(qps_bw, "Triplestore,Percentage,QpS")?;

    let mut failed_bw = BufWriter::new(File::create("results_dbpedia-fixed-failed.csv")?);
    writeln!(failed_bw, "Triplestore,Failed")?;

    for (triplestore, path) in FILES {
        let f = BufReader::new(File::open(path)?);

        let mut failed: BTreeMap<RunId, usize> = Default::default();
        let mut qpss: BTreeMap<QId, Vec<f64>> = Default::default();

        rio_turtle::NTriplesParser::new(f).parse_all::<std::io::Error>(&mut |triple| {
            let Subject::NamedNode(NamedNode { iri }) = triple.subject else {
                panic!();
            };

            let Some((qid, run)) = qid_regex.captures(iri).and_then(|caps| {
                let qid = caps.name("qid")?.as_str().parse::<QId>().ok()?;
                let run = caps.name("run")?.as_str().parse::<RunId>().ok()?;

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

                    *failed.entry(run).or_default() += f.parse::<usize>().unwrap();
                }
                "http://iguana-benchmark.eu/properties/totalTime" => {
                    let Term::Literal(Literal::Typed { value: qps, datatype: NamedNode { iri: "http://www.w3.org/2001/XMLSchema#double" } }) = triple.object else {
                        panic!();
                    };

                    let qps = qps.parse::<f64>().unwrap();

                    qpss.entry(qid)
                        .or_default()
                        .push(qps);
                }
                _ => return Ok(()),
            }

            Ok(())
        })?;

        let avg_failed = failed.values().sum::<usize>() as f64 / failed.len() as f64;
        writeln!(failed_bw, "{triplestore},{avg_failed}")?;

        let qpss: Vec<_> = qpss
            .into_iter()
            .map(|(qid, qpss)| {
                let avg_qps = average(qpss.iter());
                (qid, avg_qps)
            })
            .collect();

        for (p, variants) in qpss.chunks(3).enumerate() {
            let avg_qps = average(variants.iter().map(|(_, qps)| qps));
            writeln!(
                qps_bw,
                "{triplestore},{percentage},{avg_qps}",
                percentage = (p + 1) * 10
            )?;
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
            "/home/liss/Netzwerk/lpf-sabertooth/home/documents/Uni/bachelor/thesis/Benchmarking/dbpedia/results_blazegraph-dbpedia2015.nt",
        ),
        (
            "GraphDB",
            "/home/liss/Netzwerk/lpf-sabertooth/home/documents/Uni/bachelor/thesis/Benchmarking/dbpedia/results_graphdb-dbpedia2015.nt",
        ),
        (
            "Fuseki",
            "/home/liss/Netzwerk/lpf-sabertooth/home/documents/Uni/bachelor/thesis/Benchmarking/dbpedia/results_fuseki-dbpedia2015.nt",
        ),
        (
            "Tentris",
            "/home/liss/Netzwerk/lpf-sabertooth/home/documents/Uni/bachelor/thesis/Benchmarking/dbpedia/results_tentris-1.3.0-entry-removal-dbpedia2015.nt",
        ),
    ];

    let mut failed_bw = BufWriter::new(File::create("results_dbpedia-failed.csv")?);
    writeln!(failed_bw, "Triplestore,Failed")?;

    let mut qps_bw = BufWriter::new(File::create("results_dbpedia-qps.csv")?);
    writeln!(qps_bw, "Triplestore,Date,AvgRuntime,StdDeviation")?;

    let mut qps_chunked_bw = BufWriter::new(File::create("results_dbpedia-qps-chunked.csv")?);
    writeln!(qps_chunked_bw, "Triplestore,Date,AvgRuntime,StdDeviation")?;

    let mut qps_bw1 = BufWriter::new(File::create("results_dbpedia-qps1.csv")?);
    writeln!(qps_bw1, "Triplestore,Date,AvgRuntime,StdDeviation")?;

    let mut qps_bw2 = BufWriter::new(File::create("results_dbpedia-qps2.csv")?);
    writeln!(qps_bw2, "Triplestore,Date,AvgRuntime,StdDeviation")?;

    let mut qps_bw3 = BufWriter::new(File::create("results_dbpedia-qps3.csv")?);
    writeln!(qps_bw3, "Triplestore,Date,AvgRuntime,StdDeviation")?;

    let mut qps_bw4 = BufWriter::new(File::create("results_dbpedia-qps4.csv")?);
    writeln!(qps_bw4, "Triplestore,Date,AvgRuntime,StdDeviation")?;

    let mut qps_all_bw = BufWriter::new(File::create("results_dbpedia-qps-all.csv")?);
    writeln!(qps_all_bw, "Triplestore,Query,QpS")?;

    let mut qps_all_no_warmup_bw = BufWriter::new(File::create("results_dbpedia-qps-all-no-warmup.csv")?);
    writeln!(qps_all_no_warmup_bw, "Triplestore,Query,QpS")?;

    let mut qps_zoom_bw = BufWriter::new(File::create("results_dbpedia-qps-warmup.csv")?);
    writeln!(qps_zoom_bw, "Triplestore,Query,QpS")?;

    let mut chunks: Option<Vec<bool>> = None;

    for (triplestore, path) in FILES {
        let mut failed: BTreeMap<RunId, usize> = Default::default();
        let mut qpss: BTreeMap<QId, Vec<f64>> = Default::default();

        rio_turtle::NTriplesParser::new(BufReader::new(File::open(path)?)).parse_all::<std::io::Error>(&mut |triple| {
            let Subject::NamedNode(NamedNode { iri }) = triple.subject else {
                panic!();
            };

            let Some((qid, run)) = qid_regex.captures(iri).and_then(|caps| {
                let qid = caps.name("qid")?.as_str().parse::<QId>().ok()?;
                let run = caps.name("run")?.as_str().parse::<RunId>().ok()?;

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

                    *failed.entry(run).or_default() += f.parse::<usize>().unwrap();
                }
                "http://iguana-benchmark.eu/properties/totalTime" => {
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

        let avg_rtr_variance = average(qpss.values().map(|run_qpss| {
            let run_avg_qps = average(run_qpss.iter());
            variance(run_avg_qps, run_qpss)
        }));

        println!(
            "{triplestore} run to run standard deviation: {}",
            avg_rtr_variance.sqrt()
        );

        let qpss: Vec<_> = qpss
            .into_iter()
            .map(|(qid, qpss)| {
                let avg_qps = qpss.iter().sum::<f64>();
                let variance = variance(avg_qps, &qpss);
                let standard_deviation = variance.sqrt();

                (qid, avg_qps, standard_deviation)
            })
            .collect();

        let qpss_by_date: Vec<_> = {
            let tmp: Vec<_> = qpss.iter().zip(changeset_date_iter())
                .map(|(&(_, avg, stddev), date)| (date, avg, stddev))
                .collect();

            tmp.group_by(|(date1, _, _), (date2, _, _)| date1 == date2)
                .map(|group| {
                    let (date, _, _) = group[0];
                    let avg = average(group.iter().map(|(_, avg, _)| avg));
                    let stddev = average(group.iter().map(|(_, _, stddev)| stddev));

                    (date, avg, stddev)
                })
                .collect()
        };


        let avg_failed = average(failed.values().map(|&f| f as f64));
        writeln!(failed_bw, "{triplestore},{avg_failed}")?;

        for (qid, qps, _) in qpss.iter() {
            writeln!(qps_all_bw, "{triplestore},{qid},{qps}")?;
        }

        for (qid, qps, _) in qpss.iter().skip(200) {
            writeln!(qps_all_no_warmup_bw, "{triplestore},{qid},{qps}")?;
        }

        /*let chunk_sz = 400;
        let qps_chunks: Vec<_> = qpss.chunks(chunk_sz).collect();
        for chunk in &qps_chunks {
            let (qid, _) = chunk[0];
            let qps = average(chunk.iter().map(|(_, qps)| qps));

            writeln!(qps_bw, "{triplestore},{qid},{qps}")?;
        }*/

        {
            if chunks.is_none() {
                chunks = Some(
                    qpss_by_date.array_windows::<2>()
                        .map(|[(_, avg1, _), (_, avg2, _)]| {
                            (avg1 - avg2).abs() > 20.0
                        })
                        .collect()
                );
            }

            let qpss_by_date_chunked: Vec<_> = qpss_by_date.iter()
                .zip(chunks.as_ref().unwrap().iter().chain(std::iter::once(&false)))
                .collect();

            let qpss_by_date_chunked: Vec<_> = qpss_by_date_chunked
                .split_inclusive(|(_, split)| **split)
                .map(|group| {
                    let ((start_date, _, _), _) = group[0];
                    let ((end_date, _, _), _) = group[group.len() - 1];

                    let avg = average(group.iter().map(|((_, avg, _), _)| avg));

                    let part1 = average(group.iter().map(|((_, _, stddev), _)| stddev.powi(2)));
                    let part2 = average(group.iter().map(|((_, a, _), _)| (a - avg).powi(2)));

                    let stddev = (part1 + part2).sqrt();

                    (start_date, avg, stddev)
                })
                .collect();

            for (date, avg, stddev) in &qpss_by_date_chunked {
                writeln!(qps_chunked_bw, "{triplestore},{date},{avg},{stddev}")?;
            }
        }

        for (date, avg, stddev) in &qpss_by_date {
            writeln!(qps_bw, "{triplestore},{date},{avg},{stddev}")?;

            if (NaiveDate::from_ymd(2015, 10, 1)..=NaiveDate::from_ymd(2015, 10, 15)).contains(date) {
                writeln!(qps_bw1, "{triplestore},{date},{avg},{stddev}")?;
            } else if (NaiveDate::from_ymd(2015, 10, 16)..=NaiveDate::from_ymd(2015, 10, 31)).contains(date) {
                writeln!(qps_bw2, "{triplestore},{date},{avg},{stddev}")?;
            } else if (NaiveDate::from_ymd(2015, 11, 1)..=NaiveDate::from_ymd(2015, 11, 15)).contains(date) {
                writeln!(qps_bw3, "{triplestore},{date},{avg},{stddev}")?;
            } else if (NaiveDate::from_ymd(2015, 11, 16)..=NaiveDate::from_ymd(2015, 11, 30)).contains(date) {
                writeln!(qps_bw4, "{triplestore},{date},{avg},{stddev}")?;
            } else {
                panic!();
            }
        }

        {
            let qps_chunks_zoom = qpss.chunks(2);
            for chunk in qps_chunks_zoom.take(200) {
                let (qid, _, _) = chunk[0];

                let qps = average(chunk.iter().map(|(_, qps, _)| qps));
                writeln!(qps_zoom_bw, "{triplestore},{qid},{qps}")?;
            }
        }
    }

    Ok(())
}

fn main() -> std::io::Result<()> {
    //swdf()?;
    dbpedia()?;
    //dbpedia_fixed()?;

    Ok(())
}
