use chrono::NaiveDate;
use std::{iter::Sum, ops::Sub};

pub fn changeset_date_iter() -> Vec<NaiveDate> {
    walkdir::WalkDir::new("/home/liss/Dokumente/dbpedia/changesets")
        .sort_by_file_name()
        .into_iter()
        .map(Result::unwrap)
        .filter(|de| de.file_type().is_file() && matches!(de.path().extension(), Some(ext) if ext == "nt"))
        .map(|de| {
            let mut it = de.path().components().rev().take(5).skip(2);

            let day = it.next().unwrap().as_os_str().to_str().unwrap().parse().unwrap();
            let month = it.next().unwrap().as_os_str().to_str().unwrap().parse().unwrap();
            let year = it.next().unwrap().as_os_str().to_str().unwrap().parse().unwrap();

            NaiveDate::from_ymd(year, month, day)
        })
        .collect()
}

pub fn average<M, T>(measurements: M) -> f64
where
    M: Iterator<Item = T> + ExactSizeIterator,
    f64: Sum<T>,
{
    let nq = measurements.len();
    measurements.sum::<f64>() / nq as f64
}

pub fn variance<C, I, T>(average: f64, measurements: C) -> f64
where
    C: IntoIterator<IntoIter = I>,
    I: Iterator<Item = T> + ExactSizeIterator,
    T: Sub<f64, Output = f64>,
{
    let measurements = measurements.into_iter();
    let nq = measurements.len();

    measurements.map(|m| (m - average).powi(2)).sum::<f64>() / (nq - 1) as f64
}

pub fn average_variance<A, AT, V, VT>(avg: f64, measured_averages: A, measured_variances: V) -> f64
where
    A: Iterator<Item = AT> + ExactSizeIterator,
    AT: Sub<f64, Output = f64>,
    V: Iterator<Item = VT> + ExactSizeIterator,
    f64: Sum<AT>,
    f64: Sum<VT>,
{
    let part1 = average(measured_averages.map(|a| (a - avg).powi(2)));
    let part2 = average(measured_variances);

    part1 + part2
}