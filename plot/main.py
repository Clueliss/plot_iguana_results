import datetime
from typing import List

from plotnine import *
import pandas as pd
import numpy as np

def swdf_avg_qps():
    path = "/home/liss/Development/plot_iguana_results/parse/results_swdf-qps.csv"
    dataset = pd.read_csv(path)

    plot = ggplot(dataset) \
          + aes(x="reorder(Percentage, QpS)", y="QpS", fill="Triplestore") \
          + geom_col(stat=stat_identity, position=position_dodge) \
          + scale_x_discrete(labels=lambda lst: [f"{x}%" for x in lst]) \
          + scale_y_log10() \
          + xlab("Percentage of Dataset Removed") \
          + ylab("Average Runtime (ms)")

    plot.save("swdf_avg_qps.pdf")


def swdf_failed():
    path = "/home/liss/Development/plot_iguana_results/parse/results_swdf-failed.csv"
    dataset = pd.read_csv(path)

    plot = ggplot(dataset) \
        + aes(x="Triplestore", y="Failed", fill="Triplestore") \
        + geom_bar(stat="identity", show_legend=False) \
        + ylab("No. Failed Q") \
        + ylim(0, 20)

    plot.save("swdf_failed.pdf")


def dbpedia_fixed_avg_qps():
    path = "/home/liss/Development/plot_iguana_results/parse/results_dbpedia-fixed-qps.csv"
    dataset = pd.read_csv(path)

    plot = ggplot(dataset) \
          + aes(x="reorder(Percentage, list(reversed(QpS)))", y="QpS", fill="Triplestore") \
          + geom_col(stat="identity", position="dodge") \
          + xlab("Percentage of SWDF Removed") \
          + ylab("Average Runtime (ms)")

    plot.save("dbpedia-fixed_avg_qps.pdf")


def dbpedia_fixed_failed():
    path = "/home/liss/Development/plot_iguana_results/parse/results_dbpedia-fixed-failed.csv"
    dataset = pd.read_csv(path)

    plot = ggplot(dataset) \
        + aes(x="Triplestore", y="Failed", fill="Triplestore") \
        + geom_bar(stat="identity", show_legend=False) \
        + ylab("No. Failed Q") \
        + ylim(0, 20)

    plot.save("dbpedia-fixed_failed.pdf")


def dbpedia_failed():
    path = "/home/liss/Development/plot_iguana_results/parse/results_dbpedia-failed.csv"
    dataset = pd.read_csv(path)

    plot = ggplot(dataset) \
        + aes(x="Triplestore", y="Failed", fill="Triplestore") \
        + geom_bar(stat="identity", show_legend=False) \
        + ylab("No. Failed Q") \
        + ylim(0, 20)

    plot.save("dbpedia_failed.pdf")


def dbpedia_avg_qps():
    path = "/home/liss/Development/plot_iguana_results/parse/results_dbpedia-qps-all.csv"
    dataset = pd.read_csv(path)

    plot = ggplot(dataset) \
           + aes(x="Triplestore", y="QpS", color="Triplestore", fill="Triplestore") \
           + geom_jitter(alpha=0.1, size=0.5, show_legend=False) \
           + geom_boxplot(alpha=0, width=0.9, color="black", outlier_size=0.1, show_legend=False) \
           + stat_summary(fun_y=np.mean, geom='text', label="x", color="black") \
           + scale_y_log10() \
           + xlab("Triplestore") \
           + ylab("Runtime (ms)")

    plot.save("dbpedia_avg_qps.pdf")


def dbpedia_avg_qps_no_warmup():
    path = "/home/liss/Development/plot_iguana_results/parse/results_dbpedia-qps-all-no-warmup.csv"
    dataset = pd.read_csv(path)

    plot = ggplot(dataset) \
           + aes(x="Triplestore", y="QpS", color="Triplestore", fill="Triplestore") \
           + geom_jitter(alpha=0.1, size=0.5, show_legend=False) \
           + geom_boxplot(alpha=0, width=0.9, color="black", outlier_size=0.1, show_legend=False) \
           + stat_summary(fun_y=np.mean, geom='text', label="x", color="black") \
           + scale_y_log10() \
           + xlab("Triplestore") \
           + ylab("Runtime (ms)")

    plot.save("dbpedia_avg_qps_no_warmup.pdf")


def dbpedia_qps():
    def alt1():
        for part in range(1, 5):
            path = f"/home/liss/Development/plot_iguana_results/parse/results_dbpedia-qps{part}.csv"
            dataset = pd.read_csv(path)

            plot = ggplot(dataset) \
                   + aes(x="Date", y="AvgRuntime", color="Triplestore", fill="Triplestore", group="Triplestore") \
                   + geom_col(position=position_dodge, width=0.80) \
                   + scale_x_date(date_breaks="2 days", date_labels="%b %d") \
                   + xlab("Changeset Date") \
                   + ylab("Runtime (ms)")

            plot.save(f"dbpedia_qps{part}.pdf", width=10, height=4.8)

    def alt2():
        path = f"/home/liss/Development/plot_iguana_results/parse/results_dbpedia-qps.csv"
        dataset = pd.read_csv(path)

        plot = ggplot(dataset) \
               + aes(x="Date", y="AvgRuntime", color="Triplestore", fill="Triplestore", group="Triplestore") \
               + geom_col() \
               + scale_x_date(date_breaks="10 days", date_labels="%b %d") \
               + xlab("Changeset Date") \
               + ylab("Runtime (ms)")

        plot.save(f"dbpedia_qps.pdf")

    def alt3():
        for part in range(1, 5):
            path = f"/home/liss/Development/plot_iguana_results/parse/results_dbpedia-qps{part}.csv"
            dataset = pd.read_csv(path)

            plot = ggplot(dataset) \
                   + aes(x="Date", y="AvgRuntime", ymin="AvgRuntime-StdDeviation", ymax="AvgRuntime+StdDeviation", color="Triplestore", fill="Triplestore", group="Triplestore") \
                   + geom_col() \
                   + scale_x_date(date_breaks="2 days", date_labels="%b %d") \
                   + xlab("Changeset Date") \
                   + ylab("Runtime (ms)")

            plot.save(f"dbpedia_qps_alt{part}.pdf")

    def alt4():
        def label(date1: datetime.date, date2: datetime.date):
            if (date2 - date1).days == 1:
                return date1.strftime("%b %d")
            else:
                return "{} - {}".format(date1.strftime("%b %d"), (date2 - datetime.timedelta(days=1)).strftime("%d"))

        def label_func(dates):
            dates = list(dates)
            return [label(datetime.date.fromisoformat(date1), datetime.date.fromisoformat(date2)) for date1, date2 in
                    zip(dates, dates[1:] + ["2015-12-01"])]

        path = f"/home/liss/Development/plot_iguana_results/parse/results_dbpedia-qps-chunked.csv"
        dataset = pd.read_csv(path)

        plot = ggplot(dataset) \
               + aes(x="Date", y="AvgRuntime", ymin="AvgRuntime-StdDeviation", ymax="AvgRuntime+StdDeviation", color="Triplestore", fill="Triplestore", group="Triplestore") \
               + geom_col(position=position_dodge, width=0.8) \
               + geom_errorbar(position=position_dodge(width=0.8), width=0.4, size=0.2, color="black") \
               + xlab("Changeset Date") \
               + scale_x_discrete(labels=label_func) \
               + ylab("Runtime (ms)")

        plot.save(f"dbpedia_qps_alt_chunked.pdf", width=11, height=4.8)

    alt1()
    alt2()
    alt3()
    alt4()


def dbpedia_warmup_qps():
    path = "/home/liss/Development/plot_iguana_results/parse/results_dbpedia-qps-warmup.csv"
    dataset = pd.read_csv(path)

    plot = ggplot(dataset) \
           + aes(x="Query", y="QpS", color="Triplestore", fill="Triplestore", group="Triplestore") \
           + geom_line() \
           + scale_y_log10() \
           + xlab("Query No.") \
           + ylab("Runtime (ms)")

    # + geom_vline(xintercept=600) \
    # + annotate("text", x=900, y=0.25, label="Triplestores Warm", size=10) \

    plot.save("dbpedia_qps_warmup.pdf", width=8, height=4.8)


swdf_avg_qps()
swdf_failed()

dbpedia_failed()
dbpedia_qps()
dbpedia_avg_qps()
dbpedia_avg_qps_no_warmup()
dbpedia_warmup_qps()

dbpedia_fixed_avg_qps()
dbpedia_fixed_failed()
