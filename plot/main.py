from plotnine import *
import pandas as pd
import numpy as np

def swdf_avg_qps():
    path = "/home/liss/Development/plot_iguana_results/parse/results_swdf-qps.csv"
    dataset = pd.read_csv(path)

    plot = ggplot(dataset) \
          + aes(x="reorder(Percentage, list(reversed(QpS)))", y="QpS", fill="Triplestore") \
          + geom_col(stat="identity", position="dodge") \
          + xlab("Percentage of Dataset Removed") \
          + ylab("Average QpS") \
          + ggtitle("SWDF Average QpS")

    plot.save("swdf_avg_qps.png")


def swdf_failed():
    path = "/home/liss/Development/plot_iguana_results/parse/results_swdf-failed.csv"
    dataset = pd.read_csv(path)

    plot = ggplot(dataset) \
        + aes(x="Triplestore", y="Failed", fill="Triplestore") \
        + geom_bar(stat="identity", show_legend=False) \
        + ylab("No. Failed Q") \
        + ylim(0, 40) \
        + ggtitle("SWDF Failed Queries")

    plot.save("swdf_failed.png")


def dbpedia_failed():
    path = "/home/liss/Development/plot_iguana_results/parse/results_dbpedia-failed.csv"
    dataset = pd.read_csv(path)

    plot = ggplot(dataset) \
        + aes(x="Triplestore", y="Failed", fill="Triplestore") \
        + geom_bar(stat="identity", show_legend=False) \
        + ylab("No. Failed Q") \
        + ylim(0, 40) \
        + ggtitle("DBpedia Failed Queries")

    plot.save("dbpedia_failed.png")


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
           + ylab("QpS") \
           + ggtitle("DBpedia Average QpS")

    plot.save("dbpedia_avg_qps.png")


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
           + ylab("QpS") \
           + ggtitle("DBpedia Average QpS")

    plot.save("dbpedia_avg_qps_no_warmup.png")


def dbpedia_qps():
    path = "/home/liss/Development/plot_iguana_results/parse/results_dbpedia-qps.csv"
    dataset = pd.read_csv(path)

    plot = ggplot(dataset) \
           + aes(x="Query", y="QpS", color="Triplestore", group="Triplestore") \
           + geom_line() \
           + scale_y_log10() \
           + xlab("Query No.") \
           + ylab("QpS") \
           + ggtitle("DBpedia QpS")

    plot.save("dbpedia_qps.png")


def dbpedia_warmup_qps():
    path = "/home/liss/Development/plot_iguana_results/parse/results_dbpedia-qps-warmup.csv"
    dataset = pd.read_csv(path)

    plot = ggplot(dataset) \
           + aes(x="Query", y="QpS", color="Triplestore", group="Triplestore") \
           + geom_line() \
           + scale_y_log10() \
           + xlab("Query No.") \
           + ylab("QpS") \
           + ggtitle("Warmup Behaviour in DBpedia")

    # + geom_vline(xintercept=600) \
    # + annotate("text", x=900, y=0.25, label="Triplestores Warm", size=10) \

    plot.save("dbpedia_qps_warmup.png")


swdf_avg_qps()
swdf_failed()

dbpedia_failed()
dbpedia_qps()
dbpedia_avg_qps()
dbpedia_avg_qps_no_warmup()
dbpedia_warmup_qps()
