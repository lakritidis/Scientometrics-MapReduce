# Scientometrics-MapReduce
Computing scientometrics in large-scale academic search engines with MapReduce

This repository contains the implementations of four MapReduce algorithms for the computation of three scientometrics in massive datasets. Scientometrics are measures that have been introduced in the scientific literature for evaluating the research work of a scientist. The most popupar among them is the famous h-index metric.

Back in 2012, this code was originally developed by using Hadoop 0.20.2; however, it has been recently refactored and tested on a Hadoop 2.8.5 installation. It is a supplemental material for the article:

L. Akritidis, P. Bozanis, "Computing Scientometrics in Large-Scale Academic Search Engines with MapReduce", In Proceedings of the 13th International Conference on Web Information System Engineering (WISE), Lecture Notes in Computer Science (LLNCS), vol. 7651, pp. 609-623, 2012.

For the experimental requirements of this work we used the CiteSeerX dataset, an open repository comprised of approximately 1.8 million research articles. The most recent version of can be acquired from [here](https://csxstatic.ist.psu.edu/downloads/data). Nevertheless, since the individual processing of 1.8 million XML files is very inefficient in HDFS, we packed thousands of such XML files into line-sequential files of larger sizes. For this particular dataset, we packed the 1.8 million small-sized XML files into 432 of 64 MB each. A tiny subset of this dataset is included here.

More details about the algorithms and the code can be found on the attached paper and my [personal homepage](http://users.sch.gr/lakritid/code.php?c=1).

Note: The researchers who used/will use this code are kindly requested to cite the aforementioned article.
