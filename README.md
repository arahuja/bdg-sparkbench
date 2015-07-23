# bdg-sparkbench
Benchmarks for Genomics on Spark

### Reference Statistics

#### Downloading a reference genome

```
wget ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/ucsc.hg19.fasta.gz
```

#### Generating reference stats

```sh
spark-submit 
  --num-executors 100 
  --executor-memory 4g 
  --class org.hammerlab.sparkbench.ReferenceStatistics 
  --properties-file spark.conf 
  target/bdg-sparkbench-with-dependencies-0.1-SNAPSHOT.jar 
    --reference ucsc.hg19.fasta
    --substringLength 30 
    --outputPath hg19-reference-stats 
    --sort
```

