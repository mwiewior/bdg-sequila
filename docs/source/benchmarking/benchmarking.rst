
Benchmarking
=============

Interval joins
##############

Performance tests description
*****************************
In order to evaluate our range join strategy we have run a number of tests using both one-node and a Hadoop cluster
installations. In that way we were able to analyze both vertical (by means of adding computing resources such as CPU/RAM on one machine)
as well as horizontal (by means of adding resources on multiple machines) scalability.

The main idea of the test was to compare performance of SeQuiLa with other tools like featureCounts and genAp that can be used
to compute the number of reads intersecting predefined genomic intervals. It is by no means one of the most commonly used operations
in both DNA and RNA-seq data processing, most notably in gene differential expression and copy number variation-calling.
featureCounts performance results have been treated as a baseline. In order to show the difference between the naive approach using the
default range join strategy available in Spark SQL and SeQuiLa interval-tree one, we have included it in the single-node test.


Test environment setup
----------------------

Infrastructure
--------------

Our tests have been run on a 6-node Hadoop cluster:

======  =============== =========   ===
Role    Number of nodes CPU cores   RAM
======  =============== =========   ===
EN              1           24      64
NN/RM           1           24      64
DN/NM           4           24      64
======  =============== =========   ===

EN - edge node where only Application masters and Spark Drivers have been launched in case of cluster tests.
In case of single node tests (Apache Spark local mode), all computations have been performed on the edge node.

NN - HDFS NameNode

RM - YARN ResourceManager

DN - HDFS DataNode

NM - YARN NodeManager


Software
--------

All tests have been run using the following software components:

=============   =======
Software        Version
=============   =======
CDH             5.12
Apache Hadoop   2.6.0
Apache Spark    2.2.1
ADAM            0.22
featureCounts   1.6.0
Oracle JDK      1.8
Scala           2.11.8
=============   =======


Datasets
********
Two NGS datasets have been used in all the tests.
WES (whole exome sequencing) and WGS (whole genome sequencing) datasets have been used for vertical and horizontal scalability
evaluation respectively. Both of them came from sequencing of NA12878 sample that is widely used in many benchmarks.
The table below presents basic datasets information:

=========   ======  =========    ==========
Test name   Format  Size [GB]    Row count
=========   ======  =========    ==========
WES-SN      BAM     17           161544693
WES-SN      ADAM    14           161544693
WES-SN      BED     0.0045       193557
WGS-CL      BAM     273          2617420313
WGS-CL      ADAM    215          2617420313
WGS-CL      BED     0.0016       68191
=========   ======  =========    ==========

WES-SN - tests performed on a single node using WES dataset

WGS-CL - tests performed on a cluster using WGS dataset


Test procedure
**************
To achieve reliable results test cases have been run 3 times.
Before each run disk caches on all nodes have been purged.

File-dataframe mapping
----------------------

The first step of the testing procedure was to prepare mapping between input datasets (in BAM, ADAM and BED formats)  and
their corresponding dataframe/table abstraction. In case of alignment files our custom data sources has been used, for a BED file Spark's builtin dedicated
for CSV data access.


BAM

.. code-block:: scala

    val bamPath = "NA12878*.bam"
    spark.sql(
      s"""
         |CREATE TABLE reads
         |USING org.biodatageeks.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamPath}")
         |
      """.stripMargin)
    spark.sql(s"SELECT contigName,start,end FROM reads LIMIT 1").show()

    +----------+-----+---+
    |contigName|start|end|
    +----------+-----+---+
    |      chr1|   34|109|
    +----------+-----+---+


ADAM

.. code-block:: scala

    val adamPath = "NA12878*.adam"
    spark.sql(
      s"""
         |CREATE TABLE reads
         |USING org.biodatageeks.datasources.ADAM.ADAMDataSource
         |OPTIONS(path "${adamPath}")
         |
      """.stripMargin)
    spark.sql(s"SELECT contigName,start,end FROM reads LIMIT 1").show()

    +----------+-----+---+
    |contigName|start|end|
    +----------+-----+---+
    |      chr1|   34|109|
    +----------+-----+---+

BED

.. code-block:: scala

    val  bedPath="tgp_exome_hg18.bed"
    spark.sql(s"""
        |CREATE TABLE targets(contigName String,start Integer,end Integer)
        |USING csv
        |OPTIONS (path "file:///${bedPath}", delimiter "\t")""".stripMargin)
    spark.sql("SELECT * FROM targets LIMIT 1").show

    +----------+-----+----+
    |contigName|start| end|
    +----------+-----+----+
    |      chr1| 4806|4926|
    +----------+-----+----+





SQL query for counting features
-------------------------------

For counting reads overlapping predefined feature regions the following SQL query has been used:

.. code-block:: sql

    SELECT targets.contigName,targets.start,targets.end,count(*) FROM reads JOIN targets
         ON (targets.contigName=reads.contigName
         AND
         CAST(reads.end AS INTEGER)>=CAST(targets.start AS INTEGER)
         AND
         CAST(reads.start AS INTEGER)<=CAST(targets.end AS INTEGER)
         )
         GROUP BY targets.contigName,targets.start,targets.end

Exactly the same query has been used for both single node and cluster tests.


Apache Spark settings
---------------------

=============== ======
Parameter       Values
=============== ======
driver-memory    8g
executor-memory  4-8g
executor-cores   2-4
num-executors    1-15
=============== ======

Results
*******
SeQuiLa when run in parallel outperforms selected competing tools in terms of speed on single node (1.7-22.1x) and cluster (3.2-4.7x).
SeQuiLa strategy involving broadcasting interval forest with all data columns (SeQuiLa_it_all) performs best
in most of the cases (no network shuffling required), whereas broadcasting intervals with identifiers only (SeQuiLa_it_int)
performs comparable to, or better than GenAp.
All algorithms favours columnar (ADAM) to row oriented (BAM) file format due to column pruning and disk I/O operations reduction.


Local mode
----------

.. image:: local.*


Hadoop cluster
--------------

.. image:: cluster.*


Limitations
-----------

SeQuiLa is slower than featureCounts in a single-threaded applications due to less performat Java BAM reader (mainly BGZF decompression) available
in the Java htsjdk library. We will try to investigate and resolve this bottleneck in the next major release.

Discussion
**********
Results showed that SeQuiLa significantly accelerates  genomic interval queries.
We are aware that paradigm of distributed computing is currently not fully embraced by bioinformaticians therefore we have put
an additional effort into preparing SeQuiLa to be easily integrated into existing applications and pipelines.


Depth of coverage
#################

Performance tests description
*****************************

The main goal of our tests was to compare SeQuiLa-cov performance and scalability with other state-of-the art coverage solutions (samtools ``depth``, bedtools ``genomecov``, GATK ``DepthOfCoverage``, sambamba ``depth`` and mosdepth). The tests were performed on the aligned WES and WGS reads from the NA12878 sample and aimed at calculating blocks and window coverage whenever this functionality was available. Additionally, we performed quality check, veryfing that results generated by SeQuiLa-cov are identical to those returned by samtools ``depth`` and we evaluated the impact of Intel GKL on overall performance.

Test environment setup
----------------------

Infrastructure
--------------

Our tests have been run on a 24-node Hadoop cluster:

======  =============== =========   ===
Role    Number of nodes CPU cores   RAM
======  =============== =========   ===
EN              1         28/56     512
NN/RM           3         28/56     512
DN/NM           24        28/56     512
======  =============== =========   ===

EN - edge node where only Application masters and Spark Drivers have been launched in case of cluster tests.
In case of single node tests (Apache Spark local mode), all computations have been performed on the edge node.

NN - HDFS NameNode

RM - YARN ResourceManager

DN - HDFS DataNode

NM - YARN NodeManager


Software
--------

All tests have been run using the following software components:

=============   =======
Software        Version
=============   =======
HDP             3.0.1
Apache Hadoop   3.1.1
Apache Spark    2.3.1
Oracle JDK      1.8
Scala           2.12
samtools        1.9
bedtools        2.27
GATK            3.8
sambamba        0.6.8
mosdepth        0.2.3
=============   =======


Datasets
********
Two NGS datasets have been used in all the tests.
WES (whole exome sequencing) and WGS (whole genome sequencing) datasets have been used for vertical and horizontal scalability
evaluation respectively. Both of them came from sequencing of NA12878 sample that is widely used in many benchmarks.
The table below presents basic datasets information:

=========   ======  =========    ==========
Test name   Format  Size [GB]    Row count
=========   ======  =========    ==========
WES          BAM     17          161544693
WGS          BAM     273         2617420313
=========   ======  =========    ==========

WES - tests performed on a single node using WES dataset

WGS - tests performed on a cluster using WGS dataset


Test procedure
**************
To achieve reliable results and remove test cases have been run 3 times.
.. Before each run disk caches on all nodes have been purged.

File-dataframe mapping
----------------------

The first step of the testing procedure was to prepare mapping between input datasets in BAM format and
its dataframe/table abstraction through our custom data source.


BAM

.. code-block:: scala

    val bamPath = "NA12878*.bam"
    spark.sql(
      s"""
         |CREATE TABLE reads
         |USING org.biodatageeks.datasources.BAM.BAMDataSource
         |OPTIONS(path "${bamPath}")
         |
      """.stripMargin)
    spark.sql(s"SELECT contigName,start,end FROM reads LIMIT 1").show()

    +----------+-----+---+
    |contigName|start|end|
    +----------+-----+---+
    |      chr1|   34|109|
    +----------+-----+---+




Coverage calculations
-------------------------

For calculating the coverage the following commands have been used:

.. code-block:: bash

    ### SAMTOOLS
    #exome - bases 1 core
    { time samtools depth NA12878.ga2.exome.maq.recal.bam > samtools/NA12878.ga2.exome.maq.recal.depth ; } 2>> samtools/wes_time.txt
    # genome - bases 1 core
    { time samtools depth NA12878.hiseq.wgs.bwa.recal.bam > samtools/NA12878.hiseq.wgs.bwa.recal.depth ; } 2>> samtools/wgs_time.txt

    ### BEDTOOLS
    # exome blocks 1 core
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 biocontainers/bedtools:v2.27.0_cv2 bedtools genomecov -ibam /data/samples/NA12878/WES/NA12878.proper.wes.bam -bga > /data/samples/NA12878/bedtools_genomecov_block_coverage_wes.txt

    # genome blocks 1 core
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 biocontainers/bedtools:v2.27.0_cv2 bedtools genomecov -ibam /data/samples/NA12878/WGS/NA12878.proper.wgs.bam -bga > /data/samples/NA12878/bedtools_genomecov_block_coverage_wgs.txt

    ### GATK
    #  exome 1 core
    { time docker run -it  -v /data/samples/NA12878/WES:/data/ -v /data/samples/hg_builds/:/ref/ broadinstitute/gatk3:3.8-1 \
        java -jar GenomeAnalysisTK.jar \
    -T DepthOfCoverage \
    -R /ref/Homo_sapiens_assembly18.fasta \
    -o /data/gatk_doc_test.txt \
    -I /data/NA12878.proper.wes.bam \
    -omitIntervals \
    -nt 1} 2>> gatk_wes_time_1.txt

    # genome 1 core
    { time docker run -it  -v /data/samples/NA12878/WGS:/data/ -v /data/samples/hg_builds/:/ref/ broadinstitute/gatk3:3.8-1 \
        java -jar GenomeAnalysisTK.jar \
    -T DepthOfCoverage \
    -R /ref/Homo_sapiens_assembly18.fasta \
    -o /data/gatk_doc_test.txt \
    -I /data/NA12878.proper.wgs.bam \
    -omitIntervals \
    -nt 1} 2>> gatk_wgs_time_1.txt

    ### SAMBAMBA
    # exome - blocks 1,5,10 cores
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth base --output-file=sambamba_base_coverage.txt --nthreads=1 /data/samples/NA12878/WES/NA12878.proper.wes.bam
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth base --output-file=sambamba_base_coverage.txt --nthreads=5 /data/samples/NA12878/WES/NA12878.proper.wes.bam
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth base --output-file=sambamba_base_coverage.txt --nthreads=10 /data/samples/NA12878/WES/NA12878.proper.wes.bam
    
    # exome - windows 1,5,10 cores
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth window --output-file=sambamba_window_coverage.txt --nthreads=1 --window-size=500 /data/samples/NA12878/WES/NA12878.proper.wes.bam
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth window --output-file=sambamba_window_coverage.txt --nthreads=5 --window-size=500 /data/samples/NA12878/WES/NA12878.proper.wes.bam
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth window --output-file=sambamba_window_coverage.txt --nthreads=10 --window-size=500 /data/samples/NA12878/WES/NA12878.proper.wes.bam

    # genome - blocks 1,5,10 cores
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth base --output-file=sambamba_base_coverage.txt --nthreads=1 /data/samples/NA12878/WGS/NA12878.proper.wgs.bam
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth base --output-file=sambamba_base_coverage.txt --nthreads=5 /data/samples/NA12878/WGS/NA12878.proper.wgs.bam
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth base --output-file=sambamba_base_coverage.txt --nthreads=10 /data/samples/NA12878/WGS/NA12878.proper.wgs.bam

    # genome - windows
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth window --output-file=sambamba_window_coverage.txt --nthreads=1 --window-size=500 /data/samples/NA12878/WGS/NA12878.proper.wgs.bam
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth window --output-file=sambamba_window_coverage.txt --nthreads=5 --window-size=500 /data/samples/NA12878/WGS/NA12878.proper.wgs.bam
    time docker run --rm -v /data/samples/NA12878:/data/samples/NA12878 -w /data/samples/NA12878 wkusmirek/sambamba /opt/sambamba-0.6.8-linux-static depth window --output-file=sambamba_window_coverage.txt --nthreads=10 --window-size=500 /data/samples/NA12878/WGS/NA12878.proper.wgs.bam


    ### MOSDEPTH 
    # exome blocks 1,5,10 cores
    { time mos/mosdepth prefix NA12878.proper.wes.bam ; } 2>> mos_wes_time_1.txt
    { time mos/mosdepth -t 4 prefix NA12878.proper.wes.bam ; } 2>> mos_wes_time_5.txt
    { time mos/mosdepth -t 9 prefix NA12878.proper.wes.bam ; } 2>> mos_wes_time_10.txt

    # genome blocks 1,5,10 cores
    { time mos/mosdepthh prefix NA12878.proper.wgs.bam ; } 2>> wgs_time_1.txt
    { time mos/mosdepth -t 4 prefix NA12878.proper.wgs.bam ; } 2>> wgs_time_5.txt
    { time mos/mosdepth -t 9 prefix NA12878.proper.wgs.bam ; } 2>> wgs_time_9.txt

    ### SEQUILA-COV
    # spark shell started with 1,5,10 cores
    spark-shell  --conf "spark.sql.catalogImplementation=in-memory" --conf spark.dynamicAllocation.enabled=false  --master=yarn-client --driver-memory=4g --executor-memory=4g --num-executors=1 --packages org.biodatageeks:bdg-sequila_2.11:0.4.1-SNAPSHOT --repositories https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/ -v

    spark-shell  --conf "spark.sql.catalogImplementation=in-memory" --conf spark.dynamicAllocation.enabled=false  --master=yarn-client --driver-memory=4g --executor-memory=4g --num-executors=5 --packages org.biodatageeks:bdg-sequila_2.11:0.4.1-SNAPSHOT --repositories https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/ -v  

    spark-shell  --conf "spark.sql.catalogImplementation=in-memory" --conf spark.dynamicAllocation.enabled=false  --master=yarn-client --driver-memory=4g --executor-memory=4g --num-executors=10 --packages org.biodatageeks:bdg-sequila_2.11:0.4.1-SNAPSHOT --repositories https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/ -v  

.. code-block:: scala
    
    // inside spark-shell for SeQuiLa-cov
    import org.apache.spark.sql.SequilaSession
    import org.biodatageeks.utils.{SequilaRegister, UDFRegister,BDGInternalParams}
    spark.sqlContext.setConf(BDGInternalParams.InputSplitSize, "134217728")
        val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sqlContext.setConf("spark.biodatageeks.bam.useGKLInflate","true")
    ss.sqlContext.setConf("spark.biodatageeks.bam.useSparkBAM","false")

    /* WES -bases-blocks*/
    ss.sql("""
    CREATE TABLE IF NOT EXISTS reads_exome USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path '/data/samples/NA12878/WES/NA12878*.bam')""")
    spark.time{
    ss.sql(s"SELECT * FROM bdg_coverage('reads_exome','NA12878', 'blocks')").write.format("parquet").save("/data/samples/NA12878/output_tmp/wes_1_9.parquet")}

    /* WGS -bases-blocks*/
    import org.apache.spark.sql.SequilaSession
    import org.biodatageeks.utils.{SequilaRegister, UDFRegister}
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sqlContext.setConf("spark.biodatageeks.bam.useGKLInflate","true")
    ss.sqlContext.setConf("spark.biodatageeks.bam.useSparkBAM","false")
    /*bases-blocks*/
    ss.sql("""
    CREATE TABLE IF NOT EXISTS reads_genome USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path '/data/samples/NA12878/NA12878*.bam')""")
    spark.time{
    ss.sql(s"SELECT * FROM bdg_coverage('reads_genome','NA12878', 'blocks')").write.format("parquet").save("/data/samples/NA12878/output_tmp/wgs_1_1.parquet")}

    /*windows - 500*/
    import org.apache.spark.sql.SequilaSession
    import org.biodatageeks.utils.{SequilaRegister, UDFRegister}
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    ss.sqlContext.setConf("spark.biodatageeks.bam.useGKLInflate","true")
    ss.sqlContext.setConf("spark.biodatageeks.bam.useSparkBAM","false")
    /*bases-blocks*/
  ss.sql("""
    CREATE TABLE IF NOT EXISTS reads_exome USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path '/tmp/fp16yq/data/exome/32MB/*.bam')""")
    spark.time{
    ss.sql(s"SELECT * FROM bdg_coverage('reads_exome','NA12878', 'blocks', '500')").write.format("parquet").save("/tmp/fp16yq/data/32MB_w500_3.parquet") }




Apache Spark settings
---------------------

=============== ======
Parameter       Values
=============== ======
driver-memory    8g
executor-memory  4g
executor-cores   1
num-executors    1-500
=============== ======


Results
*******

.. image:: coverage.*
