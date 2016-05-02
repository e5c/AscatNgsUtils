package org.hammerlab.ascat.preprocess

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.hammerlab.guacamole.Common.NoSequenceDictionaryArgs
import org.hammerlab.guacamole.distributed.LociPartitionUtils
import org.hammerlab.guacamole.distributed.LociPartitionUtils._
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils._
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.logging.LoggingUtils
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.InputFilters
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.{Common, ReadSet, SparkCommand}
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.math.log

/** From ASCAT website (https://www.crick.ac.uk/peter-van-loo/software/ASCAT):
  *
  * ASCAT input files can also be derived from massively parallel sequencing data,
  * through log-transformed normalised read depth (LogR) and allele frequencies
  * (BAF) of selected SNP loci.
  *
 */

object AscatInput {

  class Arguments extends LociPartitionUtils.Arguments with NoSequenceDictionaryArgs with Common.TumorNormalReadsArgs {
    @Args4jOption(name = "--generate-depths", required = false, usage = "Also write depths to file")
    var generateDepths: String = "true"

    @Args4jOption(name = "--out-dir", usage = "Output dir for logR txt file")
    var outDir: String = "./"

    @Args4jOption(name = "--dbsnp-vcf", required = false, usage = "Vcf file to identify DBSNP variants")
    var dbSnpVcf: String = ""

    @Args4jOption(name = "--sequence-dict", required = false, usage = "Path to .dict sequence dictionary")
    var sequenceDictionary: String = ""

    @Args4jOption(name = "--reference", required = true, usage = "Path to reference")
    var referenceFastaPath: String = ""
  }

  object ProcessBams extends SparkCommand[Arguments] {
    override val name = "ASCAT preprocessing"
    override val description = "ETL NGS bam's for input to ASCAT."

    override def run(args: Arguments, sc: SparkContext) = {
      if (args.dbSnpVcf != "") {
        createLociFileFromVcf(sc, args.dbSnpVcf, args.lociFromFile)
        sc.stop()
      }

      val filters = InputFilters.empty
      val reference = ReferenceBroadcast(args.referenceFastaPath, sc)

      val (tumorReadSet, normalReadSet): (ReadSet, ReadSet) =
        Common.loadTumorNormalReadsFromArguments(
          args,
          sc,
          filters
        )

      // TO DO handle gender either via command-line or within pileup
      val bamLoci = Common.lociFromFile(args.lociFromFile, sc, normalReadSet.contigLengths)

      generateDepthOutputs(sc,
        tumorReadSet,
        normalReadSet,
        bamLoci,
        args.outDir,
        reference,
        args.generateDepths.toBoolean,
        args)
    }
  }

  /**
    * At each dbSnp base, write out the read depth in the tumor and normal bam's respectively; also write
    * the tumor's logR.
    *
    * Tumor LogR matrix: rows are probes or SNP loci; columns are samples (in this case, we only have 1 tumor sample
    * per normal - i.e. a 1-column matrix)
    * Tumor logR values: "normalized" read depths, i.e. log(tumor counts / normal counts)
    * Normal logR values: zero
    *
    * // TODO treat case of if gender == XY
    *
    * @param sc
    * @param tumorReadSet
    * @param normalReadSet
    * @param bamLoci
    * @param outDir
    * @param referenceBroadcast
    * @param distributedUtilArguments
    */
  def generateDepthOutputs(sc: SparkContext,
                           tumorReadSet: ReadSet,
                           normalReadSet: ReadSet,
                           bamLoci: LociSet,
                           outDir: String,
                           referenceBroadcast: ReferenceBroadcast,
                           generateDepths: Boolean,
                           distributedUtilArguments: LociPartitionUtils.Arguments = new LociPartitionUtils.Arguments {}) = {

    val log2: Double = log(2)

    //val broadcastDbsnp = sc.broadcast(dbSnpLoci)

    val lociPartitions = partitionLociAccordingToArgs(
      distributedUtilArguments,
      bamLoci,
      tumorReadSet.mappedReads,
      normalReadSet.mappedReads
    )

    // get the tumor count & normal count; divide; take log(base2)
    def logRFunction(tumorPileup: Pileup, normalPileup: Pileup,
                     generateDepths: Boolean): Iterator[(Position, Seq[Double])] = {
      val chr = stripChr(tumorPileup.referenceName)
      val locus = tumorPileup.locus
      val pos = Position(chr, locus)
      /*
      val onSnp: Boolean =
        broadcastDbsnp.value.onContig(chr)
          .contains(locus + 1)

      val logR: Double =
        if (onSnp) log(tumorPileup.elements.length / normalPileup.elements.length)/log2
        else 0

      if (generateDepths) {
        val (tumorDepth, normalDepth): (Double, Double) =
          if (onSnp) (tumorPileup.elements.length.toDouble, normalPileup.elements.length.toDouble)
          else (0, 0)
          */

      val logR: Double = log(tumorPileup.elements.length / normalPileup.elements.length)/log2

      if (generateDepths) {
        val (tumorDepth, normalDepth): (Double, Double) =
          (tumorPileup.elements.length.toDouble, normalPileup.elements.length.toDouble)
        Iterator((pos, Seq(logR, tumorDepth, normalDepth)))
      }
      else Iterator((pos, Seq(logR)))
    }

    def sortAndSave(rdd: RDD[(Position, Seq[Double])], path: String): Unit = {
      val srtd = rdd.sortByKey()
      srtd.cache()
      val ct = srtd.count() // force sort
      println("Number of records in RDD for %s: %d".format(path, ct))
      val txtRdd = srtd.map({ case (k, v: Seq[Double]) => k.toString + "\t" + v.mkString("\t") })
      txtRdd.saveAsTextFile(outDir + "/" + path)
      srtd.unpersist()
      txtRdd.unpersist()
    }

    val logRAndDepths: RDD[(Position, Seq[Double])] =
      pileupFlatMapTwoRDDs(tumorReadSet.mappedReads,
        tumorReadSet.mappedReads,
        lociPartitions,
        true,
        (tumorPileup, normalPileup) => logRFunction(tumorPileup, normalPileup, generateDepths),
        referenceBroadcast)

    sortAndSave(logRAndDepths, "logR_tumor_normal_depth.txt")
  }

  def stripChr(refName: String): String = {
    if (refName.startsWith("chr")) refName.stripPrefix("chr")
    else refName
  }


  def filterIntronSnps(sc: SparkContext, vcfPath: String, outPath: String, coalesce:Boolean = true) = {
    val vcf = sc.textFile(vcfPath)
    val filtered = vcf.filter(line => !(line.contains("INT")))
    if (coalesce)
      filtered.coalesce(1).saveAsTextFile(outPath)
    else filtered.saveAsTextFile(outPath)
  }

  def doesFileExist(conf: Configuration, f: String): Boolean = {
    val op = new Path(f)
    val outFs = op.getFileSystem(conf)
    outFs.exists(op)
  }
  def movePartFiles(tmpDir: String, finalPath: String, conf: Configuration) = {
    val srcPath = new Path(tmpDir)
    val destPath = new Path(finalPath)

    val sourceFs = srcPath.getFileSystem(conf)
    val destFs = destPath.getFileSystem(conf)

    FileUtil.copyMerge(sourceFs, srcPath, destFs, destPath, true, conf, null)
  }
  /**
    *
    * @param sc
    * @param vcfPath
    * @param outPath
    */
  def createLociFileFromVcf(sc: SparkContext, vcfPath: String, outPath: String) = {
    // demeter: ~/data/common_all_20160407.vcf

    // TO DO: support hdfs files
    val conf = sc.hadoopConfiguration

    if (!doesFileExist(conf, outPath)) {
      // Some logic around making sure only a no-intron dbsnp VCF is being loaded
      val vcfFile: String = {
        if (doesFileExist(conf, vcfPath + "_noIntrons.vcf")) vcfPath + "_noIntrons.vcf"
        else if (!(vcfPath contains "noIntrons.vcf")) {
          // if it hasn't been filtered yet
          val tmpDir = vcfPath + "_noIntron_tmpDir"
          filterIntronSnps(sc, vcfPath, tmpDir)
          val finalPath = vcfPath + "_noIntrons.vcf"
          movePartFiles(tmpDir, finalPath, conf)
          finalPath
        }
        else vcfPath
      }

      val variants = sc.loadVariants(vcfFile, None)
      LoggingUtils.progress("Read no-intron VCF from " + vcfFile)
      val loci = variants.map(v => (v.getContig.getContigName, v.getStart, v.getEnd))
        .map({ case (c, s, e) => s"$c:$s-$e," })

      val outTmp = outPath + "_tmp"
      loci.coalesce(1).saveAsTextFile(outTmp)
      movePartFiles(outTmp, outPath, conf)
    }
    else LoggingUtils.progress(s"$outPath already exists; continuing")
  }

  /*
      def countPartFiles(dir: String) = {
      val path = new Path(dir)
      val ri: RemoteIterator[LocatedFileStatus] = fs.listFiles(path, false)
      var numPartFiles = 0
      while (ri.hasNext) {
        val status = ri.next
        if (status.getPath.toString.startsWith("part")) numPartFiles += 1)
      }
      numPartFiles
    }

  def readDepthFunction(pileup: Pileup) = {
  val chr: String = stripChr(pileup.referenceName)

  val locus = pileup.locus
  val pos = Position(chr, locus)

  val onSnp: Boolean =
    broadcastDbsnp.value.onContig(chr)
      .contains(locus + 1)
  val readDepth: Double =
    if (onSnp) pileup.elements.length.toDouble
    else 0
  Iterator((pos, readDepth))
}


  val rddSeq = Seq(tumorNormalizedDepths, tumorReadDepth, normalReadDepth)
  val outpathSeq = Seq("tumor_logR.txt", "tumor_readDepth.txt", "normal_readDepth.txt")
  (rddSeq zip outpathSeq).foreach(
    { case (rdd: RDD[(Position, Double)], path: String) =>
    //println("Records in RDD: %s".format(rdd.take(10).mkString("\n")))
    sortAndSave(rdd, path)
    rdd.unpersist()
    })
    */
}