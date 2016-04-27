package org.hammerlab.ascat.preprocess

import java.io.File
import scala.math.log
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.hammerlab.guacamole.Common.NoSequenceDictionaryArgs
import org.hammerlab.guacamole.distributed.LociPartitionUtils
import org.hammerlab.guacamole.distributed.LociPartitionUtils._
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils._
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.InputFilters
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.{Common, ReadSet, SparkCommand}
import org.kohsuke.args4j.{Option => Args4jOption}

/** From ASCAT website (https://www.crick.ac.uk/peter-van-loo/software/ASCAT):
  *
  * ASCAT input files can also be derived from massively parallel sequencing data,
  * through log-transformed normalised read depth (LogR) and allele frequencies
  * (BAF) of selected SNP loci.
  *
 */

object AscatInput {

  class Arguments extends LociPartitionUtils.Arguments with NoSequenceDictionaryArgs with Common.TumorNormalReadsArgs {
    @Args4jOption(name = "--out-dir", usage = "Output dir for logR txt file")
    var outDir: String = "./"

    @Args4jOption(name = "--dbsnp-vcf", required = false, usage = "Vcf file to identify DBSNP variants")
    var dbSnpVcf: String = ""

    @Args4jOption(name = "--dbsnp-txt", required = true, usage = "Loci txt file containing DBSNP variants. ")
    var dbSnpTxt: String = ""

    @Args4jOption(name = "--sequence-dict", required = false, usage = "Path to .dict sequence dictionary")
    var sequenceDictionary: String = ""

    @Args4jOption(name = "--reference", required = true, usage = "Path to reference")
    var referenceFastaPath: String = ""
  }

  object ProcessBams extends SparkCommand[Arguments] {
    override val name = "ASCAT preprocessing"
    override val description = "ETL NGS bam's for input to ASCAT."

    override def run(args: Arguments, sc: SparkContext) = {
      if (args.dbSnpVcf != "")
        createLociFileFromVcf(sc, args.dbSnpVcf, args.dbSnpTxt)

      val dbsnpLoci = Common.lociFromFile(args.dbSnpTxt, null)

      val filters = InputFilters.empty
      val bamLoci = Common.lociFromArguments(args)
      val reference = ReferenceBroadcast(args.referenceFastaPath, sc)

      val (tumorReadSet, normalReadSet): (ReadSet, ReadSet) =
        Common.loadTumorNormalReadsFromArguments(
          args,
          sc,
          filters
        )

      generateDepthOutputs(sc,
        tumorReadSet,
        normalReadSet,
        bamLoci.result(normalReadSet.contigLengths),
        dbsnpLoci,
        args.outDir,
        reference,
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
    * @param dbSnpLoci
    * @param outDir
    * @param referenceBroadcast
    * @param distributedUtilArguments
    */
  def generateDepthOutputs(sc: SparkContext,
                           tumorReadSet: ReadSet,
                           normalReadSet: ReadSet,
                           bamLoci: LociSet,
                           dbSnpLoci: LociSet,
                           outDir: String,
                           referenceBroadcast: ReferenceBroadcast,
                           distributedUtilArguments: LociPartitionUtils.Arguments = new LociPartitionUtils.Arguments {}) = {

    val log2: Double = log(2)

    val broadcastDbsnp = sc.broadcast(dbSnpLoci)

    val lociPartitions = partitionLociAccordingToArgs(
      distributedUtilArguments,
      bamLoci,
      tumorReadSet.mappedReads,
      normalReadSet.mappedReads
    )

    // get the tumor count & normal count; divide; take log(base2)
    def logRFunction(tumorPileup: Pileup, normalPileup: Pileup): Iterator[(Position, Double)] = {
      val chr = tumorPileup.referenceName
      val locus = tumorPileup.locus
      val pos = Position(chr, locus)

      val onSnp: Boolean =
        broadcastDbsnp.value.onContig(chr)
          .contains(locus + 1)
      val logR =
        if (onSnp) log(tumorPileup.elements.length / normalPileup.elements.length)/log2
        else 0

      Iterator((pos, logR))
    }

    def readDepthFunction(pileup: Pileup) = {
      val chr = pileup.referenceName
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

    val tumorNormalizedDepths: RDD[(Position, Double)] =
      pileupFlatMapTwoRDDs(tumorReadSet.mappedReads,
        tumorReadSet.mappedReads,
        lociPartitions,
        true,
        (tumorPileup, normalPileup) => logRFunction(tumorPileup, normalPileup),
        referenceBroadcast)

    val tumorReadDepth: RDD[(Position, Double)] =
      pileupFlatMap(tumorReadSet.mappedReads, lociPartitions, false,
        pileup => readDepthFunction(pileup),
        referenceBroadcast
      )

    val normalReadDepth: RDD[(Position, Double)] =
      pileupFlatMap(tumorReadSet.mappedReads, lociPartitions, false,
        pileup => readDepthFunction(pileup),
        referenceBroadcast
      )

    val rddSeq = Seq(tumorNormalizedDepths, tumorReadDepth, normalReadDepth)
    val outpathSeq = Seq("tumor_logR.txt", "tumor_readDepth.txt", "normal_readDepth.txt")

    def sortAndSave(rdd: RDD[(Position, Double)], path: String): Unit = {
      val srtd = rdd.sortByKey()
      srtd.cache()
      val ct = srtd.count() // force sort
      println("Number of records in RDD for %s: %d".format(path, ct))
      val txtRdd = srtd.map({ case (k, v) => k.toString + "\t" + v.toString })
      txtRdd.saveAsTextFile(outDir + "/" + path)
      srtd.unpersist()
      txtRdd.unpersist()
    }

    (rddSeq zip outpathSeq).foreach({ case (a: RDD[(Position, Double)], b: String) => sortAndSave(a,b)})
  }

  def filterIntronSnps(sc: SparkContext, vcfPath: String, outPath: String) = {
    val vcf = sc.textFile(vcfPath)
    val filtered = vcf.map(s => {
      if (s contains "INT") ""
      else s
    } )

    filtered.saveAsTextFile(outPath)
  }

  /**
    *
    * @param sc
    * @param vcfPath
    * @param outPath
    */
  def createLociFileFromVcf(sc: SparkContext, vcfPath: String, outPath: String) = {
    // demeter: ~/data/common_all_20160407.vcf

    // Some logic around making sure only a no-intron dbsnp VCF is being loaded
    val noIntronVcfPath =
      if (!(vcfPath contains "noIntrons.vcf"))
        vcfPath + "_noIntrons.vcf"
      else vcfPath

    val f = new File(noIntronVcfPath)
    if (!f.exists)
      filterIntronSnps(sc, vcfPath, noIntronVcfPath)

    val variants = sc.loadVariants(noIntronVcfPath, None)
    val loci = variants.map(v => (v.getContig, v.getStart))
      .map({ case (c, s) => s"$c:$s"})

    loci.saveAsTextFile(outPath)
  }
}