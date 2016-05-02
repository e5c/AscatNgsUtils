package org.hammerlab.ascat.preprocess

import org.hammerlab.ascat.util.{AscatFunSuite, TestUtil}
import org.hammerlab.guacamole.Common
import org.hammerlab.guacamole.loci.LociArgs
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.logging.DebugLogArgs

class PreprocessSuite extends AscatFunSuite {
  /*  val normalBam = TestUtil.testDataPath("normal.bam")
  val tumorBam = TestUtil.testDataPath("tumor.bam")

  val dbsnpVcf = TestUtil.testDataPath("dbsnp.vcf")
  val exonVcf = TestUtil.testDataPath("dbsnp_noIntrons.vcf")

  val dbsnpTxt = TestUtil.testDataPath("dbsnpExonLoci_head500.txt")

  val hg19PartialFasta = TestUtil.testDataPath("hg19.partial.fasta")
  def hg19PartialReference = {
    ReferenceBroadcast(hg19PartialFasta, sc, partialFasta = true)
  }
  */

  sparkTest("i/o from both hdfs and nfs") {

  }

  sparkTest("loci parsing") {
    class TestArgs extends DebugLogArgs with LociArgs {}

    val args1 = new TestArgs()
    args1.loci = "1:10176-10177, 1:10351-10352, 1:10351-10352, 1:10615-10637, 1:10641-10642, 1:11007-11008, 1:11011-11012, 1:11062-11063, 1:13272-13273, 1:13283-13284, 1:13288-13291, 1:13379-13380, 1:13444-13445, 1:13452-13453, 1:13482-13483, 1:13549-13550, 1:14463-14464, 1:14598-14599"
    Common.lociFromArguments(args1).result should equal(
      LociSet("1:10176-10177, 1:10351-10352, 1:10351-10352, 1:10615-10637, 1:10641-10642, 1:11007-11008, 1:11011-11012, 1:11062-11063, 1:13272-13273, 1:13283-13284, 1:13288-13291, 1:13379-13380, 1:13444-13445, 1:13452-13453, 1:13482-13483, 1:13549-13550, 1:14463-14464, 1:14598-14599"))

    // Test -loci-from-file argument. The test file gives a loci set equal to 20:100-200.
    val args2 = new TestArgs()
    args2.lociFromFile = TestUtil.testDataPath("dbsnpExonLoci_head500.txt")
    //val lociSet = Common.lociFromArguments(args2).result // should equal(LociSet("20:100-200"))

    //val args3 = new Test
  }

  /*
  // TO DO: complete all of these
  sparkTest("filter intronic SNPs from the full dbsnp VCF") {
    AscatInput.filterIntronSnps(sc, dbsnpVcf, exonVcf)

  }

  sparkTest("Compute the correct read depths from tumor and normal bams")

  sparkTest("Compute the correct logR (base 2) for tumor")
  */
}
