package org.hammerlab.ascat.preprocess

import org.hammerlab.ascat.util.{AscatFunSuite, TestUtil}
import org.hammerlab.guacamole.reference.ReferenceBroadcast

class PreprocessSuite extends AscatFunSuite {
  val normalBam = TestUtil.testDataPath("normal.bam")
  val tumorBam = TestUtil.testDataPath("tumor.bam")

  val dbsnpVcf = TestUtil.testDataPath("dbsnp.vcf")
  val exonVcf = TestUtil.testDataPath("dbsnp_noIntrons.vcf")

  val hg19PartialFasta = TestUtil.testDataPath("hg19.partial.fasta")
  def hg19PartialReference = {
    ReferenceBroadcast(hg19PartialFasta, sc, partialFasta = true)
  }

  // TO DO: complete all of these
  sparkTest("filter intronic SNPs from the full dbsnp VCF") {
    AscatInput.filterIntronSnps(sc, dbsnpVcf, exonVcf)

  }

  sparkTest("Compute the correct read depths from tumor and normal bams")

  sparkTest("Compute the correct logR (base 2) for tumor")
}
