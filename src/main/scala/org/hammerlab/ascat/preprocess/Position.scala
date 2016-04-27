package org.hammerlab.ascat.preprocess

case class Position(chr: String, start: Long) extends Ordered[Position] {
  override def toString: String = "chr%s:%d".format(chr, start)

  override def compare(that: Position): Int = {
    // Compare chr
    if (this.chr == that.chr) {
      this.start.compare(that.start)
    } else {
      Chromosome(this.chr).compare(Chromosome(that.chr))
    }
  }
}

case class Chromosome(chr: String) extends Ordered[Chromosome] {
  def nonNumericChrMap = Map(("X", 23), ("Y", 24), ("MT", 25))

  def toInt: Int = {
    if (List("X", "Y", "x", "y", "MT").contains(this.chr)) {
      nonNumericChrMap(this.chr.toUpperCase)
    } else {
      chr.toInt
    }
  }

  override def toString: String = chr

  override def compare(that: Chromosome): Int = {
    this.toInt.compare(that.toInt)
  }
}

