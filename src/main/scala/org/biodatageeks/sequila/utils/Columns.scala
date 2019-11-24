package org.biodatageeks.sequila.utils

import org.biodatageeks.formats.Alignment


object Columns {

  private val alignmentColumns = ScalaFuncs.classAccessors[Alignment]

  val SAMPLE = alignmentColumns(0)
  val QNAME = alignmentColumns(1)
  val FLAGS = alignmentColumns(2)
  val CONTIG = alignmentColumns(3)
  val POS = alignmentColumns(4)
  val START = alignmentColumns(5)
  val END = alignmentColumns(6)
  val MAPQ = alignmentColumns(7)
  val CIGAR = alignmentColumns(8)
  val RNEXT = alignmentColumns(9)
  val PNEXT = alignmentColumns(10)
  val TLEN =  alignmentColumns(11)
  val SEQUENCE = alignmentColumns(12)
  val BASEQ = alignmentColumns(13)
  val MATEREFIND = "materefind"
  val SAMRECORD = "SAMRecord"
  val STRAND = "strand"
  val COVERAGE= "coverage"

}
