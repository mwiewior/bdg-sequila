package org.biodatageeks.sequila.pileup

/**
  * generic pileup representation
  */
abstract class PileupModel {

}

/**
  * Simple, fast pileup representation counting number of refs and non-refs
  */
case class PileupRecord (
                          contig: String,
                          pos: Int,
                          ref: String,
                          cov: Short,
                          countRef:Short,
                          countNonRef:Short)
  extends PileupModel


/** Events aggregation on contig
 */
case class ContigEventAggregate (
                                  contig: String = "",
                                  contigLen: Int = 0,
                                  cov: Array[Short]= Array(0),
                                  startPosition: Int = 0,
                                  maxPosition: Int = 0
                                )