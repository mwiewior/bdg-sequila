package htsjdk.samtools;

/*******************************************************************************
 * Copyright 2013-2016 EMBL-EBI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License countingInputStream distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/


import htsjdk.samtools.*;
import htsjdk.samtools.SAMFileHeader.SortOrder;
import htsjdk.samtools.cram.build.*;
import htsjdk.samtools.cram.build.ContainerBDGParser;
import htsjdk.samtools.cram.build.CramBDGContainerIterator;
import htsjdk.samtools.cram.build.CramBDGSpanContainerIterator;
import htsjdk.samtools.cram.io.CountingInputStream;
import htsjdk.samtools.cram.ref.CRAMReferenceSource;
import htsjdk.samtools.cram.structure.*;
import htsjdk.samtools.cram.structure.BDGContainer;
import htsjdk.samtools.cram.structure.ContainerBDGIO;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.Log;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.*;

import htsjdk.samtools.cram.CRAMException;

public class CRAMBDGIterator implements SAMRecordIterator {
    private static final Log log = Log.getInstance(htsjdk.samtools.CRAMIterator.class);
    private final CountingInputStream countingInputStream;
    private final CramHeader cramHeader;
    private final ArrayList<SAMRecord> records;
    private SAMRecord nextRecord = null;
    private final CramNormalizer normalizer;
    private byte[] refs;
    private int prevSeqId = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
    public htsjdk.samtools.cram.structure.BDGContainer container;
    private SamReader mReader;
    long firstContainerOffset = 0;
    private final Iterator<BDGContainer> containerIterator;

    private final ContainerBDGParser parser;
    private final CRAMReferenceSource referenceSource;

    private Iterator<SAMRecord> iterator = Collections.<SAMRecord>emptyList().iterator();

    private ValidationStringency validationStringency = ValidationStringency.DEFAULT_STRINGENCY;

    public ValidationStringency getValidationStringency() {
        return validationStringency;
    }

    public void setValidationStringency(
            final ValidationStringency validationStringency) {
        this.validationStringency = validationStringency;
    }

    /**
     * `samRecordIndex` only used when validation is not `SILENT`
     * (for identification by the validator which records are invalid)
     */
    private long samRecordIndex;
    private ArrayList<CramCompressionRecord> cramRecords;

    public CRAMBDGIterator(final InputStream inputStream, final CRAMReferenceSource referenceSource, final ValidationStringency validationStringency)
            throws IOException {
        if (null == referenceSource) {
            throw new CRAMException("A reference source is required for CRAM files");
        }
        this.countingInputStream = new CountingInputStream(inputStream);
        this.referenceSource = referenceSource;
        this.validationStringency = validationStringency;
        final CramBDGContainerIterator containerIterator = new CramBDGContainerIterator(this.countingInputStream);
        cramHeader = containerIterator.getCramHeader();
        this.containerIterator = containerIterator;

        firstContainerOffset = this.countingInputStream.getCount();
        records = new ArrayList<SAMRecord>(CRAMContainerStreamWriter.DEFAULT_RECORDS_PER_SLICE);
        normalizer = new CramNormalizer(cramHeader.getSamFileHeader(),
                referenceSource);
        parser = new ContainerBDGParser(cramHeader.getSamFileHeader());
    }


    public CRAMBDGIterator(final InputStream inputStream,  final ValidationStringency validationStringency)
            throws IOException {
        this.countingInputStream = new CountingInputStream(inputStream);
        this.validationStringency = validationStringency;
        final CramBDGContainerIterator containerIterator = new CramBDGContainerIterator(this.countingInputStream);
        cramHeader = containerIterator.getCramHeader();
        this.containerIterator = containerIterator;

        firstContainerOffset = this.countingInputStream.getCount();
        records = new ArrayList<SAMRecord>(CRAMContainerStreamWriter.DEFAULT_RECORDS_PER_SLICE);
        normalizer = null;
        referenceSource = null;
        parser = new ContainerBDGParser(cramHeader.getSamFileHeader());
    }

    public CRAMBDGIterator(final SeekableStream seekableStream, final CRAMReferenceSource referenceSource, final long[] coordinates, final ValidationStringency validationStringency)
            throws IOException {
        if (null == referenceSource) {
            throw new CRAMException("A reference source is required for CRAM files");
        }
        this.countingInputStream = new CountingInputStream(seekableStream);
        this.referenceSource = referenceSource;
        this.validationStringency = validationStringency;
        final CramBDGSpanContainerIterator containerIterator = CramBDGSpanContainerIterator.fromFileSpan(seekableStream, coordinates);
        cramHeader = containerIterator.getCramHeader();
        this.containerIterator = containerIterator;

        firstContainerOffset = containerIterator.getFirstContainerOffset();
        records = new ArrayList<SAMRecord>(CRAMContainerStreamWriter.DEFAULT_RECORDS_PER_SLICE);
        normalizer = new CramNormalizer(cramHeader.getSamFileHeader(),
                referenceSource);
        parser = new ContainerBDGParser(cramHeader.getSamFileHeader());
    }



    public CRAMBDGIterator(final SeekableStream seekableStream, final long[] coordinates, final ValidationStringency validationStringency)
            throws IOException {
        this.countingInputStream = new CountingInputStream(seekableStream);
        this.referenceSource = null;
        this.validationStringency = validationStringency;
        final CramBDGSpanContainerIterator containerIterator = CramBDGSpanContainerIterator.fromFileSpan(seekableStream, coordinates);
        cramHeader = containerIterator.getCramHeader();
        this.containerIterator = containerIterator;

        firstContainerOffset = containerIterator.getFirstContainerOffset();
        records = new ArrayList<SAMRecord>(CRAMContainerStreamWriter.DEFAULT_RECORDS_PER_SLICE);
        normalizer = null;
        parser = new ContainerBDGParser(cramHeader.getSamFileHeader());
    }

    @Deprecated
    public CRAMBDGIterator(final SeekableStream seekableStream, final CRAMReferenceSource referenceSource, final long[] coordinates)
            throws IOException {
        this(seekableStream, referenceSource, coordinates, ValidationStringency.DEFAULT_STRINGENCY);
    }

    public CramHeader getCramHeader() {
        return cramHeader;
    }

    void nextContainer() throws IOException, IllegalArgumentException,
            IllegalAccessException, CRAMException {

        if (containerIterator != null) {
            if (!containerIterator.hasNext()) {
                records.clear();
                nextRecord = null;
                return;
            }
            container = containerIterator.next();
            if (container.isEOF()) {
                records.clear();
                nextRecord = null;
                return;
            }
        } else {
            container = ContainerBDGIO.readContainer(cramHeader.getVersion(), countingInputStream);
            if (container.isEOF()) {
                records.clear();
                nextRecord = null;
                return;
            }
        }

        records.clear();
        if (cramRecords == null)
            cramRecords = new ArrayList<CramCompressionRecord>(container.nofRecords);
        else
            cramRecords.clear();

        parser.getRecords(container, cramRecords, validationStringency);

        if (container.sequenceId == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
            refs = new byte[]{};
            prevSeqId = SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX;
        } else if (container.sequenceId == Slice.MULTI_REFERENCE) {
            refs = null;
            prevSeqId = Slice.MULTI_REFERENCE;
        } else if (prevSeqId < 0 || prevSeqId != container.sequenceId) {
            final SAMSequenceRecord sequence = cramHeader.getSamFileHeader()
                    .getSequence(container.sequenceId);
//            refs = referenceSource.getReferenceBases(sequence, true);
//            if (refs == null) {
//                throw new CRAMException(String.format("Contig %s not found in the reference file.", sequence.getSequenceName()));
//            }
            prevSeqId = container.sequenceId;
        }

        for (int i = 0; i < container.slices.length; i++) {
            final Slice slice = container.slices[i];

            if (slice.sequenceId < 0)
                continue;

            if (refs != null && !slice.validateRefMD5(refs)) {
                final String msg = String.format(
                        "Reference sequence MD5 mismatch for slice: sequence id %d, start %d, span %d, expected MD5 %s",
                        slice.sequenceId,
                        slice.alignmentStart,
                        slice.alignmentSpan,
                        String.format("%032x", new BigInteger(1, slice.refMD5)));
                throw new CRAMException(msg);
            }
        }

        if (refs != null) normalizer.normalize(cramRecords, refs, 0,
                container.header.substitutionMatrix);

        final Cram2SamRecordFactory cramToSamRecordFactory = new Cram2SamRecordFactory(
                cramHeader.getSamFileHeader());

        for (final CramCompressionRecord cramRecord : cramRecords) {
            final SAMRecord samRecord = cramToSamRecordFactory.create(cramRecord);
            if (!cramRecord.isSegmentUnmapped()) {
                final SAMSequenceRecord sequence = cramHeader.getSamFileHeader()
                        .getSequence(cramRecord.sequenceId);
                //refs = referenceSource.getReferenceBases(sequence, true);
            }

            samRecord.setValidationStringency(validationStringency);

            if (mReader != null) {
                final long chunkStart = (container.offset << 16) | cramRecord.sliceIndex;
                final long chunkEnd = ((container.offset << 16) | cramRecord.sliceIndex) + 1;
                nextRecord.setFileSource(new SAMFileSource(mReader,
                        new BAMFileSpan(new Chunk(chunkStart, chunkEnd))));
            }

            records.add(samRecord);
        }
        cramRecords.clear();
        iterator = records.iterator();
    }

    /**
     * Skip cached records until given alignment start position.
     *
     * @param refIndex reference sequence index
     * @param pos      alignment start to skip to
     */
    public boolean advanceToAlignmentInContainer(final int refIndex, final int pos) {
        if (!hasNext()) return false;
        int i = 0;
        for (final SAMRecord record : records) {
            if (refIndex != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX && record.getReferenceIndex() != refIndex) continue;

            if (pos <= 0) {
                if (record.getAlignmentStart() == SAMRecord.NO_ALIGNMENT_START) {
                    iterator = records.listIterator(i);
                    return true;
                }
            } else {
                if (record.getAlignmentStart() >= pos) {
                    iterator = records.listIterator(i);
                    return true;
                }
            }
            i++;
        }
        iterator = Collections.<SAMRecord>emptyList().iterator();
        return false;
    }

    @Override
    public boolean hasNext() {
        if (container != null && container.isEOF()) return false;
        if (!iterator.hasNext()) {
            try {
                nextContainer();
            } catch (IOException | IllegalAccessException e) {
                throw new SAMException(e);
            }
        }

        return !records.isEmpty();
    }

    @Override
    public SAMRecord next() {
        if (hasNext()) {

            SAMRecord samRecord = iterator.next();

            if (validationStringency != ValidationStringency.SILENT) {
                SAMUtils.processValidationErrors(samRecord.isValid(), samRecordIndex++, validationStringency);
            }

            return samRecord;

        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void remove() {
        throw new RuntimeException("Removal of records not implemented.");
    }

    @Override
    public void close() {
        records.clear();
        //noinspection EmptyCatchBlock
        try {
            if (countingInputStream != null)
                countingInputStream.close();
        } catch (final IOException e) {
        }
    }

    @Override
    public SAMRecordIterator assertSorted(final SortOrder sortOrder) {
        return SamReader.AssertingIterator.of(this).assertSorted(sortOrder);
    }

    public SamReader getFileSource() {
        return mReader;
    }

    public void setFileSource(final SamReader mReader) {
        this.mReader = mReader;
    }

    public SAMFileHeader getSAMFileHeader() {
        return cramHeader.getSamFileHeader();
    }

}
