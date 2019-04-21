
package htsjdk.samtools;

import htsjdk.samtools.BAMBDGRecord;
import htsjdk.samtools.util.RuntimeIOException;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * This class is required in order to access the protected
 * {@link SAMRecord#eagerDecode()} method in HTSJDK.
 */

public class SAMBDGRecordHelper {
    public static void eagerDecode(SAMRecord record) {

        ( (BAMBDGRecord) record).eagerDecode();
        //record.eagerDecode();
//        final int tagsOffset = record.getVariableBinaryRepresentation().length;
//        final int tagsSize = record.getAttributesBinarySize();
//        final ByteBuffer byteBuffer = ByteBuffer.wrap(record.getVariableBinaryRepresentation(), tagsOffset, tagsSize);
//        //final SAMBinaryTagAndValue attributes = BinaryTagCodec.readTags(record.getVariableBinaryRepresentation(), tagsOffset, tagsSize, record.getValidationStringency());
//        try {
//            while (byteBuffer.hasRemaining()) {
//                final short tag = byteBuffer.getShort();
//                final byte tagType = byteBuffer.get();
//            }
//            //byteBuffer.rewind();
//            //record.eagerDecode();
//        }
//        catch (BufferUnderflowException e) {
//          System.err.println(String.format("Skipping decoding due to %s",e.getMessage()));
//          throw new RuntimeIOException();
//        }


    }
}
