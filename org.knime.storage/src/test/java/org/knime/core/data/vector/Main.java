package org.knime.core.data.vector;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.knime.core.data.arrow.ArrowUtils;
import org.knime.core.data.table.column.ColumnSchema;
import org.knime.core.data.table.column.ColumnType;
import org.knime.core.data.table.column.ReadableColumnCursor;
import org.knime.core.data.table.column.WritableColumnCursor;
import org.knime.core.data.table.value.ReadableDoubleValue;
import org.knime.core.data.table.value.WritableDoubleValue;
import org.knime.core.data.vector.table.VectorStoreBackedTable;

public class Main {

	public static void main(String[] args) {
		try {
			new Main().columnwiseWriteReadSingleDoubleColumnIdentityTest();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// in numValues per vector
	private static final int BATCH_SIZE = 3_000_000;

	// in bytes
	private static final long OFFHEAP_SIZE = 2048_000_000;
	private static final ColumnSchema doubleVectorSchema = () -> ColumnType.DOUBLE;

	public void columnwiseWriteReadSingleDoubleColumnIdentityTest() throws Exception {
		final long numRows = 100_000_000;

		for (int z = 0; z < 100; z++) {
			try (final VectorStoreBackedTable table = ArrowUtils.createArrowTable(BATCH_SIZE, OFFHEAP_SIZE, doubleVectorSchema)) {

				final long time = System.nanoTime();
				// first write
				try (final WritableColumnCursor column = table.getWritableColumnCursor(0)) {
					final WritableDoubleValue value = (WritableDoubleValue) column.getValueAccess();
					for (long i = 0; i < numRows; i++) {
						column.fwd();
						if (i % numRows / 100 == 0) {
							value.setMissing();
						} else {
							value.setDoubleValue(i);
						}
					}
				}
				// then read
				try (final ReadableColumnCursor readableColumn = table.getReadableColumn(0).createCursor()) {
					final ReadableDoubleValue readableValue = (ReadableDoubleValue) readableColumn
							.getValueAccess();
					for (long i = 0; readableColumn.canFwd(); i++) {
						readableColumn.fwd();
						if (i % numRows / 100 == 0) {
							Assert.assertTrue(readableValue.isMissing());
						} else {
							Assert.assertEquals(i, readableValue.getDoubleValue(), 0.0000001);
						}
					}
				}
				System.out.println("Took ms: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time));
			}
		}
	}
}
