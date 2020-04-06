package org.knime.core.data.struct;

import org.junit.Assert;
import org.junit.Test;
import org.knime.core.data.StorageTest;
import org.knime.core.data.arrow.ArrowUtils;
import org.knime.core.data.partition.ReadablePartitionedTable;
import org.knime.core.data.partition.Store;
import org.knime.core.data.partition.WritablePartitionedTable;
import org.knime.core.data.table.column.ColumnSchema;
import org.knime.core.data.table.column.ColumnType;
import org.knime.core.data.table.column.NativeType;
import org.knime.core.data.table.column.ReadableColumnCursor;
import org.knime.core.data.table.column.WritableColumnCursor;
import org.knime.core.data.table.value.ReadableDoubleValue;
import org.knime.core.data.table.value.ReadableStringValue;
import org.knime.core.data.table.value.ReadableStructValue;
import org.knime.core.data.table.value.WritableDoubleValue;
import org.knime.core.data.table.value.WritableStringValue;
import org.knime.core.data.table.value.WritableStructValue;

public class StructTest {

	final static long NUM_ROWS = 100;

	// some funky struct schema
	private static final ColumnSchema[] STRUCT_SCHEMA = new ColumnSchema[] { new ColumnSchema() {
		@Override
		public String name() {
			return "My String, Double Struct";
		}

		@Override
		public ColumnType getColumnType() {
			return new ColumnType() {

				@Override
				public NativeType[] getNativeTypes() {
					return new NativeType[] { NativeType.STRING, NativeType.DOUBLE };
				}
			};
		}
	} };

	class Person {
		String name;
		long age;
	}

	@Test
	public void columnwiseWriteReadStructColumnIdentityTest() throws Exception {
		try (final Store root = ArrowUtils.createArrowStore(StorageTest.OFFHEAP_SIZE, StorageTest.BATCH_SIZE,
				STRUCT_SCHEMA)) {

			// Create writable table on store. Just an access on store.
			final WritablePartitionedTable writableTable = new WritablePartitionedTable(root);

			// first column write
			try (final WritableColumnCursor col0 = writableTable.getWritableColumn(0).createWritableCursor()) {
				final WritableStructValue val0 = (WritableStructValue) col0.getValueAccess();
				// TODO we could offer convenience API with reflection to operate on POJO
				// structs :-)
				final WritableStringValue stringValue = (WritableStringValue) val0.writableValueAt(0);
				final WritableDoubleValue doubleValue = (WritableDoubleValue) val0.writableValueAt(1);
				for (long i = 0; i < NUM_ROWS; i++) {
					col0.fwd();
					stringValue.setStringValue("Name " + i);
					doubleValue.setDoubleValue(i);
				}
			}

			// Done writing?
			final ReadablePartitionedTable readableTable = new ReadablePartitionedTable(root);

			// then read
			try (final ReadableColumnCursor col0 = readableTable.getReadableColumn(0).createCursor()) {
				final ReadableStructValue val0 = (ReadableStructValue) col0.getValueAccess();
				final ReadableStringValue stringValue = (ReadableStringValue) val0.readableValueAt(0);
				final ReadableDoubleValue doubleValue = (ReadableDoubleValue) val0.readableValueAt(1);
				for (long i = 0; col0.canFwd(); i++) {
					col0.fwd();
					Assert.assertEquals(i, doubleValue.getDoubleValue(), 0.0000001);
					Assert.assertEquals("Name " + i, stringValue.getStringValue());
				}
			}
		}
	}

}
