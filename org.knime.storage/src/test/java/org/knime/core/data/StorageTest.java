
package org.knime.core.data;

import org.junit.Assert;
import org.junit.Test;
import org.knime.core.data.arrow.ArrowUtils;
import org.knime.core.data.partition.ReadablePartitionedTable;
import org.knime.core.data.partition.Store;
import org.knime.core.data.partition.WritablePartitionedTable;
import org.knime.core.data.table.column.ColumnSchema;
import org.knime.core.data.table.column.ColumnType;
import org.knime.core.data.table.column.NativeType;
import org.knime.core.data.table.column.ReadableColumnCursor;
import org.knime.core.data.table.column.WritableColumnCursor;
import org.knime.core.data.table.row.ColumnBackedReadableRow;
import org.knime.core.data.table.row.ColumnBackedWritableRow;
import org.knime.core.data.table.row.ReadableRow;
import org.knime.core.data.table.row.WritableRow;
import org.knime.core.data.table.value.ReadableStringValue;
import org.knime.core.data.table.value.WritableStringValue;

public class StorageTest {

	public static void main(String[] args) throws Exception {
		final StorageTest storageTest = new StorageTest();
		for (int i = 0; i < 2; i++) {
			System.out.println("Iteration: " + i);
			storageTest.columnwiseWriteReadSingleColumnIdentityTest();
		}
	}

	/**
	 * Some variables
	 */
	// in numValues per vector
	public static final int BATCH_SIZE = 1_000_000;

	// in bytes
	public static final long OFFHEAP_SIZE = 2000_000_000;

	// num rows used for testing
	public static final long NUM_ROWS = 1_000_000;

	// some schema
	private static final ColumnSchema[] SCHEMAS = new ColumnSchema[] { new ColumnSchema() {
		@Override
		public String name() {
			return "My Double Column";
		}

		@Override
		public ColumnType getColumnType() {
			return new ColumnType() {

				@Override
				public NativeType[] getNativeTypes() {
					return new NativeType[] { NativeType.STRING };
				}
			};
		}
	} };

	/**
	 * TESTS
	 */

	@Test
	public void doubleArrayTest() {
		final double[] array = new double[100_000_000];
		for (int i = 0; i < array.length; i++) {
			array[i] = i;
		}

		for (int i = 0; i < array.length; i++) {
			double k = array[i];
			Assert.assertEquals(array[i], k, 0.00000000000001);
		}
	}

	@Test
	public void columnwiseWriteReadSingleColumnIdentityTest() throws Exception {
		try (final Store root = ArrowUtils.createArrowStore(OFFHEAP_SIZE, BATCH_SIZE, SCHEMAS)) {

			// Create writable table on store. Just an access on store.
			final WritablePartitionedTable writableTable = new WritablePartitionedTable(root);

			// first column write
			try (final WritableColumnCursor col0 = writableTable.getWritableColumn(0).createWritableCursor()) {
				final WritableStringValue val0 = (WritableStringValue) col0.getValueAccess();
				for (long i = 0; i < NUM_ROWS; i++) {
					// TODO it would be cool to do col0.fwd().setDouble('val') or
					// col0.next().getDouble()
					// for(DoubleColumValue val : doubleColumn){
					// }

//					root.flush();

					col0.fwd();
					val0.setStringValue("Entry" + i);
				}
			}

			// TODO this is unfortunately required before reading...
			// TODO implication: We can't offer read access to a table if the table is not
			// entirely flushed AND/OR held in memory, unless we write multiple files per
			// table (chunks) (-> current implementation)
			// TODO maybe Parquet behaves differently?
			// root.closeForWriting();

			// Done writing?
			final ReadablePartitionedTable readableTable = new ReadablePartitionedTable(root);

			// then read
			try (final ReadableColumnCursor col0 = readableTable.getReadableColumn(0).createCursor()) {
				final ReadableStringValue val0 = (ReadableStringValue) col0.getValueAccess();
				for (long i = 0; col0.canFwd(); i++) {
					col0.fwd();
					Assert.assertEquals("Entry" + i, val0.getStringValue());
				}
			}
		}
	}

	@Test
	public void rowwiseWriteReadSingleDoubleColumnIdentityTest() throws Exception {
		// Read/Write table...
		try (final Store root = ArrowUtils.createArrowStore(OFFHEAP_SIZE, BATCH_SIZE, SCHEMAS)) {

			// Create writable table on store. Just an access on store.
			final WritablePartitionedTable writableTable = new WritablePartitionedTable(root);

			try (final WritableRow row = ColumnBackedWritableRow.fromWritableTable(writableTable)) {
				final WritableStringValue val0 = (WritableStringValue) row.getValueAccessAt(0);
				for (long i = 0; i < NUM_ROWS; i++) {
					row.fwd();
					val0.setStringValue("Entry " + i);
				}
			}

			// Done writing?
			final ReadablePartitionedTable readableTable = new ReadablePartitionedTable(root);

			try (final ReadableRow row = ColumnBackedReadableRow.fromReadableTable(readableTable)) {
				final ReadableStringValue val0 = (ReadableStringValue) row.getValueAccessAt(0);
				for (long i = 0; row.canFwd(); i++) {
					row.fwd();
					Assert.assertEquals("Entry " + i, val0.getStringValue());
				}
			}
		}
	}
}
/*
 * We can revisit this later. we're nearly done with an implementation which is
 * also suitable for streaming :-)
 */
//	@Test
//	public void readWhileWriteTest() throws Exception {
//
//		long NUM_ROWS = 100000;
//
//		// Read/Write table...
//		try (final ArrowStore store = createStore(NUM_ROWS);
//				final CachedColumnPartitionedTable table = new CachedColumnPartitionedTable(
//						new ColumnSchema[] { doubleVectorSchema }, store)) {
//
//			final Thread t1 = new Thread("Producer") {
//				public void run() {
//					// read AND write...
//					try (final WritableColumn column = table.getWritableColumn(0)) {
//						final WritableDoubleValueAccess value = (WritableDoubleValueAccess) column.getValueAccess();
//						for (long i = 0; i < NUM_ROWS; i++) {
//							column.fwd();
//							if (i % NUM_ROWS / 100 == 0) {
//								value.setMissing();
//							} else {
//								value.setDoubleValue(i);
//							}
//						}
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//				};
//			};
//
//			final Thread t2 = new Thread("Producer") {
//				public void run() {
//					// then read
//					try (final ReadableColumnCursor readableColumn = table.createReadableColumnCursor(0)) {
//						final ReadableDoubleValueAccess readableValue = (ReadableDoubleValueAccess) readableColumn
//								.getValueAccess();
//						for (long i = 0; readableColumn.canFwd(); i++) {
//							readableColumn.fwd();
//							if (i % NUM_ROWS / 100 == 0) {
//								Assert.assertTrue(readableValue.isMissing());
//							} else {
//								System.out.println(readableValue.getDoubleValue());
//								Assert.assertEquals(i, readableValue.getDoubleValue(), 0.0000001);
//							}
//						}
//					} catch (Exception e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				};
//			};
//
//			t1.run();
//			t2.run();
//
//		}
//	}

/*
 *
 * MOCKS TO MIMIC KNIME API
 *
 */
//	@Test
//	public void pushViaKNIMEAPI() throws IOException {
////		final DataTableSpec spec = null;
////		final DataContainer container = new DataContainer() {
////
////			private final WritableTable m_table;
////			{
////				final Store store = new ArrowStoreNewestOld(convert(spec));
////				m_table = new DefaultWritableTable(store);
////			}
////
////			private ColumnSchema[] convert(final DataTableSpec spec) {
////				return null;
////			}
////
////			@Override
////			public void addRowToTable(final DataRow row) {
////			}
////
////			@Override
////			public DataTableSpec getSpec() {
////				return null;
////			}
////		};
//	}
//
//	interface DataContainer {
//
//		void addRowToTable(DataRow row);
//
//		DataTableSpec getSpec();
//	}
//
//	interface DataTableSpec {
//
//	}
//
//	interface BufferedDataTable extends Iterable<DataRow> {
//
//	}
//
//	interface DataRow {
//
//		String getRowKey();
//
//		DataCell getCell(int i);
//
//		int numCells();
//
//		// TODO more stuff
//	}
//
//	class DataCell implements DataValue {
//
//	}
//
//	interface DataValue {
//
//	}
//
//	interface WritableDataValue {
//
//	}
//}
