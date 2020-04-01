
package org.knime.core.data.store.knime;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;
import org.knime.core.data.store.arrow.ArrowStore;
import org.knime.core.data.store.column.ColumnSchema;
import org.knime.core.data.store.column.ColumnType;
import org.knime.core.data.store.column.ReadableColumnCursor;
import org.knime.core.data.store.column.WritableColumn;
import org.knime.core.data.store.column.partition.DefaultPartitionedColumnsTable;
import org.knime.core.data.store.column.value.ReadableDoubleValueAccess;
import org.knime.core.data.store.column.value.WritableDoubleValueAccess;
import org.knime.core.data.store.table.row.ColumnBackedReadableRow;
import org.knime.core.data.store.table.row.ColumnBackedWritableRow;
import org.knime.core.data.store.table.row.ReadableRow;
import org.knime.core.data.store.table.row.WritableRow;

import com.google.common.io.Files;

public class StorageTest {

	// in numValues per vector
	private static final int BATCH_SIZE = 10_000_000;

	// in bytes
	private static final long OFFHEAP_SIZE = 2048_000_000;
	private static final ColumnSchema doubleVectorSchema = () -> ColumnType.DOUBLE;

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
	public void columnwiseWriteReadSingleDoubleColumnIdentityTest() throws Exception {
		final long numRows = 100_000_000;

		for (int z = 0; z < 10; z++) {
			try (final DefaultPartitionedColumnsTable table = new DefaultPartitionedColumnsTable(
					new ColumnSchema[] { doubleVectorSchema }, createStore(numRows))) {

				long time = System.currentTimeMillis();
				// first write
				try (final WritableColumn column = table.getWritableColumn(0)) {
					final WritableDoubleValueAccess value = (WritableDoubleValueAccess) column.getValueAccess();
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
				try (final ReadableColumnCursor readableColumn = table.getReadableColumn(0).cursor()) {
					final ReadableDoubleValueAccess readableValue = (ReadableDoubleValueAccess) readableColumn
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
				System.out.println((System.currentTimeMillis() - time));
			}
		}
	}

	@Test
	public void rowwiseWriteReadSingleDoubleColumnIdentityTest() throws Exception {
		final long numRows = 128;

		// Read/Write table...
		try (final DefaultPartitionedColumnsTable table = new DefaultPartitionedColumnsTable(
				new ColumnSchema[] { doubleVectorSchema }, createStore(numRows))) {

			try (final WritableRow row = ColumnBackedWritableRow.fromWritableTable(table)) {
				final WritableDoubleValueAccess value = (WritableDoubleValueAccess) row.getValueAccessAt(0);
				for (long i = 0; i < numRows; i++) {
					row.fwd();
					if (i % numRows / 100 == 0) {
						value.setMissing();
					} else {
						value.setDoubleValue(i);
					}
				}
			}

			try (final ReadableRow row = ColumnBackedReadableRow.fromReadableTable(table)) {
				final ReadableDoubleValueAccess value = (ReadableDoubleValueAccess) row.getValueAccessAt(0);
				for (long i = 0; row.canFwd(); i++) {
					row.fwd();
					if (i % numRows / 100 == 0) {
						Assert.assertTrue(value.isMissing());
					} else {
						Assert.assertEquals(i, value.getDoubleValue(), 0.0000001);
					}
				}
			}
		}
	}

	private ArrowStore createStore(long numRows) {
		final File baseDir = Files.createTempDir();
		baseDir.deleteOnExit();
		return new ArrowStore(baseDir, BATCH_SIZE, OFFHEAP_SIZE);
	}
}
/*
 * We can revisit this later. we're nearly done with an implementation which is
 * also suitable for streaming :-)
 */
//	@Test
//	public void readWhileWriteTest() throws Exception {
//
//		long numRows = 100000;
//
//		// Read/Write table...
//		try (final ArrowStore store = createStore(numRows);
//				final CachedColumnPartitionedTable table = new CachedColumnPartitionedTable(
//						new ColumnSchema[] { doubleVectorSchema }, store)) {
//
//			final Thread t1 = new Thread("Producer") {
//				public void run() {
//					// read AND write...
//					try (final WritableColumn column = table.getWritableColumn(0)) {
//						final WritableDoubleValueAccess value = (WritableDoubleValueAccess) column.getValueAccess();
//						for (long i = 0; i < numRows; i++) {
//							column.fwd();
//							if (i % numRows / 100 == 0) {
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
//							if (i % numRows / 100 == 0) {
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
