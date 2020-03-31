
package org.knime.core.data.store.knime;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.knime.core.data.store.Store;
import org.knime.core.data.store.arrow.table.ArrowStore;
import org.knime.core.data.store.partition.CachedColumnPartitionedTable;
import org.knime.core.data.store.table.column.ColumnSchema;
import org.knime.core.data.store.table.column.ColumnType;
import org.knime.core.data.store.table.column.ReadableColumnCursor;
import org.knime.core.data.store.table.column.WritableColumn;
import org.knime.core.data.store.table.row.ColumnBackedReadableRow;
import org.knime.core.data.store.table.row.ColumnBackedWritableRow;
import org.knime.core.data.store.table.row.ReadableRow;
import org.knime.core.data.store.table.row.WritableRow;
import org.knime.core.data.store.table.value.ReadableDoubleValueAccess;
import org.knime.core.data.store.table.value.WritableDoubleValueAccess;

import com.google.common.io.Files;

public class StorageTest {

//	 in numValues per vector
	private static final int BATCH_SIZE = 512;

	// in bytes
	private static final long OFFHEAP_SIZE = 512_000_000;
	private static final ColumnSchema doubleVectorSchema = () -> ColumnType.DOUBLE;

	@Test
	public void columnwiseWriteReadSingleDoubleColumnIdentityTest() throws Exception {
		final File baseDir = Files.createTempDir();
		baseDir.deleteOnExit();
		final long numRows = 1000;
		final Store store = new ArrowStore(baseDir, BATCH_SIZE, OFFHEAP_SIZE);

		// Read/Write table...
		try (final CachedColumnPartitionedTable table = new CachedColumnPartitionedTable(
				new ColumnSchema[] { doubleVectorSchema }, store)) {

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
			try (final ReadableColumnCursor readableColumn = table.createReadableColumnCursor(0)) {
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
		}
	}

	@Test
	public void rowwiseWriteReadSingleDoubleColumnIdentityTest() throws Exception {
		final File baseDir = Files.createTempDir();
		baseDir.deleteOnExit();
		final long numRows = 1000;
		final Store store = new ArrowStore(baseDir, BATCH_SIZE, OFFHEAP_SIZE);

		// Read/Write table...
		try (final CachedColumnPartitionedTable table = new CachedColumnPartitionedTable(
				new ColumnSchema[] { doubleVectorSchema }, store)) {

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
