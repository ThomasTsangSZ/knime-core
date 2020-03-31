
package org.knime.core.data.store.knime;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.knime.core.data.store.arrow.table.ArrowStoreNewestOld;
import org.knime.core.data.store.table.column.ColumnSchema;
import org.knime.core.data.store.table.column.ColumnType;
import org.knime.core.data.store.table.column.ReadableBufferTable;
import org.knime.core.data.store.table.column.ReadableColumnCursor;
import org.knime.core.data.store.table.column.ReadableTable;
import org.knime.core.data.store.table.column.Store;
import org.knime.core.data.store.table.column.DefaultWritableTable;
import org.knime.core.data.store.table.column.WritableColumn;
import org.knime.core.data.store.table.column.WritableTable;
import org.knime.core.data.store.table.row.ColumnBackedReadableRow;
import org.knime.core.data.store.table.row.ColumnBackedWritableRow;
import org.knime.core.data.store.table.row.ReadableRow;
import org.knime.core.data.store.table.row.WritableRow;
import org.knime.core.data.store.table.value.ReadableDoubleValueAccess;
import org.knime.core.data.store.table.value.WritableDoubleValueAccess;

public class StorageTest {

	private static final ColumnSchema doubleVectorSchema = () -> ColumnType.DOUBLE;

	@Test
	public void columnwiseWriteReadSingleDoubleColumnIdentityTest() throws Exception {
		final long numRows = 1000;
		final Store store = new ArrowStoreNewestOld(new ColumnSchema[] { doubleVectorSchema });

		try (final WritableTable writableTable = new DefaultWritableTable(store)) {
			final WritableColumn column = writableTable.getWritableColumn(0);
			final WritableDoubleValueAccess value = (WritableDoubleValueAccess) column.getValueAccess();
			for (long i = 0; i < numRows; i++) {
				column.fwd();
				if (i % numRows / 100 == 0) {
					value.setMissing();
				}
				else {
					value.setDoubleValue(i);
				}
			}
		}

		final ReadableTable readableTable = new ReadableBufferTable(store);
		try (final ReadableColumnCursor column = readableTable.createReadableColumnCursor(0)) {
			final ReadableDoubleValueAccess value = (ReadableDoubleValueAccess) column.getValueAccess();
			for (long i = 0; column.canFwd(); i++) {
				column.fwd();
				if (i % numRows / 100 == 0) {
					Assert.assertTrue(value.isMissing());
				}
				else {
					Assert.assertEquals(i, value.getDoubleValue(), 0.0000001);
				}
			}
		}
	}

	@Test
	public void rowwiseWriteReadSingleDoubleColumnIdentityTest() throws Exception {
		final long numRows = 1000;
		final Store store = new ArrowStoreNewestOld(new ColumnSchema[] { doubleVectorSchema });

		final WritableTable writableTable = new DefaultWritableTable(store);
		try (final WritableRow row = ColumnBackedWritableRow.fromWritableTable(writableTable)) {
			final WritableDoubleValueAccess value = (WritableDoubleValueAccess) row.getValueAccessAt(0);
			for (long i = 0; i < numRows; i++) {
				row.fwd();
				if (i % numRows / 100 == 0) {
					value.setMissing();
				}
				else {
					value.setDoubleValue(i);
				}
			}
		}

		final ReadableTable readableTable = new ReadableBufferTable(store);
		try (final ReadableRow row = ColumnBackedReadableRow.fromReadableTable(readableTable)) {
			final ReadableDoubleValueAccess value = (ReadableDoubleValueAccess) row.getValueAccessAt(0);
			for (long i = 0; row.canFwd(); i++) {
				row.fwd();
				if (i % numRows / 100 == 0) {
					Assert.assertTrue(value.isMissing());
				}
				else {
					Assert.assertEquals(i, value.getDoubleValue(), 0.0000001);
				}
			}
		}
	}

	/*
	 *
	 * MOCKS TO MIMIC KNIME API
	 *
	 */
	@Test
	public void pushViaKNIMEAPI() throws IOException {
		final DataTableSpec spec = null;
		final DataContainer container = new DataContainer() {

			private final WritableTable m_table;
			{
				final Store store = new ArrowStoreNewestOld(convert(spec));
				m_table = new DefaultWritableTable(store);
			}

			private ColumnSchema[] convert(final DataTableSpec spec) {
				return null;
			}

			@Override
			public void addRowToTable(final DataRow row) {}

			@Override
			public DataTableSpec getSpec() {
				return null;
			}
		};
	}

	interface DataContainer {

		void addRowToTable(DataRow row);

		DataTableSpec getSpec();
	}

	interface DataTableSpec {

	}

	interface BufferedDataTable extends Iterable<DataRow> {

	}

	interface DataRow {

		String getRowKey();

		DataCell getCell(int i);

		int numCells();

		// TODO more stuff
	}

	class DataCell implements DataValue {

	}

	interface DataValue {

	}

	interface WritableDataValue {

	}
}
