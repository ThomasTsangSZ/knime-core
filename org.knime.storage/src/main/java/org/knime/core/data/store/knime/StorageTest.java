
package org.knime.core.data.store.knime;

import org.junit.Assert;
import org.junit.Test;
import org.knime.core.data.store.arrow.table.ArrowStore;
import org.knime.core.data.store.table.Store;
import org.knime.core.data.store.table.column.ColumnSchema;
import org.knime.core.data.store.table.column.ColumnType;
import org.knime.core.data.store.table.column.DefaultReadableTable;
import org.knime.core.data.store.table.column.DefaultWritableTable;
import org.knime.core.data.store.table.column.ReadableColumnIterator;
import org.knime.core.data.store.table.column.ReadableTable;
import org.knime.core.data.store.table.column.WritableColumn;
import org.knime.core.data.store.table.column.WritableTable;
import org.knime.core.data.store.table.row.ReadableRowIterator;
import org.knime.core.data.store.table.row.Row;
import org.knime.core.data.store.table.row.WritableRowIterator;
import org.knime.core.data.store.table.value.ReadableDoubleValueAccess;
import org.knime.core.data.store.table.value.ReadableValueAccess;
import org.knime.core.data.store.table.value.WritableDoubleValueAccess;
import org.knime.core.data.store.table.value.WritableValueAccess;

public class StorageTest {

	private static final ColumnSchema doubleVectorSchema = () -> ColumnType.DOUBLE;

	@Test
	public void columnwiseWriteReadSingleDoubleColumnIdentityTest() throws Exception {
		final long numRows = 1000;
		final Store store = new ArrowStore(new ColumnSchema[] { doubleVectorSchema });

		try (final WritableTable writableTable = new DefaultWritableTable(store)) {
			final WritableColumn column = writableTable.getColumnAt(0);
			final WritableDoubleValueAccess value = (WritableDoubleValueAccess) column.get();
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

		final ReadableTable readableTable = new DefaultReadableTable(store);
		try (ReadableColumnIterator column = readableTable.iterator(0)) {
			final ReadableDoubleValueAccess value = (ReadableDoubleValueAccess) column.get();
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

	/**
	 * Somewhat close to KNIME's original table API. The underlying implementation
	 * already uses row and data-value proxies, so we could pre-cast them and only
	 * fwd the iterators.<br>
	 * TODO: Actually do that.
	 */
	@Test
	public void rowwiseWriteReadSingleDoubleColumnIdentityTest() throws Exception {
		final long numRows = 1000;
		final Store store = new ArrowStore(new ColumnSchema[] { doubleVectorSchema });

		final WritableTable writableTable = new DefaultWritableTable(store);
		try (final WritableRowIterator iterator = TableFactory.createRowWriteAccess(writableTable)) {
			for (long i = 0; i < numRows; i++) {
				final Row<WritableValueAccess> row = iterator.next();
				final WritableDoubleValueAccess value = (WritableDoubleValueAccess) row.getValueAt(0);
				if (i % numRows / 100 == 0) {
					value.setMissing();
				}
				else {
					value.setDoubleValue(i);
				}
			}
		}

		final ReadableTable readableTable = new DefaultReadableTable(store);
		try (final ReadableRowIterator iterator = TableFactory.createRowReadAccess(readableTable)) {
			for (long i = 0; iterator.hasNext(); i++) {
				final Row<ReadableValueAccess> row = iterator.next();
				final ReadableDoubleValueAccess value = (ReadableDoubleValueAccess) row.getValueAt(0);
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
	public void pushViaKNIMEAPI() {
		final DataTableSpec spec = null;
		final DataContainer container = new DataContainer() {

			private final WritableTable m_table;
			{
				final Store store = new ArrowStore(convert(spec));
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
