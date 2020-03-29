
package org.knime.core.data.store.tests;

import org.junit.Assert;
import org.junit.Test;
import org.knime.core.data.store.table.TableFactory;
import org.knime.core.data.store.table.column.ReadableTable;
import org.knime.core.data.store.table.column.WritableTable;
import org.knime.core.data.store.table.column.impl.ReadableDoubleColumn;
import org.knime.core.data.store.table.column.impl.WritableDoubleColumn;
import org.knime.core.data.store.table.row.ReadableDataValue;
import org.knime.core.data.store.table.row.ReadableRowIterator;
import org.knime.core.data.store.table.row.Row;
import org.knime.core.data.store.table.row.WritableDataValue;
import org.knime.core.data.store.table.row.WritableRowIterator;
import org.knime.core.data.store.table.row.impl.ReadableDoubleValue;
import org.knime.core.data.store.table.row.impl.WritableDoubleValue;
import org.knime.core.data.store.vec.VecSchema;
import org.knime.core.data.store.vec.VecType;

public class StorageTest {

	private static final VecSchema doubleVectorSchema = () -> VecType.DOUBLE;

	@Test
	public void columnwiseWriteReadSingleDoubleColumnIdentityTest() throws Exception {
		final long numRows = 100_000_000;

		try (final WritableTable table = TableFactory.createWritableTable(doubleVectorSchema)) {
			try (final WritableDoubleColumn column = (WritableDoubleColumn) table.getColumnAt(0)) {
				for (long i = 0; i < numRows; i++) {
					column.fwd();
					if (i % numRows / 100 == 0) {
						column.setMissing();
					}
					else {
						column.setDoubleValue(i);
					}
				}
			}
		}

		// TODO: Somehow transfer underlying table store from writable table above
		// to readable table here.
		try (final ReadableTable table = TableFactory.createReadableTable()) {
			try (final ReadableDoubleColumn column = (ReadableDoubleColumn) table.getColumnAt(0)) {
				for (long i = 0; column.canFwd(); i++) {
					column.fwd();
					if (i % numRows / 100 == 0) {
						Assert.assertTrue(column.isMissing());
					}
					else {
						Assert.assertEquals(i, column.getDoubleValue(), 0.0000001);
					}
				}
			}
		}
	}

	/**
	 * Somewhat close to KNIME's original table API. The underlying implementation
	 * already uses row and data-value proxies, so we could pre-cast them and only
	 * fwd the iterators.
	 */
	@Test
	public void rowwiseWriteReadSingleDoubleColumnIdentityTest() throws Exception {
		final long numRows = 100_000_000;

		try (final WritableRowIterator iterator = //
			TableFactory.createRowwiseWriteAccess(TableFactory.createWritableTable(doubleVectorSchema)))
		{
			for (long i = 0; i < numRows; i++) {
				final Row<WritableDataValue> row = iterator.next();
				final WritableDoubleValue value = (WritableDoubleValue) row.getValueAt(0);
				if (i % numRows / 100 == 0) {
					value.setMissing();
				}
				else {
					value.setDoubleValue(i);
				}
			}
		}

		try (final ReadableRowIterator iterator = //
			// TODO: Somehow transfer underlying table store from writable table above
			// to readable table here.
			TableFactory.createRowwiseReadAccess(TableFactory.createReadableTable()))
		{
			for (long i = 0; iterator.hasNext(); i++) {
				final Row<ReadableDataValue> row = iterator.next();
				final ReadableDoubleValue value = (ReadableDoubleValue) row.getValueAt(0);
				if (i % numRows / 100 == 0) {
					Assert.assertTrue(value.isMissing());
				}
				else {
					Assert.assertEquals(i, value.getDoubleValue(), 0.0000001);
				}
			}
		}
	}
}
