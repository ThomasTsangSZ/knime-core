
package org.knime.core.data.store.tests;

import org.junit.Assert;
import org.junit.Test;
import org.knime.core.data.store.table.Row;
import org.knime.core.data.store.table.TableAccess;
import org.knime.core.data.store.table.TableAccessible;
import org.knime.core.data.store.table.TableAccessibleBounded;
import org.knime.core.data.store.table.TableFactory;
import org.knime.core.data.store.value.DoubleValue;
import org.knime.core.data.store.value.MutableDoubleValue;
import org.knime.core.data.store.vec.VecSchema;
import org.knime.core.data.store.vec.VecType;

public class StorageTest {

	private static final VecSchema singleDoubleVecSchema = new VecSchema() {

		@Override
		public int getNumVecs() {
			return 1;
		}

		@Override
		public VecType getVecTypeAt(final int i) {
			assert i == 0;
			return VecType.DOUBLE;
		}
	};

	@Test
	public void singleDoubleColumnReadWriteIdentityTest() throws Exception {
		// 1) Write some data
		final long numOutputRowsToCreate = 100_000_000;

		// Note: Casting entire access only works because we deal with a single
		// (double) column. TODO: How to do this in the case of multiple columns?
		// Retrieve mutable values once, cast them, and reuse their reference while
		// only using fwd to go through the table?
		final TableAccessible<MutableDoubleValue> wTaible = (TableAccessible<MutableDoubleValue>) TableFactory
			.createWritableTable(singleDoubleVecSchema);
		try (final TableAccess<MutableDoubleValue> wTa = wTaible.access()) {
			final Row<MutableDoubleValue> row = wTa.next();
			final MutableDoubleValue value = row.valueAt(0);
			// TODO: Remove hasNext from Write-API? Not needed, might be confusing.
			for (long i = 0; i < numOutputRowsToCreate && wTa.hasNext(); i++) {
				value.setDoubleValue(i);
				wTa.fwd();
			}
		}

		// 2) Read back in

		final TableAccessibleBounded<DoubleValue> rTaible = (TableAccessibleBounded<DoubleValue>) TableFactory
			.createReadableTable(singleDoubleVecSchema);
		try (final TableAccess<DoubleValue> rTa = rTaible.access()) {
			// TODO: Only works if table is not empty. Start at -1 instead, change
			// names ("row" -> "schema", "value" -> "column" or something like that),
			// and move "fwd" before "getDoubleValue"?
			final Row<DoubleValue> row = rTa.next();
			final DoubleValue value = row.valueAt(0);
			for (long i = 0; rTa.hasNext(); i++) {
				Assert.assertEquals(i, value.getDoubleValue(), 0.0000001);
				rTa.fwd();
			}
		}
	}
}
