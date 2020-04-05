
package org.knime.core.data.inmemory;

import org.knime.core.data.partition.Store;
import org.knime.core.data.table.column.ColumnSchema;

public final class NativeArraysUtils {

	private NativeArraysUtils() {
	}

	public static Store createInMemoryStore(int batchSize, ColumnSchema... schemas) {
		return new NativeArraysRootStore(batchSize, schemas);
	}
}
