
package org.knime.core.data.store.table;

import java.util.ArrayList;
import java.util.List;

import org.knime.core.data.store.chunk.ChunkStore;
import org.knime.core.data.store.table.column.ReadableColumn;
import org.knime.core.data.store.table.column.ReadableTable;
import org.knime.core.data.store.table.column.WritableColumn;
import org.knime.core.data.store.table.column.WritableTable;
import org.knime.core.data.store.table.row.ReadableRowIterator;
import org.knime.core.data.store.table.row.WritableRowIterator;
import org.knime.core.data.store.vec.VecSchema;

public final class TableFactory {

	private TableFactory() {
	}

	public static WritableTable createWritableTable(final VecSchema... schema) {
		// TODO get store from somewhere
		final ChunkStore store = null;

		// TODO builder pattern or similar? :-)
		// return new WritableTable(new ChunkedVecWriteAccessible(new
		// CachedChunkStore(store)));
		throw new IllegalStateException("not yet implemented"); // TODO: implement
	}

	public static ReadableTable createReadableTable() {
		// TODO get store from somewhere
		// TODO Q: same store as in write case? A: at least shared cache as chunks
		// are
		// cached during writing and reader might be able to read from disc.
		final ChunkStore store = null;

		// TODO now we have a problem. We need the total size of the table (bounded
		// property of buffereddatatable).
		// During writing we don't have that info, because we don't know how large
		// the table will grow (we only may know our restriction on chunk sizes).
		throw new IllegalStateException("not yet implemented"); // TODO: implement
	}

	public static WritableRowIterator createRowWriteAccess(final WritableTable table) {
		final List<WritableColumn> columns = new ArrayList<>(Math.toIntExact(table.getNumColumns()));
		for (long i = 0; i < table.getNumColumns(); i++) {
			columns.add(table.getColumnAt(i));
		}
		return new WritableRowIterator(columns);
	}

	public static ReadableRowIterator createRowReadAccess(final ReadableTable table) {
		final List<ReadableColumn> columns = new ArrayList<>(Math.toIntExact(table.getNumColumns()));
		for (long i = 0; i < table.getNumColumns(); i++) {
			columns.add(table.getColumnAt(i));
		}
		return new ReadableRowIterator(columns);
	}
}
