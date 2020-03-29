package org.knime.core.data.store.table;

import org.knime.core.data.store.CachedChunkStore;
import org.knime.core.data.store.MutableDataValue;
import org.knime.core.data.store.DataValue;
import org.knime.core.data.store.chunk.ChunkStore;
import org.knime.core.data.store.chunk.ChunkedVecWriteAccessible;
import org.knime.core.data.store.vec.VecSchema;

public class TableFactory {

	public static TableAccessible<MutableDataValue> createWritableTable(VecSchema schema) {
		// TODO get store from somewhere
		ChunkStore store = null;

		// TODO builder pattern or similar? :-)
		return new DefaultTableAccessible(new ChunkedVecWriteAccessible(new CachedChunkStore(store)));
	}

	public static TableAccessibleBounded<DataValue> createReadableTable(VecSchema schema) {
		// TODO get store from somewhere
		// TODO Q: same store as in write case? A: at least shared cache as chunks are
		// cached during writing and reader might be able to read from disc.
		ChunkStore store = null;

		// TODO now we have a problem. We need the total size of the table (bounded
		// property of buffereddatatable).
		// During writing we don't have that info, because we don't know how large the table will grow (we only may know our restriction on chunk sizes).
		return null;
	}
}
