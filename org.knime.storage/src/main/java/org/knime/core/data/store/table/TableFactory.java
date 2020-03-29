package org.knime.core.data.store.table;

import org.knime.core.data.store.CachedChunkStore;
import org.knime.core.data.store.MutableValue;
import org.knime.core.data.store.chunk.ChunkStore;
import org.knime.core.data.store.chunk.ChunkedVecWriteAccessible;
import org.knime.core.data.store.vec.VecSchema;

public class TableFactory {

	public static TableAccessible<MutableValue> createWritableTable(VecSchema schema) {
		// TODO get store from somewhere
		ChunkStore store = null;
		// TODO builder pattern or similar? :-)
		return new DefaultTableAccessible<MutableValue>(new ChunkedVecWriteAccessible(new CachedChunkStore(store)));
	}

	public static TableAccessibleBounded<Value> createWritableTable(VecSchema schema) {
		// TODO get store from somewhere
		ChunkStore store = null;
		// TODO builder pattern or similar? :-)
		return new DefaultTableAccessible<MutableValue>(new ChunkedVecWriteAccessible(new CachedChunkStore(store)));
	}

}
