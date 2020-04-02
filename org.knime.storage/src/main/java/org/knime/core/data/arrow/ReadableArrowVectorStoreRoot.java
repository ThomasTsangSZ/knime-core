
package org.knime.core.data.arrow;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.knime.core.data.table.column.NativeType;
import org.knime.core.data.vector.AbstractRefManaged;
import org.knime.core.data.vector.ReadableVectorStore;
import org.knime.core.data.vector.ReadableVectorStoreFactory;
import org.knime.core.data.vector.RefManaged;
import org.knime.core.data.vector.group.ReadableVectorStoreGroup;

// TODO add common subclass for Writable/Readble
public final class ReadableArrowVectorStoreRoot<V extends RefManaged> extends AbstractRefManaged
		implements ReadableVectorStoreFactory<Long> {

	private final File m_baseDirectory;

	private final Schema[] m_schema;

	private final BufferAllocator m_allocator;

	private final List<ReadableVectorStore<Long, ? extends V>> m_readableStores = new ArrayList<>();

	// TODO what's outside? what's inside?
	public ReadableArrowVectorStoreRoot() {
		m_baseDirectory = null;
		m_schema = null;
		m_allocator = null;
	}

	// TODO For some backends it may make sense to handle structs more efficiently
	// themselves. Can we let them decide how to handle structs?

	@Override
	public ReadableVectorStore<Long, ? extends V> createReadableStore(NativeType... types) {
		final ReadableVectorStore<Long, ? extends V> storeForType;
		if (types.length == 1) {
			storeForType = createGroupForType(types[0]);
		} else {
			@SuppressWarnings("unchecked")
			final ReadableVectorStore<Long, ? extends V>[] struct = new ReadableVectorStore[types.length];
			for (int i = 0; i < struct.length; i++) {
				struct[i] = createGroupForType(types[i]);
			}
			storeForType = new ReadableVectorStoreGroup<>(struct);
		}
		m_readableStores.add(storeForType);
		return storeForType;
	}

	@Override
	protected void onAllReferencesReleased() {
		// release own reference to all groups. groups will always exists as long as the
		// store exists.
		m_readableStores.forEach(g -> g.release());
	}

	private ReadableVectorStore<Long, ? extends V> createGroupForType(NativeType type) {
		// TODO Auto-generated method stub
		return null;
	}
}
