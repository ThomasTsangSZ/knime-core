package org.knime.core.data.store.arrow.table;

import java.io.IOException;

import org.apache.arrow.vector.ValueVector;
import org.knime.core.data.store.partition.ColumnPartitionReadableValueAccess;
import org.knime.core.data.store.partition.ColumnPartitionStore;
import org.knime.core.data.store.partition.ColumnPartitionWritableValueAccess;

public class ArrowColumnPartitionStore<V extends ValueVector> implements ColumnPartitionStore<V> {

	private V m_vector;

	public ArrowColumnPartitionStore(final V vector) {
		m_vector = vector;
	}

	@Override
	public long getNumPartitions() {
		return 0;
	}

	@Override
	public void close() throws Exception {

	}

	@Override
	public void persist(long partitionIndex) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public ArrowColumnPartition<V> getOrCreatePartition(long partitionIndex) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ColumnPartitionReadableValueAccess<V> createLinkedReadAccess() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ColumnPartitionWritableValueAccess<V> createLinkedWriteAccess() {
		// TODO Auto-generated method stub
		return null;
	}

}
