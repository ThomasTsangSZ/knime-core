package org.knime.core.data.store.arrow.table;

import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.store.partition.ColumnPartition;
import org.knime.core.data.store.partition.ColumnPartitionReadableValueAccess;
import org.knime.core.data.store.partition.ColumnPartitionStore;
import org.knime.core.data.store.partition.ColumnPartitionWritableValueAccess;

public class ArrowColumnPartitionStore<V extends FieldVector> implements ColumnPartitionStore {

	public ArrowColumnPartitionStore(V ) {

	}

	@Override
	public long getNumPartitions() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ColumnPartition getOrCreatePartition(long partitionIndex) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ColumnPartitionReadableValueAccess getReadAccess() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ColumnPartitionWritableValueAccess getWriteAccess() {
		// TODO Auto-generated method stub
		return null;
	}

}
