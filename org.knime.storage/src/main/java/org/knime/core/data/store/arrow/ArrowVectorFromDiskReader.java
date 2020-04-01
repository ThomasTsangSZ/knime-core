package org.knime.core.data.store.arrow;

import java.io.File;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.store.column.partition.ColumnPartition;
import org.knime.core.data.store.column.partition.ColumnPartitionReader;

public class ArrowVectorFromDiskReader<V extends FieldVector> implements ColumnPartitionReader<V> {

	public ArrowVectorFromDiskReader(File baseDir, Object object, BufferAllocator allocator) {
		// TODO Auto-generated constructor stub
	}

	public ColumnPartition<V> read(long m_idx) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public ColumnPartition<V> readNext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void skip() {
		// TODO Auto-generated method stub

	}

}
