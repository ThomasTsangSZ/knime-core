package org.knime.core.data.store.arrow;

import java.io.File;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.store.column.partition.ColumnPartition;

public class ArrowVectorFromDiskReader<T extends FieldVector> {

	public ArrowVectorFromDiskReader(File baseDir, Object object, BufferAllocator allocator) {
		// TODO Auto-generated constructor stub
	}

	public ColumnPartition<T> read(long m_idx) {
		// TODO Auto-generated method stub
		return null;
	}

}
