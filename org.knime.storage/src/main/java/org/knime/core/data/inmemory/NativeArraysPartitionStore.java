package org.knime.core.data.inmemory;

import org.knime.core.data.partition.PartitionStore;

public interface NativeArraysPartitionStore<F> extends PartitionStore<F> {
	void flush() throws Exception;
}
