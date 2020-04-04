package org.knime.core.data.arrow;

import org.knime.core.data.partition.PartitionStore;

public interface ArrowPartitionStore<F> extends PartitionStore<F> {
	void flush() throws Exception;
}
