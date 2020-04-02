
package org.knime.core.data.table.column;

import java.io.IOException;

// TODO would it help us to make use of the sequential character of the data?
public interface ReadablePartitionedColumn<D> extends PartitionedColumn<D> {

	/**
	 * @return a <O> managed by the store. Either a new object or an existing one.
	 *         Increases ref counter before return.
	 * @throws IOException
	 */
	Partition<D> get(long idx) throws IOException;

	/**
	 * @return number of vectors in this store
	 */
	long getNumPartitions();

}
