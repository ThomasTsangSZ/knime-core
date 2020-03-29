
package org.knime.core.data.store.table.column;

public interface WritableColumn extends AutoCloseable {

	void fwd();

	void setMissing();
}
