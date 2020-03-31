
package org.knime.core.data.store.table.value;

/**
 * Base interface for proxies through which data values are written.
 */
public interface WritableValueAccess {

	void setMissing();

	void incIndex();
}
