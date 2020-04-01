
package org.knime.core.data.store.column.value;

/**
 * Base interface for proxies through which data values are written.
 */
public interface WritableValueAccess {

	void setMissing();
}
