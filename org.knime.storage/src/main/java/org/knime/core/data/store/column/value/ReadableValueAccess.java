
package org.knime.core.data.store.column.value;

/**
 * Base interface for proxies through which data values are read.
 */
public interface ReadableValueAccess {

	boolean isMissing();
}
