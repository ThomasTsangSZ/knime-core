
package org.knime.core.data.store.arrow;

import org.apache.arrow.vector.ValueVector;

import io.netty.buffer.ArrowBuf;

public final class ArrowUtils {

	private ArrowUtils() {}

	public static void retainVector(final ValueVector vector) {
		for (final ArrowBuf buffer : vector.getBuffers(false)) {
			buffer.getReferenceManager().retain();
		}
	}

	public static void releaseVector(final ValueVector vector) {
		for (final ArrowBuf buffer : vector.getBuffers(false)) {
			buffer.getReferenceManager().release();
		}
	}
}
