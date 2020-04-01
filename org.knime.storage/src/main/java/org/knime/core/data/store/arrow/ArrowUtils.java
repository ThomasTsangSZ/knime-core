
package org.knime.core.data.store.arrow;

import java.io.File;
import java.io.IOException;

import org.apache.arrow.vector.ValueVector;
import org.knime.core.data.store.column.ColumnSchema;

import com.google.common.io.Files;

import io.netty.buffer.ArrowBuf;

public final class ArrowUtils {

	private ArrowUtils() {
	}

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

	public static ArrowTable createArrowTable(int batchSize, long offHeapSize, ColumnSchema[] schemas)
			throws IOException {
		final File baseDir = Files.createTempDir();
		baseDir.deleteOnExit();
		return new ArrowTable(schemas, new ArrowStore(baseDir, batchSize, offHeapSize));
	}
}
