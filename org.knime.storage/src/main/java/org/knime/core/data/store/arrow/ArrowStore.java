package org.knime.core.data.store.arrow;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.arrow.memory.RootAllocator;
import org.knime.core.data.store.VecGroupSchema;
import org.knime.core.data.store.ChunkStore;

// TODO maybe we want to actually split into read/write later.
public class ArrowStore implements ChunkStore<ArrowInMemoryBatch> {

	private ArrowVecFactory m_factory;
	private VecGroupSchema m_spec;
	private File m_dest;
	private RootAllocator m_root;
	private int m_numChunks;

	public ArrowStore(File dest, final VecGroupSchema spec, final long limit) {
		// TODO add allocation listener for this store.
		// TODO likely we need one central allocator for ALL tables/stores
		m_root = new RootAllocator(limit);
		
		// TODO where is the batch-size coming from?
		m_factory = new ArrowVecFactory(2048, m_root);
		m_spec = spec;
		m_dest = dest;
	}

	@Override
	public void persist(ArrowInMemoryBatch batch) {
		// TODO write to disc.
		// TODO one file per batch. maybe physical batch size != logical (CPU cache etc)
		
		// NB: this method can also be called during reading (i.e. when a chunk has been cached before).
	}

	@Override
	public ArrowInMemoryBatch load(long idx) {
		// TODO load from disc
		return null;
	}

	@Override
	public ArrowInMemoryBatch createNext() {
		// TODO in the KNIME case this method will never be called after load(long idx) has been called.
		// TODO however in general - we can create new batches while reading - this is possible... undecided what to do. use-case: streaming.
		
		m_numChunks++;
		// batc to be closed by caller
		return new ArrowInMemoryBatch(m_factory, m_spec);
	}

	@Override
	public void destroy() {
		// Assumption: we never, ever keep any references on batches, so all references
		// should have been closed externally.
		// however as a fallback we could simply kill the root allocator for this store.
		// TODO check if this is correct, in case someone is still "accessing the batch"
//		m_root.close();
		try {
			Files.delete(m_dest.toPath());
		} catch (IOException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	@Override
	public long numChunks() {
		return m_numChunks;
	}

}
