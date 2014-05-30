package uk.ac.susx.tag.apt.construct;

import uk.ac.susx.tag.apt.APT;
import uk.ac.susx.tag.apt.APTFactory;
import uk.ac.susx.tag.apt.DistributionalLexicon;
import uk.ac.susx.tag.apt.store.APTStore;

import java.io.IOException;

/**
 * @author ds300
 */
public class AccumulativeDistributionalLexicon implements DistributionalLexicon<AccumulativeLazyAPT> {

    final APTStore<AccumulativeLazyAPT> store;
    final APTFactory<AccumulativeLazyAPT> factory = new AccumulativeLazyAPT.Factory();
    final int depth;

    public AccumulativeDistributionalLexicon(APTStore.Builder storeBuilder, int depth) {
        this.store = storeBuilder.build(factory);
        this.depth = depth;
    }

    @Override
    public void include(int entityId, APT apt) throws IOException {
        AccumulativeLazyAPT existing = store.get(entityId);
        existing.merge(apt, depth, 0);
        store.put(entityId, existing);
    }

    @Override
    public AccumulativeLazyAPT getAPT(int entityId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replace(int entityId, APT apt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(int entityId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        store.close();
    }
}
