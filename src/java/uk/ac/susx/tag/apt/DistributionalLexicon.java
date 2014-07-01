package uk.ac.susx.tag.apt;


import java.io.IOException;

/**
 * @author ds300
 */
public interface DistributionalLexicon<EntityType, RelationType, APTType extends APT> extends APTStore<APTType> {
    /**
     *
     * @return entity index
     */
    BidirectionalIndexer<EntityType> getEntityIndex();

    /**
     *
     * @return relation index
     */
    BidirectionalIndexer<RelationType> getRelationIndex();

    /**
     *
     * @return the total number of observations contributing to this lexicon.
     * A single observation being a record of the form (entity, relation-path, entity2, count)
     */
    Long getSum();


    /**
     * include an entire graph at once
     * @param rGraph
     * @throws IOException
     */
    void include(RGraph rGraph) throws IOException;
}

