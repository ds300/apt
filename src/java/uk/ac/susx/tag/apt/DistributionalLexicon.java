package uk.ac.susx.tag.apt;


/**
 * @author ds300
 */
public interface DistributionalLexicon<EntityType, RelationType, APTType extends APT> extends APTStore<APTType> {
    BidirectionalIndexer<EntityType> getEntityIndex();
    BidirectionalIndexer<RelationType> getRelationIndex();


}
