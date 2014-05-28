package uk.ac.susx.tag.apt;

/**
 * RGraph represents a connected DAG where nodes are entities and edges are typed relations.
 * @author ds300
 */
public class RGraph {
    /**
     * A Relation represents a typed, directed relationship between two entities
     */
    public class Relation {
        public final int dependent;
        public final int governor;
        public final int type;

        public Relation (int dependent, int governor, int type) {
            if (type == 0)
                throw new RuntimeException("Relation type indices cannot be zero");
            else if (dependent < 0 || dependent >= entityIds.length)
                throw new IllegalArgumentException("Dependent IDs must be a valid index of the entityIds array");
            else if (governor < -1 || governor >= entityIds.length)
                throw new IllegalArgumentException("Governor IDs must be a valid index of the entityIds array or -1");
            this.dependent = dependent;
            this.governor = governor;
            this.type = type;
        }
    }

    /**
     * An array of the unique ids of the entities involved in this graph
     */
    public final int[] entityIds;
    public Relation[] relations;
    int numRelations = 0;

    /**
     * Constructs a new RGraph designed to hold {@code numEntities} entities
     * @param numEntities the number of entities that the graph constitutes
     */
    public RGraph (int numEntities) {
        entityIds = new int[numEntities];
        relations = new Relation[numEntities];
    }

    /**
     * Constructs a new RGraph with the given array of entity IDs.
     * @param entityIds the array of entityIDs
     */
    public RGraph (int[] entityIds) {
        this.entityIds = entityIds;
        relations = new Relation[entityIds.length];
    }

    /**
     * Adds a new relation to the graph
     * @param dependentIdx the index of the dependent entity in the {@code entityIds} array
     * @param governorIdx the index of the governor in the {@code entityIds array}, or -1 to signify an abstract root.
     * @param type the type of the relation. Must not be zero.
     */
    public void addRelation(int dependentIdx, int governorIdx, int type) {
        if (numRelations == relations.length) {
            Relation[] newRelations = new Relation[relations.length << 1];
            System.arraycopy(relations, 0, newRelations, 0, relations.length);
            relations = newRelations;
        }
        relations[numRelations++] = new Relation(dependentIdx, governorIdx, type);
    }

}
