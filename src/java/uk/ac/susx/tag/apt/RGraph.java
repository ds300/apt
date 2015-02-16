package uk.ac.susx.tag.apt;

import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import org.eclipse.jetty.setuid.RLimit;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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

    // STATIC DATA STRUCTURES FOR TOPOLOGICAL SORT
    // REASON: GOOD LOCALITIES AND LESS GARBAGES MAKES FASTER PROGRAMS
    private static ThreadLocal<boolean[]> participatories = new ThreadLocal<>();

    private boolean[] getParticipatoryArray() {
        boolean[] a = participatories.get();
        if (a == null || a.length < entityIds.length) {
            a = new boolean[entityIds.length << 1];
            participatories.set(a);
        } else {
            Arrays.fill(a, false);
        }
        return a;
    }

    private static ThreadLocal<ObjectArraySet<Relation>> edgeses = new ThreadLocal<>();

    private ObjectArraySet<Relation> getEdgesSet () {
        ObjectArraySet<Relation> s = edgeses.get();
        if (s == null) {
            s = new ObjectArraySet<>(numRelations << 1);
            edgeses.set(s);
        } else {
            s.clear();
        }
        return s;
    }

    private static class intarrayset {
        int[] ints;
        int offset = 0;
        intarrayset(int n) {
            ints = new int[n];
        }
        void add(int a) {
            for (int i=0; i<offset; i++) {
                if (ints[i] == a) return;
            }
            ints[offset++] = a;
        }
        void remove(int a) {
            for (int i=0; i<offset; i++) {
                if (ints[i] == a) {
                    if (i < offset - 1) {
                        ints[i] = ints[offset-1];
                    }
                    offset--;
                    return;
                }
            }
        }
        int removeAndGet() {
            if (offset > 0) {
                offset--;
                return ints[offset];
            } else {
                throw new IllegalStateException("can't remove from empty set");
            }
        }
        void clear() {
            offset = 0;
        }
        boolean isEmpty () {
            return offset == 0;
        }
    }

    private static ThreadLocal<intarrayset> nodeses = new ThreadLocal<>();

    private intarrayset getNodesSet () {
        intarrayset s = nodeses.get();
        if (s == null) {
            s = new intarrayset(entityIds.length << 1);
            nodeses.set(s);
        } else {
            s.clear();
        }
        return s;
    }

    /**
     * @return topologically sorted indices into entityIds array, removes indices that do not participate in relations
     */
    public int[] sorted () {
        boolean[] paricipatory = getParticipatoryArray();
        int numParicipatory = 0;
        ObjectArraySet<Relation> edges = getEdgesSet();
        for (Relation r : relations) {
            if (r == null) break;
            edges.add(r);
            if (!paricipatory[r.dependent]) {
                numParicipatory++;
                paricipatory[r.dependent] = true;
            }
            if (r.governor >= 0 && !paricipatory[r.governor]) {
                numParicipatory++;
                paricipatory[r.governor] = true;
            }
        }
//      L ← Empty list that will contain the sorted elements
        int[] L = new int[numParicipatory];
        int Li = 0;

//      S ← Set of all nodes with no incoming edges
        intarrayset S = getNodesSet();
        for (int i=0;i<entityIds.length;i++) {
            if (paricipatory[i]) {
                S.add(i);
            }
        }
        final int nr = numRelations;
        for (int i=0; i<nr; i++) {
            Relation r = relations[i];
            S.remove(r.dependent);
            if (r.governor < 0) S.add(r.governor);
        }
//      while S is non-empty do
        while (!S.isEmpty()) {
//          remove a node n from S
            int n = S.removeAndGet();
//          add n to tail of L
            if (n >= 0 && paricipatory[n]) L[Li++] = n;
//          for each node m with an edge e from n to m do
            outer: for (Relation e : new HashSet<>(edges)) {
                if (e.governor == n) {
//                  remove edge e from the graph
                    edges.remove(e);
//                  if m has no other incoming edges then insert m into S
                    for (Relation r : edges) if (r.dependent == e.dependent) continue outer;
                    S.add(e.dependent);
                }
            }
        }
//      if graph has edges then
        if (!edges.isEmpty()) {
//          return error (graph has at least one cycle)
            throw new RuntimeException("Cyclic RGraph detected.");
        }
//      else
//      return L (a topologically sorted order)
        return L;
    }

}
