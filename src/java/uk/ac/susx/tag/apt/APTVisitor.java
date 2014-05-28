package uk.ac.susx.tag.apt;

/**
 * @author ds300
 */
public interface APTVisitor {
    void visit(int[] path, APT APT);
}
