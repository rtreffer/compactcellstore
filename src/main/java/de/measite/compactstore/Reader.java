package de.measite.compactstore;

public interface Reader {

    public Object[] get(Object ... key);

    public Object[][] getAll(Object ... key);

}
