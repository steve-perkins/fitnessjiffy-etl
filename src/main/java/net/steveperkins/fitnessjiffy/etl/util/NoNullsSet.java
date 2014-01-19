package net.steveperkins.fitnessjiffy.etl.util;

import java.util.Collection;
import java.util.HashSet;

public class NoNullsSet<E> extends HashSet<E> {

    @Override
    public boolean add(E element) {
        return (element != null) && super.add(element);
    }

    @Override
    public boolean addAll(Collection<? extends E> collection) {
        boolean setChanged = false;
        for(E element : collection) {
            if(add(element)) setChanged = true;
        }
        return setChanged;
    }

}
