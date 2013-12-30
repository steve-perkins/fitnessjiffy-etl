package net.steveperkins.fitnessjiffy.data.util;

import com.google.common.collect.ForwardingSet;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class NoNullsSet<E> extends ForwardingSet<E> {

    final Set<E> delegate = new HashSet<E>();

    @Override
    protected Set<E> delegate() {
        return delegate;
    }

    @Override
    public boolean add(E element) {
        return (element != null) ? super.add(element) : false;
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
