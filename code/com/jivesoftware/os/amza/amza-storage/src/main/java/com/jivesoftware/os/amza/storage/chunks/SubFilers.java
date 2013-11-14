package com.jivesoftware.os.amza.storage.chunks;


/*
 This class segments a single Filer into filerResource filers where
 each filerResource filer restates fp = 0. It only allows one filerResource filer
 at a time to be in control. It is the responsibility of the
 programmer to remove the filerResource filers as the become stale.
 */
public class SubFilers extends ASetObject {

    final static SoftIndex<SubFiler, OrderedKeys, Object> subFilers = new SoftIndex<>("Sub Filers Index");
    Object name;
    IFiler filer;

    public SubFilers(Object _name, IFiler _filer) {
        name = _name;
        filer = _filer;
    }

    @Override
    public Object hashObject() {
        return filer;
    }

    @Override
    public String toString() {
        return "SubFilers:" + name;
    }

    public SubFiler get(long _startOfFP, long _endOfFP, long _count) {
        SubFiler subFiler;
        synchronized (subFilers) {
            OrderedKeys key = key(this, _startOfFP, _endOfFP);
            subFiler = subFilers.get(key);
            if (subFiler == null) {
                subFiler = new SubFiler(this, _startOfFP, _endOfFP, _count);
                subFilers.set(subFiler, key);
            }
        }
        return subFiler;

    }

    public static OrderedKeys key(SubFilers _sfs, long _startOfFP, long _endOfFP) {
        try {
            return new OrderedKeys(_sfs, _startOfFP, _endOfFP);
        } catch (Exception x) {
            throw new RuntimeException(x);
        }
    }
}