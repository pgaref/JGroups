package org.jgroups.protocols.relay;

import org.jgroups.util.AsciiString;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.util.Arrays;

/**
 * Implementation of SiteAddress
 * @author Bela Ban
 * @since 3.2
 */
public class SiteUUID extends ExtendedUUID implements SiteAddress {
    protected static final byte[] NAME      = Util.stringToBytes("name"); // logical name, can be null
    protected static final byte[] SITE_NAME = Util.stringToBytes("site");
    private static final long     serialVersionUID=7128439052905502361L;


    public SiteUUID() {
    }

    public SiteUUID(long mostSigBits, long leastSigBits, String name, String site) {
        super(mostSigBits,leastSigBits);
        if(name != null)
            put(NAME, Util.stringToBytes(name));
        put(SITE_NAME, Util.stringToBytes(site));
    }

    public SiteUUID(long mostSigBits, long leastSigBits, byte[] name, byte[] site) {
        super(mostSigBits,leastSigBits);
        if(name != null)
            put(NAME, name);
        put(SITE_NAME, site);
    }

    public SiteUUID(UUID uuid, String name, String site) {
        super(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        if(name != null)
            put(NAME, Util.stringToBytes(name));
        put(SITE_NAME, Util.stringToBytes(site));
    }

    public String getName() {
        return Util.bytesToString(get(NAME));
    }

    public String getSite() {
        return Util.bytesToString(get(SITE_NAME));
    }

    public UUID copy() {
        return new SiteUUID(mostSigBits, leastSigBits, get(NAME), get(SITE_NAME));
    }

    public String toString() {
        String name=getName();
        String retval=name != null? name : get(this);
        return retval + ":" + getSite() + printOthers();
    }

    protected String printOthers() {
        StringBuilder sb=new StringBuilder();
        if(flags != 0)
            sb.append(" flags=" + flags + " (" + flagsToString() + ")");
        if(keys == null)
            return sb.toString();
        for(int i=0; i < keys.length; i++) {
            byte[] key=keys[i];
            if(key == null || Arrays.equals(key,SITE_NAME) || Arrays.equals(key, NAME))
                continue;
            byte[] val=values[i];
            Object obj=null;
            try {
                obj=Util.objectFromByteBuffer(val);
            }
            catch(Throwable t) {
            }
            if(obj == null) {
                try {
                    obj=Util.bytesToString(val);
                }
                catch(Throwable t) {
                    obj=val != null? val.length + " bytes" : null;
                }
            }

            sb.append(", ").append(new AsciiString(key)).append("=").append(obj);
        }
        return sb.toString();
    }
}
