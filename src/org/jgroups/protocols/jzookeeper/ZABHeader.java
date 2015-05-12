package org.jgroups.protocols.jzookeeper;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Collection;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;


    public class ZABHeader extends Header {
         public static final byte REQUEST       = 1;
         public static final byte FORWARD       = 2;
         public static final byte PROPOSAL      = 3;
         public static final byte ACK           = 4;
         public static final byte COMMIT        = 5;
         public static final byte RESPONSE      = 6;
         public static final byte DELIVER       = 7;
         public static final byte START_SENDING = 8;
         public static final byte COMMITOUTSTANDINGREQUESTS = 9;
         public static final byte BUNDLED_MESSAGE = 10; // A message that contains multiple requests

         private Collection<MessageId> bundledMsgId = null;
         private byte        type=0;
         private long        seqno=0;
         private MessageId   messageId=null;

        public ZABHeader() {
        }

        public ZABHeader(byte type) {
            this.type=type;
        }
        public ZABHeader(byte type, MessageId id) {
            this.type=type;
            this.messageId=id;
        }

        public ZABHeader(byte type, long seqno) {
            this(type);
            this.seqno=seqno;
        }
        public ZABHeader(byte type, long seqno, MessageId messageId) {
            this(type);
            this.seqno=seqno;
            this.messageId=messageId;
        }
        public ZABHeader(byte type, Collection<MessageId> bundledMsgId) {
            this.type = type;
            this.bundledMsgId = bundledMsgId;
        }
    
        public byte getType() {
			return type;
		}       

        public Collection<MessageId> getBundledMsgId() {
            return bundledMsgId;
        }

		public void setType(byte type) {
			this.type = type;
		}

		public String toString() {
            StringBuilder sb=new StringBuilder(64);
            sb.append(printType());
            if(seqno >= 0)
                sb.append(" seqno=" + seqno);
            if(messageId!=null)
            	sb.append(", message_id=" + messageId);
            return sb.toString();
        }

        protected final String printType() {
        	
        	switch(type) {
            case REQUEST:        return "REQUEST";
            case FORWARD:        return "FORWARD";
            case PROPOSAL:       return "PROPOSAL";
            case ACK:            return "ACK";
            case COMMIT:         return "COMMIT";
            case RESPONSE:       return "RESPONSE";
            case DELIVER:        return "DELIVER";
            case START_SENDING:  return "START_SENDING";
            case COMMITOUTSTANDINGREQUESTS:  return "COMMITOUTSTANDINGREQUESTS";
            case BUNDLED_MESSAGE:       return "BUNDLED_MESSAGE";
            default:             return "n/a";
        }
           
        }
        
        public long getZxid() {
            return seqno;
        }
        
        public MessageId getMessageId(){
        	return messageId;
        }
        
        private int getBundledSize() {
            int size = 0;
            for (MessageId msgId : bundledMsgId)
                size += msgId.serializedSize();
            return size;
        }
        
       
        @Override
        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Bits.writeLong(seqno,out);
            Util.writeStreamable(messageId, out);
            writeBundledMsgId(bundledMsgId, out);
        }
        
        @Override
        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            seqno=Bits.readLong(in);
            messageId = (MessageId) Util.readStreamable(MessageId.class, in);
            bundledMsgId = readBundledMsgId(in);

        }
        
        @Override
        public int size() {
            return Global.BYTE_SIZE + Bits.size(seqno) + (messageId != null ? messageId.serializedSize(): 0) + Global.BYTE_SIZE + (bundledMsgId != null ? getBundledSize() : 0); 
         }
        
        private void writeBundledMsgId(Collection<MessageId> bundledMessageIds, DataOutput out) throws Exception{
            if (bundledMessageIds == null) {
                out.writeShort(-1);
                return;
            }

            out.writeShort(bundledMessageIds.size());
            for (MessageId id : bundledMessageIds)
                Util.writeStreamable(id, out);
        }

        private Collection<MessageId> readBundledMsgId(DataInput in) throws Exception {
            short length = in.readShort();
            if (length < 0) return null;

            Collection<MessageId> bundledMessageIds = new ArrayList<MessageId>();
            for (int i = 0; i < length; i++)
            	bundledMessageIds.add((MessageId) Util.readStreamable(MessageId.class, in));
            return bundledMessageIds;
        }
        
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ZABHeader that = (ZABHeader) o;

            if (type != that.type) return false;
            if (bundledMsgId != null ? !bundledMsgId.equals(that.bundledMsgId) : that.bundledMsgId != null)
                return false;
            if (messageId != null ? !messageId.equals(that.messageId) : that.messageId != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) type;
            result = 31 * result + (messageId != null ? messageId.hashCode() : 0);
            result = 31 * result + (bundledMsgId != null ? bundledMsgId.hashCode() : 0);
            return result;
        }

    }