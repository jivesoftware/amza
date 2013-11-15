package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.transport.tcp.replication.client.ClientChannelProvider;
import com.jivesoftware.os.amza.transport.tcp.replication.client.TCPChangeSetSender;
import com.jivesoftware.os.amza.transport.tcp.replication.client.TCPChangeSetTaker;
import de.ruedigermoeller.serialization.FSTConfiguration;

/**
 *
 */
public class ExampleMain {
    
    public static void main(String[] args) {
        int bufferSize = 10 * 1024;
        
        BufferProvider bufferProvider = new BufferProvider(bufferSize, 10);
        
        ClientChannelProvider clientChannelProvider = new ClientChannelProvider(50000, 2000, bufferSize, bufferSize,
            new MessageFramer(), 
            bufferProvider);
        
        FstMarshaller marshaller = new FstMarshaller(FSTConfiguration.getDefaultConfiguration());
        TCPChangeSetTaker taker = new TCPChangeSetTaker(clientChannelProvider, bufferProvider, marshaller);
        
        TCPChangeSetSender sender = new TCPChangeSetSender(clientChannelProvider, marshaller);
        
        //blah blah blah take, send
        
        clientChannelProvider.closeAll();
        
        
    }
    
}
