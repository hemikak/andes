/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.server.information.management;

import com.gs.collections.api.iterator.MutableLongIterator;
import com.gs.collections.impl.list.mutable.primitive.LongArrayList;
import com.gs.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.AndesUtils;
import org.wso2.andes.kernel.DestinationType;
import org.wso2.andes.kernel.DisablePubAckImpl;
import org.wso2.andes.kernel.FlowControlListener;
import org.wso2.andes.management.common.mbeans.DLCManagementInformation;
import org.wso2.andes.management.common.mbeans.SubscriptionManagementInformation;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.management.AMQManagedObject;

import java.util.ArrayList;
import java.util.List;
import javax.management.NotCompliantMBeanException;

/**
 * MBeans for managing resources related to dead letter channel.
 */
public class DLCManagementInformationMBean extends AMQManagedObject implements DLCManagementInformation {
    /**
     * AndesChannel for this dead letter channel restore which implements flow control.
     */
    AndesChannel andesChannel;

    /**
     * Publisher Acknowledgements are disabled for this MBean hence using DisablePubAckImpl to drop any pub ack request
     * by Andes.
     */
    private final DisablePubAckImpl disablePubAck;

    /**
     * The message restore flowcontrol blocking state. If true message restore will be interrupted from dead letter
     * channel.
     */
    boolean restoreBlockedByFlowControl = false;

    /**
     * Initializes dlc management MBean
     *
     * @throws NotCompliantMBeanException
     * @throws AndesException
     */
    public DLCManagementInformationMBean() throws NotCompliantMBeanException, AndesException {
        super(DLCManagementInformation.class, DLCManagementInformation.TYPE);
        andesChannel = Andes.getInstance().createChannel(new FlowControlListener() {
            @Override
            public void block() {
                restoreBlockedByFlowControl = true;
            }

            @Override
            public void unblock() {
                restoreBlockedByFlowControl = false;
            }

            @Override
            public void disconnect(){
                // Do nothing. since its not applicable.
            }
        });

        disablePubAck = new DisablePubAckImpl();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessagesFromDeadLetterQueue(
            @MBeanOperationParameter(name = "andesMetadataIDs", description = "ID of the Messages to Be Deleted")
            long[] andesMetadataIDs,
            @MBeanOperationParameter(name = "destinationQueueName", description = "The Dead Letter Queue Name for the" +
                                                                                  " selected tenant")
            String destinationQueueName) {
        List<AndesMessageMetadata> messageMetadataList = new ArrayList<>(andesMetadataIDs.length);

        for (long andesMetadataID : andesMetadataIDs) {
            AndesMessageMetadata messageToRemove = new AndesMessageMetadata(andesMetadataID, null, false);
            messageToRemove.setStorageQueueName(destinationQueueName);
            messageToRemove.setDestination(destinationQueueName);
            messageMetadataList.add(messageToRemove);
        }

        // Deleting messages which are in the list.
        try {
            Andes.getInstance().deleteMessagesFromDLC(messageMetadataList);
        } catch (AndesException e) {
            throw new RuntimeException("Error deleting messages from Dead Letter Channel", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restoreMessagesFromDeadLetterQueue(
            @MBeanOperationParameter(name = "andesMetadataIDs", description = "IDs of the Messages to Be Restored")
            long[] andesMetadataIDs,
            @MBeanOperationParameter(name = "destinationQueueName", description = "The Dead Letter Queue Name for " +
                                                                                  "the selected tenant")
            String destinationQueueName) {
        if (null != andesMetadataIDs) {
            LongArrayList andesMessageIdList = new LongArrayList(andesMetadataIDs.length);
            andesMessageIdList.addAll(andesMetadataIDs);
            List<AndesMessageMetadata> messagesToRemove = new ArrayList<>(andesMessageIdList.size());

            try {
                LongObjectHashMap<List<AndesMessagePart>> messageContent = Andes.getInstance()
                        .getContent(andesMessageIdList);

                boolean interruptedByFlowControl = false;

                MutableLongIterator iterator = andesMessageIdList.longIterator();

                while (iterator.hasNext()) {
                    long messageId = iterator.next();
                    if (restoreBlockedByFlowControl) {
                        interruptedByFlowControl = true;
                        break;
                    }
                    AndesMessageMetadata metadata = Andes.getInstance().getMessageMetaData(messageId);
                    String destination = metadata.getDestination();

                    metadata.setStorageQueueName(AndesUtils.getStorageQueueForDestination(destination,
                            ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID(),
                            DestinationType.QUEUE));

                    messagesToRemove.add(metadata);

                    AndesMessageMetadata clonedMetadata = metadata.shallowCopy(metadata.getMessageID());
                    AndesMessage andesMessage = new AndesMessage(clonedMetadata);

                    // Update Andes message with all the chunk details
                    List<AndesMessagePart> messageParts = messageContent.get(messageId);
                    messageParts.forEach(andesMessage::addMessagePart);

                    // Handover message to Andes. This will generate a new message ID and store it
                    Andes.getInstance().messageReceived(andesMessage, andesChannel, disablePubAck);
                }

                // Delete old messages
                Andes.getInstance().deleteMessagesFromDLC(messagesToRemove);

                if (interruptedByFlowControl) {
                    // Throw this out so UI will show this to the user as an error message.
                    throw new RuntimeException("Message restore from dead letter queue has been interrupted by flow "
                                               + "control. Please try again later.");
                }

            } catch (AndesException e) {
                throw new RuntimeException("Error restoring messages from " + destinationQueueName, e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restoreMessagesFromDeadLetterQueue(
            @MBeanOperationParameter(name = "andesMetadataIDs", description = "IDs of the Messages to Be Restored")
            long[] andesMetadataIDs,
            @MBeanOperationParameter(name = "destination", description = "Destination of the message to be restored")
            String newDestinationQueueName,
            @MBeanOperationParameter(name = "deadLetterQueueName", description = "The Dead Letter Queue Name for the " +
                                                                                 "selected tenant")
            String destinationQueueName) {
        if (null != andesMetadataIDs) {

            LongArrayList andesMessageIdList = new LongArrayList(andesMetadataIDs.length);
            andesMessageIdList.addAll(andesMetadataIDs);
            List<AndesMessageMetadata> messagesToRemove = new ArrayList<>(andesMessageIdList.size());

            try {
                LongObjectHashMap<List<AndesMessagePart>> messageContent = Andes.getInstance()
                        .getContent(andesMessageIdList);

                boolean interruptedByFlowControl = false;

                MutableLongIterator iterator = andesMessageIdList.longIterator();
                while (iterator.hasNext()) {

                    long messageId = iterator.next();
                    if (restoreBlockedByFlowControl) {
                        interruptedByFlowControl = true;
                        break;
                    }

                    AndesMessageMetadata metadata = Andes.getInstance().getMessageMetaData(messageId);

                    // Set the new destination queue
                    metadata.setDestination(newDestinationQueueName);
                    metadata.setStorageQueueName(AndesUtils.getStorageQueueForDestination(newDestinationQueueName,
                            ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID(),
                            DestinationType.QUEUE));

                    metadata.updateMetadata(newDestinationQueueName, AMQPUtils.DIRECT_EXCHANGE_NAME);
                    AndesMessageMetadata clonedMetadata = metadata.shallowCopy(metadata.getMessageID());
                    AndesMessage andesMessage = new AndesMessage(clonedMetadata);

                    messagesToRemove.add(metadata);

                    // Update Andes message with all the chunk details
                    List<AndesMessagePart> messageParts = messageContent.get(messageId);
                    messageParts.forEach(andesMessage::addMessagePart);

                    // Handover message to Andes. This will generate a new message ID and store it
                    Andes.getInstance().messageReceived(andesMessage, andesChannel, disablePubAck);
                }

                // Delete old messages
                Andes.getInstance().deleteMessagesFromDLC(messagesToRemove);

                if (interruptedByFlowControl) {
                    // Throw this out so UI will show this to the user as an error message.
                    throw new RuntimeException("Message restore from dead letter queue has been interrupted by flow "
                                               + "control. Please try again later.");
                }

            } catch (AndesException e) {
                throw new RuntimeException("Error restoring messages from " + destinationQueueName, e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getObjectInstanceName() {
        return DLCManagementInformation.TYPE;
    }
}
