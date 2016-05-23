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

package org.wso2.andes.management.common.mbeans;

import org.wso2.andes.management.common.mbeans.annotations.MBeanOperation;
import org.wso2.andes.management.common.mbeans.annotations.MBeanOperationParameter;

import javax.management.MBeanOperationInfo;

/**
 * Interface for managing messages in the dead letter channel.
 */
public interface DLCManagementInformation {
    String TYPE = "DLCManagementInformation";

    /**
     * Delete a selected message list from a given Dead Letter Queue of a tenant.
     *
     * @param andesMetadataIDs     The browser message Ids
     * @param destinationQueueName The Dead Letter Queue Name for the tenant
     */
    @MBeanOperation(name = "Delete messages in dead letter queue",
                    description = "Will delete messages from dead letter queue",
                    impact = MBeanOperationInfo.ACTION)
    void deleteMessagesFromDeadLetterQueue(
            @MBeanOperationParameter(name = "andesMetadataIDs", description = "ID of the Messages to Be Deleted")
            long[] andesMetadataIDs,
            @MBeanOperationParameter(name = "destinationQueueName", description = "The Dead Letter Queue Name for the" +
                                                                                  " selected tenant")
            String destinationQueueName);

    /**
     * Restore a given browser message Id list from the Dead Letter Queue to the same queue it was previous in before
     * moving to the Dead Letter Queue and remove them from the Dead Letter Queue.
     *
     * @param andesMetadataIDs     The browser message Ids
     * @param destinationQueueName The Dead Letter Queue Name for the tenant
     */
    @MBeanOperation(name = "Restore Back a Specific set of Messages",
                    description = "Will Restore a Specific Set of Messages Back to Its Original Queue",
                    impact = MBeanOperationInfo.ACTION)
    void restoreMessagesFromDeadLetterQueue(
            @MBeanOperationParameter(name = "andesMetadataIDs", description = "IDs of the Messages to Be Restored")
            long[] andesMetadataIDs,
            @MBeanOperationParameter(name = "destinationQueueName", description = "The Dead Letter Queue Name for " +
                                                                                  "the selected tenant")
            String destinationQueueName);

    /**
     * Restore a given browser message Id list from the Dead Letter Queue to a different given queue in the same tenant
     * and remove them from the Dead Letter Queue.
     *
     * @param destinationQueueName    The Dead Letter Queue Name for the tenant
     * @param andesMetadataIDs        The browser message Ids
     * @param newDestinationQueueName The new destination
     */
    @MBeanOperation(name = "Restore Back a Specific set of Messages",
                    description = "Will Restore a Specific Set of Messages Back to a Queue different from the original",
                    impact = MBeanOperationInfo.ACTION)
    void restoreMessagesFromDeadLetterQueue(
            @MBeanOperationParameter(name = "andesMetadataIDs", description = "IDs of the Messages to Be Restored")
            long[] andesMetadataIDs,
            @MBeanOperationParameter(name = "destination", description = "Destination of the message to be restored")
            String newDestinationQueueName,
            @MBeanOperationParameter(name = "deadLetterQueueName", description = "The Dead Letter Queue Name for the " +
                                                                                 "selected tenant")
            String destinationQueueName);
}
