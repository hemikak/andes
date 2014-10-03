package org.wso2.andes.store.cassandra;

import java.util.*;
import com.datastax.driver.core.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.ConfigurationProperties;
import org.wso2.andes.kernel.*;
import org.wso2.andes.store.cassandra.dao.CQLQueryBuilder;
import org.wso2.andes.store.cassandra.dao.CassandraHelper;
import org.wso2.andes.store.cassandra.dao.GenericCQLDAO;
import org.wso2.andes.server.stats.PerformanceCounter;
import org.wso2.andes.server.store.util.CQLDataAccessHelper;
import org.wso2.andes.server.store.util.CassandraDataAccessException;
import org.wso2.andes.server.util.AlreadyProcessedMessageTracker;
import org.wso2.andes.server.util.AndesConstants;
import org.wso2.andes.tools.utils.DisruptorBasedExecutor.PendingJob;

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;

public class CQLBasedMessageStoreImpl implements org.wso2.andes.kernel.MessageStore {
    private static Log log = LogFactory.getLog(CQLBasedMessageStoreImpl.class);

    private AlreadyProcessedMessageTracker alreadyMovedMessageTracker;

    /**
     * Cassandra cluster object.
     */
    private Cluster cluster;
    /**
     * CQLConnection object which tracks the Cassandra connection
     */
    private CQLConnection cqlConnection;

    public CQLBasedMessageStoreImpl() {
    }

    public DurableStoreConnection initializeMessageStore(ConfigurationProperties
                                                       connectionProperties) throws AndesException {

        // create connection object
        cqlConnection = new CQLConnection();
        cqlConnection.initialize(connectionProperties);

        // get cassandra cluster and create column families
        initializeCassandraMessageStore(cqlConnection);

        alreadyMovedMessageTracker = new AlreadyProcessedMessageTracker("Message-move-tracker", 15000000000L, 10);

        return cqlConnection;
    }

    private void initializeCassandraMessageStore(CQLConnection cqlConnection) throws
            AndesException {
        try {
            cluster = cqlConnection.getCluster();
            createColumnFamilies(cqlConnection);
        } catch (CassandraDataAccessException e) {
            log.error("Error while initializing cassandra message store", e);
            throw new AndesException(e);
        }
    }

    /**
     * Create a cassandra column families for andes usage
     *
     * @throws CassandraDataAccessException
     */
    private void createColumnFamilies(CQLConnection connection) throws CassandraDataAccessException {
        int gcGraceSeconds = connection.getGcGraceSeconds();
        CQLDataAccessHelper.createColumnFamily(CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY, CassandraConstants.KEYSPACE, this.cluster, CassandraConstants.LONG_TYPE, DataType.blob(), gcGraceSeconds);
        CQLDataAccessHelper.createColumnFamily(CassandraConstants.NODE_QUEUES_COLUMN_FAMILY, CassandraConstants.KEYSPACE, this.cluster, CassandraConstants.LONG_TYPE, DataType.blob(), gcGraceSeconds);
        CQLDataAccessHelper.createColumnFamily(CassandraConstants.GLOBAL_QUEUES_COLUMN_FAMILY, CassandraConstants.KEYSPACE, this.cluster, CassandraConstants.LONG_TYPE, DataType.blob(), gcGraceSeconds);
        CQLDataAccessHelper.createColumnFamily(CassandraConstants.META_DATA_COLUMN_FAMILY, CassandraConstants.KEYSPACE, this.cluster, CassandraConstants.LONG_TYPE, DataType.blob(), gcGraceSeconds);
        CQLDataAccessHelper.createColumnFamily(CassandraConstants.PUB_SUB_MESSAGE_IDS_COLUMN_FAMILY, CassandraConstants.KEYSPACE, this.cluster, CassandraConstants.LONG_TYPE, DataType.blob(), gcGraceSeconds);
        CQLDataAccessHelper.createCounterColumnFamily(CassandraConstants.MESSAGE_COUNTERS_COLUMN_FAMILY, CassandraConstants.KEYSPACE, this.cluster, gcGraceSeconds);
        CQLDataAccessHelper.createMessageExpiryColumnFamily(CassandraConstants.MESSAGES_FOR_EXPIRY_COLUMN_FAMILY, CassandraConstants.KEYSPACE, this.cluster, gcGraceSeconds);
    }

    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException {
        try {
            /*Mutator<String> messageMutator = HFactory.createMutator(keyspace, stringSerializer);*/
            List<Insert> inserts = new ArrayList<Insert>();
            for (AndesMessagePart part : partList) {
                final String rowKey = AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX
                        + part.getMessageID();
                Insert insert = CQLDataAccessHelper.addMessageToQueue(CassandraConstants.KEYSPACE, CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY,
                        rowKey, part.getOffSet(), part.getData(), false);
                inserts.add(insert);
                //System.out.println("STORE >> message part id" + part.getMessageID() + " offset " + part.getOffSet());
            }
            /*messageMutator.execute();*/
            GenericCQLDAO.batchExecute(CassandraConstants.KEYSPACE, inserts.toArray(new Insert[inserts.size()]));
/*            for(AndesMessagePart part : partList) {
                log.info("STORED PART ID: " + part.getMessageID() + " OFFSET: " + part.getOffSet() + " Data Len= " + part.getDataLength());
            }*/
        } catch (CassandraDataAccessException e) {
            //TODO handle Cassandra failures
            //When a error happened, we should remember that and stop accepting messages
            log.error(e);
            throw new AndesException("Error in adding the message part to the store", e);
        }
    }

    public void duplicateMessageContent(long messageId, long messageIdOfClone) throws AndesException {
        String originalRowKey = AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX + messageId;
        String cloneMessageKey = AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX + messageIdOfClone;
        try {

            long tryCount = 0;
            //read from store
        /*    ColumnQuery columnQuery = HFactory.createColumnQuery(KEYSPACE, stringSerializer,
                    integerSerializer, byteBufferSerializer);
            columnQuery.setColumnFamily(MESSAGE_CONTENT_COLUMN_FAMILY);
            columnQuery.setKey(originalRowKey.trim());

            SliceQuery<String, Integer, ByteBuffer> query = HFactory.createSliceQuery(KEYSPACE, stringSerializer, integerSerializer, byteBufferSerializer);
            query.setColumnFamily(MESSAGE_CONTENT_COLUMN_FAMILY).setKey(originalRowKey).setRange(0, Integer.MAX_VALUE, false, Integer.MAX_VALUE);
            QueryResult<ColumnSlice<Integer, ByteBuffer>> result = query.execute();*/

            List<AndesMessageMetadata> messages = CQLDataAccessHelper.getMessagesFromQueue(originalRowKey.trim(), CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY, CassandraConstants.KEYSPACE, 0, Long.MAX_VALUE, Long.MAX_VALUE, true, false);

            //if there are values duplicate them

            /*if (!result.get().getColumns().isEmpty()) {*/
            if (!messages.isEmpty()) {
               /* Mutator<String> mutator = HFactory.createMutator(KEYSPACE, stringSerializer);*/
                /*for (HColumn<Integer, ByteBuffer> column : result.get().getColumns()) {
                    int offset = column.getName();
                    final byte[] chunkData = bytesArraySerializer.fromByteBuffer(column.getValue());
                    CQLDataAccessHelper.addMessageToQueue(MESSAGE_CONTENT_COLUMN_FAMILY, cloneMessageKey,
                            offset, chunkData, mutator, false);
                    System.out.println("DUPLICATE>> new id " + messageIdOfClone + " cloned from id " + messageId + " offset" + offset);

                }*/
                //mutator.execute();
                for (AndesMessageMetadata msg : messages) {
                    long offset = msg.getMessageID();
                    final byte[] chunkData = msg.getMetadata();
                    CQLDataAccessHelper.addMessageToQueue(CassandraConstants.KEYSPACE, CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY, cloneMessageKey,
                            offset, chunkData, false);
                    System.out.println("DUPLICATE>> new id " + messageIdOfClone + " cloned from id " + messageId + " offset" + offset);
                }
            } else {
                tryCount += 1;
                if (tryCount == 3) {
                    throw new AndesException("Original Content is not written. Cannot duplicate content. Tried 3 times");
                }
                try {
                    Thread.sleep(20 * tryCount);
                } catch (InterruptedException e) {
                    //silently ignore
                }

                this.duplicateMessageContent(messageId, messageIdOfClone);
            }

        } catch (CassandraDataAccessException e) {
            throw new AndesException(e);
        }
    }

    /**
     * Get message content part of a message.
     *
     * @param messageId The message Id
     * @param offsetValue Message content offset value
     * @return Message content part
     * @throws AndesException
     */
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {
        AndesMessagePart messagePart;
        try {
            String rowKey = AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX + messageId;
            messagePart = CQLDataAccessHelper.getMessageContent(rowKey.trim(), CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY, CassandraConstants.KEYSPACE, messageId, offsetValue);
        } catch (Exception e) {
            log.error("Error in reading content messageID= " + messageId + " offset=" + offsetValue, e);
            throw new AndesException("Error in reading content messageID=" + messageId + " offset=" + offsetValue, e);
        }
        return messagePart;
    }

    public void deleteMessageParts(long messageID, byte[] data) {
    }

    @Override
    public void addMetaData(List<AndesMessageMetadata> metadataList) throws AndesException {
        try {
            List<Insert> inserts = new ArrayList<Insert>();
            for (AndesMessageMetadata md : metadataList) {

                Insert insert = CQLDataAccessHelper.addMessageToQueue(CassandraConstants.KEYSPACE, CassandraConstants.META_DATA_COLUMN_FAMILY, md.getDestination(),
                        md.getMessageID(), md.getMetadata(), false);

                inserts.add(insert);
            }
            long start = System.currentTimeMillis();
            GenericCQLDAO.batchExecute(CassandraConstants.KEYSPACE, inserts.toArray(new Insert[inserts.size()]));

            PerformanceCounter.recordIncomingMessageWrittenToCassandraLatency((int) (System.currentTimeMillis() - start));

            // Client waits for these message ID to be written, this signal those, if there is a error
            //we will not signal and client who tries to close the connection will timeout.
            //We can do this better, but leaving this as is or now.
            for (AndesMessageMetadata md : metadataList) {
                PendingJob jobData = md.getPendingJobsTracker().get(md.getMessageID());
                if (jobData != null) {
                    jobData.semaphore.release();
                }
            }
        } catch (Exception e) {
            //TODO handle Cassandra failures
            //TODO may be we can write those message to a disk, or do something. Currently we will just loose them
            log.error("Error writing incoming messages to Cassandra", e);
            throw new AndesException("Error writing incoming messages to Cassandra", e);
        }
    }

    @Override
    public void addMetaData(AndesMessageMetadata metadata) throws AndesException {

    }

    @Override
    public void addMetaDataToQueue(String queueName, AndesMessageMetadata metadata) throws AndesException {

    }

    @Override
    public void addMetadataToQueue(String queueName, List<AndesMessageMetadata> metadataList) throws AndesException {
    }

    @Override
    //TODO:hasitha - do we want this method?
    public AndesMessageMetadata getMetaData(long messageId) throws AndesException {
        AndesMessageMetadata metadata = null;
        try {

            byte[] value = CQLDataAccessHelper.getMessageMetaDataFromQueue(CassandraConstants.NODE_QUEUES_COLUMN_FAMILY, CassandraConstants.KEYSPACE, messageId);
            if (value == null) {
                value = CQLDataAccessHelper.getMessageMetaDataFromQueue(CassandraConstants.PUB_SUB_MESSAGE_IDS_COLUMN_FAMILY, CassandraConstants.KEYSPACE, messageId);
            }
            metadata = new AndesMessageMetadata(messageId, value, true);

        } catch (Exception e) {
            log.error("Error in getting meta data of provided message id", e);
            throw new AndesException("Error in getting meta data for messageID " + messageId, e);
        }
        return metadata;
    }

    @Override
    public List<AndesMessageMetadata> getMetaDataList(String queueName, long firstMsgId, long lastMsgID) throws AndesException {
        try {
            //TODO: what should be put to count
            List<AndesMessageMetadata> metadataList = CQLDataAccessHelper.getMessagesFromQueue(queueName,
                    CassandraConstants.META_DATA_COLUMN_FAMILY, CassandraConstants.KEYSPACE, firstMsgId, lastMsgID, 1000, true, true);
            return metadataList;
        } catch (CassandraDataAccessException e) {
            throw new AndesException(e);
        }


    }

    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(String queueName, long firstMsgId, int count) throws AndesException {
        try {
            List<AndesMessageMetadata> metadataList = CQLDataAccessHelper.getMessagesFromQueue(queueName,
                    CassandraConstants.META_DATA_COLUMN_FAMILY, CassandraConstants.KEYSPACE, firstMsgId + 1, Long.MAX_VALUE, count, true, true);
            //combining metadata with message properties create QueueEntries
            /*for (Object column : messagesColumnSlice.getColumns()) {
                if (column instanceof HColumn) {
                    long messageId = ((HColumn<Long, byte[]>) column).getName();
                    byte[] value = ((HColumn<Long, byte[]>) column).getValue();
                    metadataList.add(new AndesMessageMetadata(messageId, value));
                }
            }*/
            return metadataList;
        } catch (CassandraDataAccessException e) {
            throw new AndesException(e);
        }

    }

    @Override
    public void deleteMessageMetadataFromQueue(String queueName, List<AndesRemovableMetadata> messagesToRemove) throws AndesException {
        try {
            // Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
            List<Statement> statements = new ArrayList<Statement>();
            for (AndesRemovableMetadata message : messagesToRemove) {
                //mutator.addDeletion(queueAddress.queueName, getColumnFamilyFromQueueAddress(queueAddress), message.messageID, longSerializer);
                //Delete delete = CQLDataAccessHelper.deleteLongColumnFromRaw(KEYSPACE, getColumnFamilyFromQueueAddress(queueAddress), queueAddress.queueName, message.messageID, false);
                Delete delete = CQLDataAccessHelper.deleteLongColumnFromRaw(CassandraConstants.KEYSPACE, CassandraConstants.META_DATA_COLUMN_FAMILY, queueName , message.messageID, false);
                statements.add(delete);
            }
            //mutator.execute();
            GenericCQLDAO.batchExecute(CassandraConstants.KEYSPACE, statements.toArray(new Statement[statements.size()]));

        } catch (Exception e) {
            log.error("Error while deleting messages", e);
            throw new AndesException(e);
        }
    }

    @Override
    public void deleteMessageParts(List<Long> messageIdList) throws AndesException {
        try {
            List<String> rows2Remove = new ArrayList<String>();
            for (long messageId : messageIdList) {
                rows2Remove.add(new StringBuffer(
                        AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX).append(messageId).toString());
            }
            //remove content
            if (!rows2Remove.isEmpty()) {
                CQLDataAccessHelper.deleteIntegerRowListFromColumnFamily(CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY, rows2Remove, CassandraConstants.KEYSPACE);
            }
        } catch (CassandraDataAccessException e) {
            throw new AndesException(e);
        }
    }

    private String getColumnFamilyFromQueueAddress(QueueAddress address) {
        String columnFamilyName;
        if (address.queueType.equals(QueueAddress.QueueType.QUEUE_NODE_QUEUE)) {
            columnFamilyName = CassandraConstants.NODE_QUEUES_COLUMN_FAMILY;
        } else if (address.queueType.equals(QueueAddress.QueueType.GLOBAL_QUEUE)) {
            columnFamilyName = CassandraConstants.GLOBAL_QUEUES_COLUMN_FAMILY;
        } else if (address.queueType.equals(QueueAddress.QueueType.TOPIC_NODE_QUEUE)) {
            columnFamilyName = CassandraConstants.PUB_SUB_MESSAGE_IDS_COLUMN_FAMILY;
        } else {
            columnFamilyName = null;
        }
        return columnFamilyName;
    }

    private boolean msgOKToMove(long msgID, QueueAddress sourceAddress, QueueAddress targetAddress) throws CassandraDataAccessException {
        if (alreadyMovedMessageTracker.checkIfAlreadyTracked(msgID)) {
            List<Statement> statements = new ArrayList<Statement>();
            Delete delete = CQLDataAccessHelper.deleteLongColumnFromRaw(CassandraConstants.KEYSPACE, getColumnFamilyFromQueueAddress(sourceAddress), sourceAddress.queueName, msgID, false);
            statements.add(delete);
            GenericCQLDAO.batchExecute(CassandraConstants.KEYSPACE, statements.toArray(new Statement[statements.size()]));
            if (log.isTraceEnabled()) {
                log.trace("Rejecting moving message id =" + msgID + " as it is already read from " + sourceAddress.queueName);
            }
            return false;
        } else {
            alreadyMovedMessageTracker.insertMessageForTracking(msgID, msgID);
            if (log.isTraceEnabled()) {
                log.trace("allowing to move message id - " + msgID + " from " + sourceAddress.queueName);
            }
            return true;
        }
    }

    public void close() {
        alreadyMovedMessageTracker.shutDownMessageTracker();
        cqlConnection.close();
    }

    @Override
    public List<AndesRemovableMetadata> getExpiredMessages(int limit) {
        // todo implement

        return new ArrayList<AndesRemovableMetadata>();
    }

    @Override
/**
 * Adds the received JMS Message ID along with its expiration time to "MESSAGES_FOR_EXPIRY_COLUMN_FAMILY" queue
 * @param messageId
 * @param expirationTime
 * @throws CassandraDataAccessException
 */
    public void addMessageToExpiryQueue(Long messageId, Long expirationTime, boolean isMessageForTopic, String destination) throws AndesException {

        final String columnFamily = CassandraConstants.MESSAGES_FOR_EXPIRY_COLUMN_FAMILY;
        //final String rowKey = CassandraConstants.MESSAGES_FOR_EXPIRY_ROW_NAME;

        if (columnFamily == null || messageId == 0) {
            throw new AndesException("Can't add data with queueType = " + columnFamily +
                    " and messageId  = " + messageId + " expirationTime = " + expirationTime);
        }

        try {
            Map<String, Object> keyValueMap = new HashMap<String, Object>();
            keyValueMap.put(CQLDataAccessHelper.MESSAGE_ID, messageId);
            keyValueMap.put(CQLDataAccessHelper.MESSAGE_EXPIRATION_TIME, expirationTime);
            keyValueMap.put(CQLDataAccessHelper.MESSAGE_DESTINATION, destination);
            keyValueMap.put(CQLDataAccessHelper.MESSAGE_IS_FOR_TOPIC, isMessageForTopic);

            Insert insert = CQLQueryBuilder.buildSingleInsert(CassandraConstants.KEYSPACE, columnFamily, keyValueMap);

            GenericCQLDAO.execute(CassandraConstants.KEYSPACE, insert.getQueryString());

        } catch (CassandraDataAccessException e) {
            log.error("Error while adding message to expiry queue" , e);
            throw new AndesException(e);
        }

    }

    @Override
/**
 * Method to delete entries from MESSAGES_FOR_EXPIRY_COLUMN_FAMILY Column Family
 */
    public void deleteMessagesFromExpiryQueue(List<Long> messagesToRemove) throws AndesException {
        try {
            List<Statement> statements = new ArrayList<Statement>();
            for (Long messageId : messagesToRemove) {

                CQLQueryBuilder.CqlDelete cqlDelete = new CQLQueryBuilder.CqlDelete(CassandraConstants.KEYSPACE, CassandraConstants.MESSAGES_FOR_EXPIRY_COLUMN_FAMILY);

                cqlDelete.addCondition(CQLDataAccessHelper.MESSAGE_ID, messageId, CassandraHelper.WHERE_OPERATORS.EQ);

                Delete delete = CQLQueryBuilder.buildSingleDelete(cqlDelete);

                statements.add(delete);
            }

            // There is another way. Can delete using a single where clause on the timestamp. (delete all messages with expiration time < current timestamp)
            // But that could result in inconsistent data overall.
            // If a message has been added to the queue between expiry checker invocation and this method, it could be lost.
            // Therefore, the above approach to delete.
            GenericCQLDAO.batchExecute(CassandraConstants.KEYSPACE, statements.toArray(new Statement[statements.size()]));
        } catch (Exception e) {
            log.error("Error while deleting messages", e);
            throw new AndesException(e);
        }
    }

}