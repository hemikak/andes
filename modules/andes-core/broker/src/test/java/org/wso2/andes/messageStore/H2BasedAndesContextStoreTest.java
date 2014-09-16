package org.wso2.andes.messageStore;

import junit.framework.Assert;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.*;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameAlreadyBoundException;
import java.sql.*;
import java.util.List;
import java.util.Map;

public class H2BasedAndesContextStoreTest {

    private static Connection connection;
    private H2BasedAndesContextStore contextStore;

    @BeforeClass
    public static void BeforeClass() throws Exception {
        try {
            // Create initial context
            System.setProperty(Context.INITIAL_CONTEXT_FACTORY,
                    "org.apache.naming.java.javaURLContextFactory");
            System.setProperty(Context.URL_PKG_PREFIXES,
                    "org.apache.naming");

            InitialContext ic = new InitialContext();
            ic.createSubcontext("jdbc");
            JdbcDataSource ds = new JdbcDataSource();
            ds.setURL("jdbc:h2:mem:msg_store;DB_CLOSE_ON_EXIT=FALSE");
            ic.bind("jdbc/InMemoryMessageStoreDB", ds);

            Class.forName("org.h2.Driver");
            connection = DriverManager.getConnection("jdbc:h2:mem:msg_store;DB_CLOSE_ON_EXIT=FALSE");
        } catch (NameAlreadyBoundException ignored) {
        }
    }

    @Before
    public void setup() throws Exception {
        createTables();
        contextStore = new H2BasedAndesContextStore(true); // in memory mode mode
        contextStore.init(null);
    }

    @After
    public void tearDown() throws Exception {
        dropTables();
    }

    @AfterClass
    public static void afterClass() {

    }

    @Test
    public void testStoreDurableSubscription() throws Exception {
        String destIdentifier = "destination";
        String subId = "sub-0";
        String subEncodedAsStr = "data";
        contextStore.storeDurableSubscription(destIdentifier, subId, subEncodedAsStr);

        String select = "SELECT * FROM " + JDBCConstants.DURABLE_SUB_TABLE;
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(select);

        Assert.assertEquals(true, resultSet.first());
        Assert.assertEquals(subId, resultSet.getString(JDBCConstants.DURABLE_SUB_ID));
        Assert.assertEquals(destIdentifier, resultSet.getString(JDBCConstants.DESTINATION_IDENTIFIER));
        Assert.assertEquals(subEncodedAsStr, resultSet.getString(JDBCConstants.DURABLE_SUB_DATA));

    }

    @Test
    public void testGetAllDurableSubscriptions() throws Exception {
        String insert = "INSERT INTO " + JDBCConstants.DURABLE_SUB_TABLE + " (" +
                JDBCConstants.DESTINATION_IDENTIFIER + ", " +
                JDBCConstants.DURABLE_SUB_ID + ", " +
                JDBCConstants.DURABLE_SUB_DATA + ") " +
                " VALUES ( ?,?,? )";

        // populate data
        String destinationIdOne = "destination1";
        String subscriptionIdOne = "sub-0";
        String dataOne = "data1";

        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, destinationIdOne);
        preparedStatement.setString(2, subscriptionIdOne);
        preparedStatement.setString(3, dataOne);
        preparedStatement.addBatch();

        String destinationIdTwo = "destination2";
        String subscriptionIdTwo = "sub-1";
        String dataTwo = "data2";

        preparedStatement.setString(1, destinationIdTwo);
        preparedStatement.setString(2, subscriptionIdTwo);
        preparedStatement.setString(3, dataTwo);
        preparedStatement.addBatch();
        preparedStatement.executeBatch();

        // retrieve data
        Map<String, List<String>> subscriberMap = contextStore.getAllStoredDurableSubscriptions();

        // test
        int destinationCount = 2;
        Assert.assertEquals(destinationCount, subscriberMap.size());

        List<String> subscriberList = subscriberMap.get(destinationIdOne);
        Assert.assertEquals(dataOne, subscriberList.get(0));

        subscriberList = subscriberMap.get(destinationIdTwo);
        Assert.assertEquals(dataTwo, subscriberList.get(0));
    }

    @Test
    public void testGetAllSubscribersWithSameDestinationForALL() throws Exception {
        String insert = "INSERT INTO " + JDBCConstants.DURABLE_SUB_TABLE + " (" +
                JDBCConstants.DESTINATION_IDENTIFIER + ", " +
                JDBCConstants.DURABLE_SUB_ID + ", " +
                JDBCConstants.DURABLE_SUB_DATA + ") " +
                " VALUES ( ?,?,? )";

        // populate data
        String destinationIdOne = "destination1";
        String subscriptionIdOne = "sub-0";
        String dataOne = "data1";

        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, destinationIdOne);
        preparedStatement.setString(2, subscriptionIdOne);
        preparedStatement.setString(3, dataOne);
        preparedStatement.addBatch();

        String subscriptionIdTwo = "sub-1";
        String dataTwo = "data2";

        preparedStatement.setString(1, destinationIdOne);
        preparedStatement.setString(2, subscriptionIdTwo);
        preparedStatement.setString(3, dataTwo);
        preparedStatement.addBatch();
        preparedStatement.executeBatch();

        // retrieve data
        Map<String, List<String>> subscriberMap = contextStore.getAllStoredDurableSubscriptions();

        // test
        int destinationCount = 1;
        Assert.assertEquals(destinationCount, subscriberMap.size());

        List<String> subscriberList = subscriberMap.get(destinationIdOne);
        Assert.assertEquals(dataOne, subscriberList.get(0));
        Assert.assertEquals(dataTwo, subscriberList.get(1));
    }

    @Test
    public void testRemoveDurableSubscription() throws Exception {
        String insert = "INSERT INTO " + JDBCConstants.DURABLE_SUB_TABLE + " (" +
                JDBCConstants.DESTINATION_IDENTIFIER + ", " +
                JDBCConstants.DURABLE_SUB_ID + ", " +
                JDBCConstants.DURABLE_SUB_DATA + ") " +
                " VALUES ( ?,?,? )";

        // populate data
        String destinationIdOne = "destination1";
        String subscriptionIdOne = "sub-0";
        String dataOne = "data1";

        PreparedStatement preparedStatement = connection.prepareStatement(insert);
        preparedStatement.setString(1, destinationIdOne);
        preparedStatement.setString(2, subscriptionIdOne);
        preparedStatement.setString(3, dataOne);
        preparedStatement.executeUpdate();
        preparedStatement.close();

        // delete
        contextStore.removeDurableSubscription(destinationIdOne, subscriptionIdOne);

        String select = "SELECT * FROM " + JDBCConstants.DURABLE_SUB_TABLE +
                " WHERE " + JDBCConstants.DESTINATION_IDENTIFIER + "=? " +
                " AND " + JDBCConstants.DURABLE_SUB_ID + "=?";

        preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, destinationIdOne);
        preparedStatement.setString(2, subscriptionIdOne);
        ResultSet resultSet = preparedStatement.executeQuery();

        // there should be no entries in result set.
        Assert.assertEquals(false, resultSet.first());
    }

    @Test
    public void testStoreNodeDetails() throws Exception {
        String nodeId = "nodeId";
        String data = "node data";

        // store data
        contextStore.storeNodeDetails(nodeId, data);

        // retrieve to test
        String select = "SELECT * FROM " + JDBCConstants.NODE_INFO_TABLE +
                " WHERE " + JDBCConstants.NODE_ID + "=?";

        PreparedStatement preparedStatement = connection.prepareStatement(select);
        preparedStatement.setString(1, nodeId);
        ResultSet resultSet = preparedStatement.executeQuery();

        // test
        Assert.assertEquals(true, resultSet.first());
        Assert.assertEquals(nodeId, resultSet.getString(JDBCConstants.NODE_ID));
        Assert.assertEquals(data, resultSet.getString(JDBCConstants.NODE_DATA));
    }

    private void createTables() throws Exception {

        String[] queries = {
                "CREATE TABLE durable_subscriptions (" +
                        "sub_id VARCHAR NOT NULL, " +
                        "destination_identifier VARCHAR NOT NULL," +
                        "data VARCHAR NOT NULL " +
                        ");"
                ,

                "CREATE TABLE node_info (" +
                        "node_id VARCHAR NOT NULL," +
                        "data VARCHAR NOT NULL, " +
                        "PRIMARY KEY(node_id) " +
                        ");"


        };
        Statement stmt = connection.createStatement();
        for (String q : queries) {
            stmt.addBatch(q);
        }
        stmt.executeBatch();
        stmt.close();
    }

    private void dropTables() throws Exception {
        String[] queries = {
                "DROP TABLE durable_subscriptions;",
                "DROP TABLE node_info;"
        };
        Statement stmt = connection.createStatement();
        for (String q : queries) {
            stmt.addBatch(q);
        }
        stmt.executeBatch();
        stmt.close();
    }


}