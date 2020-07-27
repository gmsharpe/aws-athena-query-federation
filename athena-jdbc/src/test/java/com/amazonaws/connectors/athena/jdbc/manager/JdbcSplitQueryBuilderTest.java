package com.amazonaws.connectors.athena.jdbc.manager;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * author: gmsharpe@ucdavis.edu
 */

public class JdbcSplitQueryBuilderTest {

    @Test
    public void buildSql() throws SQLException {

        JdbcSplitQueryBuilderAbstract jdbcSplitQueryBuilderAbstract = new JdbcSplitQueryBuilderAbstract("'");

        String sql = "select * from test_table where test_attribute = '1'".toUpperCase();

        TableName inputTableName = new TableName("testSchema", "testTable");
        SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol1", org.apache.arrow.vector.types.Types.MinorType.INT.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testCol2", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        expectedSchemaBuilder.addField(FieldBuilder.newBuilder("testPartitionCol", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
        Schema fieldSchema = expectedSchemaBuilder.build();

        String[] schema = {"testCol1", "testCol2"};
        int[] columnTypes = {Types.INTEGER, Types.VARCHAR};
        Object[][] values = {{1, "testVal1"}, {2, "testVal2"}};

        String catalog = "testCatalog";

        Constraints constraints = Mockito.mock(Constraints.class, Mockito.RETURNS_DEEP_STUBS);

        S3SpillLocation s3SpillLocation = S3SpillLocation.newBuilder().withIsDirectory(true).build();

        Split.Builder splitBuilder = Split.newBuilder(s3SpillLocation, null)
                .add("testPartitionCol", String.valueOf("testPartitionValue"));

        Connection connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        PreparedStatement statement = jdbcSplitQueryBuilderAbstract.buildSql(connection,
                catalog,
                inputTableName.getSchemaName(),
                inputTableName.getTableName(),
                fieldSchema,
                constraints,
                splitBuilder.build());

        assertTrue(true);

    }



    private class JdbcSplitQueryBuilderAbstract extends JdbcSplitQueryBuilder {

        /**
         * @param quoteCharacters database quote character for enclosing identifiers.
         */
        public JdbcSplitQueryBuilderAbstract(String quoteCharacters) {
            super(quoteCharacters);
        }

        @Override
        protected String getFromClauseWithSplit(String catalog, String schema, String table, Split split) {
            return null;
        }

        @Override
        protected List<String> getPartitionWhereClauses(Split split) {
            return null;
        }
    }

}
