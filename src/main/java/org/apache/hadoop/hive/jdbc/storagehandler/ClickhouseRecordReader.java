package org.apache.hadoop.hive.jdbc.storagehandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBRecordReader;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author andr83
 *         created on 11.08.16
 */
public class ClickhouseRecordReader<T extends DBWritable> extends DBRecordReader<T> {
    final Configuration conf;
    private static final Log LOG = LogFactory.getLog(GenericDBRecordReader.class);

    public ClickhouseRecordReader(DBInputFormat.DBInputSplit split, Class<T> inputClass, Configuration conf, Connection conn, DBConfiguration dbConfig, String cond, String[] fields, String table) throws SQLException {
        super(split, inputClass, conf, conn, dbConfig, cond, fields, table);
        this.conf = conf;
    }

    @Override
    protected ResultSet executeQuery(String query) throws SQLException {
        this.statement = getConnection().prepareStatement(query);
        return statement.executeQuery();
    }

    @Override
    public void close() throws IOException {
        try {
            statement.cancel();
        } catch (SQLException e) {
            // Ignore any errors in cancelling, this is not fatal
            LOG.error("Could not cancel query: " + this.getSelectQuery());
        }
        super.close();
    }

    @Override
    protected String getSelectQuery() {
        StringBuilder query = new StringBuilder();
        DBConfiguration dbConf = getDBConf();
        String conditions = getConditions();
        String tableName = getTableName();
        String[] fieldNames = getFieldNames();
        DBInputFormat.DBInputSplit split = getSplit();

        if (dbConf.getInputQuery() == null) {
            query.append("SELECT ");

            for (int ex = 0; ex < fieldNames.length; ++ex) {
                query.append(fieldNames[ex]);
                if (ex != fieldNames.length - 1) {
                    query.append(", ");
                }
            }

            query.append(" FROM ").append(tableName);
            if (conditions != null && conditions.length() > 0) {
                query.append(" WHERE (").append(conditions).append(")");
            }

            String var4 = dbConf.getInputOrderBy();
            if (var4 != null && var4.length() > 0) {
                query.append(" ORDER BY ").append(var4);
            }
        } else {
            query.append(dbConf.getInputQuery());
        }

        try {
            query.append(" LIMIT ").append(split.getStart()).append(", ").append(split.getLength());
        } catch (IOException err) {
            LOG.error(err.getMessage(), err);
        }
        return query.toString();
    }
}
